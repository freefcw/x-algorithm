use crate::id::IdentityRegistrationContext;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use crate::visibility::models::{Action, FilteredReason, VisibilityDecision};
use crate::visibility::vf_client::{
    GetTwitterContextViewer, SafetyLevel, SafetyLevel::TimelineHome,
    SafetyLevel::TimelineHomeRecommendations, TwitterContextViewer, VisibilityFilteringClient,
};
use futures::future::join3;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct VFCandidateHydrator {
    pub vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync>,
}

impl VFCandidateHydrator {
    pub async fn new(vf_client: Arc<dyn VisibilityFilteringClient + Send + Sync>) -> Self {
        Self { vf_client }
    }

    async fn fetch_vf_results(
        client: &Arc<dyn VisibilityFilteringClient + Send + Sync>,
        tweet_ids: Vec<crate::models::PostId>,
        safety_level: SafetyLevel,
        for_user_id: crate::models::UserId,
        context: Option<TwitterContextViewer>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<crate::models::PostId, Option<FilteredReason>>, String> {
        if tweet_ids.is_empty() {
            return Ok(HashMap::new());
        }

        tokio::time::timeout(
            Duration::from_millis(params::VF_REQUEST_TIMEOUT_MS),
            client.get_result(tweet_ids, safety_level, for_user_id, context, identity),
        )
        .await
        .map_err(|_| {
            format!(
                "visibility request timed out after {}ms",
                params::VF_REQUEST_TIMEOUT_MS
            )
        })?
        .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for VFCandidateHydrator {
    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let context = query.get_viewer();
        let user_id = query.user_id;
        let client = &self.vf_client;
        let identity = query.registration_context();

        let mut in_network_ids = Vec::new();
        let mut oon_ids = Vec::new();
        for candidate in candidates.iter() {
            if candidate.in_network.unwrap_or(false) {
                in_network_ids.push(candidate.tweet_id);
            } else {
                oon_ids.push(candidate.tweet_id);
            }
        }

        let in_network_future = Self::fetch_vf_results(
            client,
            in_network_ids,
            TimelineHome,
            user_id,
            context.clone(),
            identity.clone(),
        );

        let oon_future = Self::fetch_vf_results(
            client,
            oon_ids,
            TimelineHomeRecommendations,
            user_id,
            context,
            identity.clone(),
        );

        let ancillary_ids = candidates
            .iter()
            .flat_map(|candidate| [candidate.retweeted_tweet_id, candidate.quoted_tweet_id])
            .flatten()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let ancillary_future = Self::fetch_vf_results(
            client,
            ancillary_ids,
            TimelineHomeRecommendations,
            user_id,
            query.get_viewer(),
            identity,
        );

        let (in_network_result, oon_result, ancillary_result) =
            join3(in_network_future, oon_future, ancillary_future).await;

        let hydrated = candidates
            .iter()
            .map(|candidate| {
                let direct_result = if candidate.in_network.unwrap_or(false) {
                    &in_network_result
                } else {
                    &oon_result
                };
                let visibility_decision = decision_for(direct_result, candidate.tweet_id);
                let visibility_action = action_for_decision(&visibility_decision);
                let drop_ancillary_posts = ancillary_must_drop(
                    &ancillary_result,
                    [candidate.retweeted_tweet_id, candidate.quoted_tweet_id]
                        .into_iter()
                        .flatten(),
                );
                Ok(PostCandidate {
                    visibility_decision,
                    visibility_action,
                    drop_ancillary_posts: Some(drop_ancillary_posts),
                    ..Default::default()
                })
            })
            .collect::<Vec<Result<PostCandidate, String>>>();
        let unavailable_candidates = hydrated
            .iter()
            .filter(|result| {
                result.as_ref().is_ok_and(|candidate| {
                    matches!(
                        &candidate.visibility_decision,
                        VisibilityDecision::Unavailable(_)
                    )
                })
            })
            .count();
        if unavailable_candidates > 0 {
            log::warn!(
                "request_id={} visibility unavailable for {} candidates; deferring to configured failure policy",
                query.request_id,
                unavailable_candidates
            );
        }
        hydrated
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.visibility_decision = hydrated.visibility_decision;
        candidate.visibility_action = hydrated.visibility_action;
        candidate.drop_ancillary_posts = hydrated.drop_ancillary_posts;
    }
}

fn decision_for(
    result: &Result<HashMap<crate::models::PostId, Option<FilteredReason>>, String>,
    tweet_id: crate::models::PostId,
) -> VisibilityDecision {
    match result {
        Ok(values) => match values.get(&tweet_id) {
            Some(None) => VisibilityDecision::Allowed,
            Some(Some(reason)) => VisibilityDecision::Restricted(reason.clone()),
            None => VisibilityDecision::Unavailable("visibility response omitted post".to_string()),
        },
        Err(error) => VisibilityDecision::Unavailable(error.clone()),
    }
}

fn action_for_decision(decision: &VisibilityDecision) -> Option<Action> {
    match decision {
        VisibilityDecision::Allowed => Some(Action::Allow),
        VisibilityDecision::Restricted(FilteredReason::SafetyResult(result)) => {
            Some(result.action.clone())
        }
        VisibilityDecision::Restricted(FilteredReason::GenericFiltered(_)) => {
            Some(Action::Drop(Default::default()))
        }
        VisibilityDecision::Unchecked | VisibilityDecision::Unavailable(_) => None,
    }
}

fn ancillary_must_drop(
    result: &Result<HashMap<crate::models::PostId, Option<FilteredReason>>, String>,
    ids: impl Iterator<Item = crate::models::PostId>,
) -> bool {
    ids.into_iter().any(|id| match result {
        Ok(values) => !matches!(values.get(&id), Some(None)),
        Err(_) => true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeVisibilityClient;

    #[async_trait]
    impl VisibilityFilteringClient for FakeVisibilityClient {
        async fn get_result(
            &self,
            tweet_ids: Vec<crate::models::PostId>,
            _safety_level: SafetyLevel,
            _for_user_id: crate::models::UserId,
            _context: Option<TwitterContextViewer>,
            _identity: Arc<crate::id::IdentityRegistrationContext>,
        ) -> Result<HashMap<crate::models::PostId, Option<FilteredReason>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let reason = (id == crate::models::pid(2))
                        .then(|| FilteredReason::GenericFiltered("unsafe quote".to_string()));
                    (id, reason)
                })
                .collect())
        }
    }

    #[test]
    fn marks_candidate_when_quoted_post_is_not_visible() {
        let hydrator = VFCandidateHydrator {
            vf_client: Arc::new(FakeVisibilityClient),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let hydrated = runtime.block_on(hydrator.hydrate(
            &ScoredPostsQuery::test_default(),
            &[PostCandidate {
                tweet_id: 1,
                quoted_tweet_id: Some(2),
                ..Default::default()
            }],
        ));
        let hydrated = hydrated[0].as_ref().expect("visibility hydration");

        assert!(matches!(
            hydrated.visibility_decision,
            VisibilityDecision::Allowed
        ));
        assert_eq!(hydrated.drop_ancillary_posts, Some(true));
    }

    #[test]
    fn omitted_visibility_result_is_unavailable() {
        let result = Ok(HashMap::new());
        match decision_for(&result, crate::models::pid(1)) {
            VisibilityDecision::Unavailable(reason) => {
                assert_eq!(reason, "visibility response omitted post");
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
        assert!(ancillary_must_drop(
            &result,
            [crate::models::pid(2)].into_iter()
        ));
    }

    struct FailingVisibilityClient;

    #[async_trait]
    impl VisibilityFilteringClient for FailingVisibilityClient {
        async fn get_result(
            &self,
            _tweet_ids: Vec<crate::models::PostId>,
            _safety_level: SafetyLevel,
            _for_user_id: crate::models::UserId,
            _context: Option<TwitterContextViewer>,
            _identity: Arc<crate::id::IdentityRegistrationContext>,
        ) -> Result<HashMap<crate::models::PostId, Option<FilteredReason>>, anyhow::Error> {
            anyhow::bail!("vf unavailable")
        }
    }

    #[tokio::test]
    async fn client_failure_becomes_explicit_unavailable_decision() {
        let hydrator = VFCandidateHydrator {
            vf_client: Arc::new(FailingVisibilityClient),
        };
        let hydrated = hydrator
            .hydrate(
                &ScoredPostsQuery::test_default(),
                &[PostCandidate {
                    tweet_id: 1,
                    in_network: Some(false),
                    quoted_tweet_id: Some(2),
                    ..Default::default()
                }],
            )
            .await;
        let hydrated = hydrated[0].as_ref().expect("explicit degradation state");

        assert!(matches!(
            hydrated.visibility_decision,
            VisibilityDecision::Unavailable(_)
        ));
        assert_eq!(hydrated.drop_ancillary_posts, Some(true));
    }
}
