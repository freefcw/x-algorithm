use crate::feature_policy::VfFailurePolicy;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::visibility::models::{Action, FilteredReason, VisibilityDecision};
use xai_candidate_pipeline::filter::{Filter, FilterResult};

#[derive(Default)]
pub struct VFFilter {
    failure_policy: VfFailurePolicy,
}

impl VFFilter {
    pub fn new(failure_policy: VfFailurePolicy) -> Self {
        Self { failure_policy }
    }
}

impl Filter<ScoredPostsQuery, PostCandidate> for VFFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (removed, kept): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|candidate| should_drop_candidate(candidate, self.failure_policy));

        FilterResult { kept, removed }
    }
}

fn should_drop_candidate(candidate: &PostCandidate, failure_policy: VfFailurePolicy) -> bool {
    if let Some(action) = &candidate.visibility_action {
        return matches!(action, Action::Drop(_));
    }
    match &candidate.visibility_decision {
        VisibilityDecision::Allowed => false,
        VisibilityDecision::Restricted(reason) => should_drop_reason(reason),
        VisibilityDecision::Unchecked | VisibilityDecision::Unavailable(_) => {
            match failure_policy {
                VfFailurePolicy::FailClosed => true,
                VfFailurePolicy::InNetworkOnly => candidate.in_network != Some(true),
                VfFailurePolicy::AllowAll => false,
            }
        }
    }
}

fn should_drop_reason(reason: &FilteredReason) -> bool {
    match reason {
        FilteredReason::SafetyResult(safety_result) => {
            matches!(safety_result.action, Action::Drop(_))
        }
        FilteredReason::GenericFiltered(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        tweet_id: u64,
        in_network: Option<bool>,
        visibility_decision: VisibilityDecision,
    ) -> PostCandidate {
        PostCandidate {
            tweet_id,
            in_network,
            visibility_decision,
            ..Default::default()
        }
    }

    #[test]
    fn allow_all_policy_keeps_unavailable_in_network_and_out_of_network() {
        let result = VFFilter::new(VfFailurePolicy::AllowAll).filter(
            &ScoredPostsQuery::default(),
            vec![
                candidate(
                    1,
                    Some(true),
                    VisibilityDecision::Unavailable("vf down".to_string()),
                ),
                candidate(
                    2,
                    Some(false),
                    VisibilityDecision::Unavailable("vf down".to_string()),
                ),
            ],
        );

        assert_eq!(
            result.kept.iter().map(|c| c.tweet_id).collect::<Vec<_>>(),
            vec![crate::models::pid(1), crate::models::pid(2)]
        );
        assert!(result.removed.is_empty());
    }

    #[test]
    fn allow_all_policy_keeps_unchecked_out_of_network() {
        let result = VFFilter::new(VfFailurePolicy::AllowAll).filter(
            &ScoredPostsQuery::default(),
            vec![candidate(2, Some(false), VisibilityDecision::Unchecked)],
        );

        assert_eq!(result.kept[0].tweet_id, crate::models::pid(2));
        assert!(result.removed.is_empty());
    }

    #[test]
    fn default_policy_drops_every_unverified_candidate() {
        let result = VFFilter::default().filter(
            &ScoredPostsQuery::default(),
            vec![
                candidate(1, Some(true), VisibilityDecision::Unchecked),
                candidate(
                    2,
                    Some(true),
                    VisibilityDecision::Unavailable("vf down".to_string()),
                ),
                candidate(3, Some(true), VisibilityDecision::Allowed),
            ],
        );

        assert_eq!(
            result.kept.iter().map(|c| c.tweet_id).collect::<Vec<_>>(),
            vec![crate::models::pid(3)]
        );
        assert_eq!(
            result
                .removed
                .iter()
                .map(|c| c.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(1), crate::models::pid(2)]
        );
    }

    #[test]
    fn explicit_action_is_used_independently_from_reason() {
        let candidate = PostCandidate {
            visibility_decision: VisibilityDecision::Restricted(FilteredReason::SafetyResult(
                crate::visibility::models::SafetyResult {
                    action: Action::Drop(Default::default()),
                    description: None,
                },
            )),
            visibility_action: Some(Action::Allow),
            ..Default::default()
        };

        let result = VFFilter::default().filter(&ScoredPostsQuery::default(), vec![candidate]);
        assert_eq!(result.kept.len(), 1);
        assert!(result.removed.is_empty());
    }

    #[test]
    fn in_network_only_policy_drops_unverified_out_of_network_and_unknown() {
        let result = VFFilter::new(VfFailurePolicy::InNetworkOnly).filter(
            &ScoredPostsQuery::default(),
            vec![
                candidate(
                    1,
                    Some(true),
                    VisibilityDecision::Unavailable("vf down".to_string()),
                ),
                candidate(
                    2,
                    Some(false),
                    VisibilityDecision::Unavailable("vf down".to_string()),
                ),
                candidate(3, None, VisibilityDecision::Unchecked),
            ],
        );

        assert_eq!(
            result.kept.iter().map(|c| c.tweet_id).collect::<Vec<_>>(),
            vec![crate::models::pid(1)]
        );
        assert_eq!(
            result
                .removed
                .iter()
                .map(|c| c.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(2), crate::models::pid(3)]
        );
    }

    #[test]
    fn in_network_only_policy_still_drops_explicit_drop_action() {
        let candidate = PostCandidate {
            in_network: Some(true),
            visibility_decision: VisibilityDecision::Unavailable("vf down".to_string()),
            visibility_action: Some(Action::Drop(Default::default())),
            ..Default::default()
        };

        let result = VFFilter::new(VfFailurePolicy::InNetworkOnly)
            .filter(&ScoredPostsQuery::default(), vec![candidate]);
        assert!(result.kept.is_empty());
        assert_eq!(result.removed.len(), 1);
    }
}
