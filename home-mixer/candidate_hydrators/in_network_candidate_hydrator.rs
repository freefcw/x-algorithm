use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::collections::HashSet;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct InNetworkCandidateHydrator;

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for InNetworkCandidateHydrator {
    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let viewer_id = query.user_id;
        let followed_ids: HashSet<crate::models::UserId> = query
            .user_features
            .followed_user_ids
            .iter()
            .copied()
            .filter(|id| !id.is_nil())
            .collect();

        let hydrated_candidates = candidates
            .iter()
            .map(|candidate| {
                let is_self = candidate.author_id == viewer_id;
                // Source adapters may already know the authoritative origin:
                // Thunder marks true and the business fallback marks false.
                // Only infer membership for sources that leave it unset.
                let is_in_network = candidate
                    .in_network
                    .unwrap_or_else(|| is_self || followed_ids.contains(&candidate.author_id));
                Ok(PostCandidate {
                    in_network: Some(is_in_network),
                    ..Default::default()
                })
            })
            .collect();

        hydrated_candidates
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.in_network = hydrated.in_network;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_features::UserFeatures;

    #[tokio::test]
    async fn ignores_nonpositive_followed_ids() {
        let query = ScoredPostsQuery {
            user_features: UserFeatures {
                followed_user_ids: vec![crate::models::UserId::NIL, 10.into()],
                ..Default::default()
            },
            ..Default::default()
        };
        let hydrated = InNetworkCandidateHydrator
            .hydrate(
                &query,
                &[
                    PostCandidate {
                        author_id: crate::models::uid(u64::MAX),
                        ..Default::default()
                    },
                    PostCandidate {
                        author_id: 10.into(),
                        ..Default::default()
                    },
                ],
            )
            .await;

        assert_eq!(
            hydrated[0].as_ref().expect("candidate").in_network,
            Some(false)
        );
        assert_eq!(
            hydrated[1].as_ref().expect("candidate").in_network,
            Some(true)
        );
    }

    #[tokio::test]
    async fn preserves_source_authority_when_membership_is_already_set() {
        let query = ScoredPostsQuery {
            user_id: crate::models::uid(1),
            user_features: UserFeatures {
                followed_user_ids: vec![crate::models::uid(2)],
                ..Default::default()
            },
            ..Default::default()
        };
        let hydrated = InNetworkCandidateHydrator
            .hydrate(
                &query,
                &[PostCandidate {
                    author_id: crate::models::uid(2),
                    // A fallback source can be authored by a followed user but
                    // still be explicitly classified as out-of-network.
                    in_network: Some(false),
                    ..Default::default()
                }],
            )
            .await;
        assert_eq!(
            hydrated[0].as_ref().expect("candidate").in_network,
            Some(false)
        );
    }
}
