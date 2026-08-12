use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::collections::HashSet;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Filters out subscription-only posts from authors the viewer is not subscribed to.
pub struct IneligibleSubscriptionFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for IneligibleSubscriptionFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let subscribed_user_ids: HashSet<u64> = query
            .user_features
            .subscribed_user_ids
            .iter()
            .filter_map(|id| u64::try_from(*id).ok().filter(|id| *id != 0))
            .collect();

        let (kept, removed): (Vec<_>, Vec<_>) =
            candidates
                .into_iter()
                .partition(|candidate| match candidate.subscription_author_id {
                    Some(author_id) => subscribed_user_ids.contains(&author_id),
                    None => true,
                });

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_features::UserFeatures;

    #[test]
    fn ignores_invalid_signed_subscription_ids() {
        let query = ScoredPostsQuery {
            user_features: UserFeatures {
                subscribed_user_ids: vec![-1, 0, 10],
                ..Default::default()
            },
            ..Default::default()
        };
        let result = IneligibleSubscriptionFilter.filter(
            &query,
            vec![
                PostCandidate {
                    tweet_id: 1,
                    subscription_author_id: Some(u64::MAX),
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: 2,
                    subscription_author_id: Some(10),
                    ..Default::default()
                },
            ],
        );

        assert_eq!(result.kept[0].tweet_id, 2);
        assert_eq!(result.removed[0].tweet_id, 1);
    }
}
