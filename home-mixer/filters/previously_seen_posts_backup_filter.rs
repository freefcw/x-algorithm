use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::util::candidates_util::related_post_ids_iter;
use std::collections::HashSet;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct PreviouslySeenPostsBackupFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for PreviouslySeenPostsBackupFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.seen_ids.is_empty() && !query.impressed_post_ids.is_empty()
    }

    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let impressed: HashSet<crate::models::PostId> =
            query.impressed_post_ids.iter().copied().collect();
        // 与主 seen 过滤同口径：看过原帖后，它的转推与回复也算看过。
        let (removed, kept) = candidates.into_iter().partition(|candidate| {
            related_post_ids_iter(candidate).any(|id| impressed.contains(&id))
        });
        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_impressions_when_primary_seen_ids_are_unavailable() {
        let query = ScoredPostsQuery {
            impressed_post_ids: vec![2.into()],
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                ..Default::default()
            },
        ];
        let result = PreviouslySeenPostsBackupFilter.filter(&query, candidates);

        assert_eq!(result.kept[0].tweet_id, crate::models::pid(1));
        assert_eq!(result.removed[0].tweet_id, crate::models::pid(2));
    }

    #[test]
    fn impressed_original_also_removes_its_retweet_and_reply() {
        let query = ScoredPostsQuery {
            impressed_post_ids: vec![100.into()],
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                retweeted_tweet_id: Some(100.into()),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                in_reply_to_tweet_id: Some(100.into()),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3.into(),
                ..Default::default()
            },
        ];
        let result = PreviouslySeenPostsBackupFilter.filter(&query, candidates);

        assert_eq!(result.kept.len(), 1);
        assert_eq!(result.kept[0].tweet_id, crate::models::pid(3));
        assert_eq!(result.removed.len(), 2);
    }
}
