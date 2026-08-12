use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
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
        let impressed: HashSet<u64> = query.impressed_post_ids.iter().copied().collect();
        let (removed, kept) = candidates
            .into_iter()
            .partition(|candidate| impressed.contains(&candidate.tweet_id));
        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_impressions_when_primary_seen_ids_are_unavailable() {
        let query = ScoredPostsQuery {
            impressed_post_ids: vec![2],
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                ..Default::default()
            },
        ];
        let result = PreviouslySeenPostsBackupFilter.filter(&query, candidates);

        assert_eq!(result.kept[0].tweet_id, 1);
        assert_eq!(result.removed[0].tweet_id, 2);
    }
}
