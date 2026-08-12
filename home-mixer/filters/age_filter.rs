use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::util::snowflake;
use std::time::Duration;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Filter that removes tweets older than a specified duration.
pub struct AgeFilter {
    pub max_age: Duration,
}

impl AgeFilter {
    pub fn new(max_age: Duration) -> Self {
        Self { max_age }
    }

    fn is_within_age(&self, tweet_id: u64) -> bool {
        snowflake::duration_since_creation_opt(tweet_id)
            .map(|age| age <= self.max_age)
            .unwrap_or(false)
    }
}

impl Filter<ScoredPostsQuery, PostCandidate> for AgeFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (kept, removed): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|c| self.is_within_age(c.tweet_id));

        FilterResult { kept, removed }
    }
}
