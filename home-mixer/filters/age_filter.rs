use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Filter that removes tweets older than a specified duration.
pub struct AgeFilter {
    pub max_age: Duration,
}

impl AgeFilter {
    pub fn new(max_age: Duration) -> Self {
        Self { max_age }
    }

    fn is_within_age(&self, candidate: &PostCandidate) -> bool {
        let Some(created_ms) = candidate.created_at_ms.or_else(|| {
            let ts = candidate.tweet_id.timestamp_secs();
            (ts > 0).then(|| u64::from(ts).saturating_mul(1000))
        }) else {
            return false;
        };
        let Some(now_ms) = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis() as u64)
        else {
            return false;
        };
        let age_ms = now_ms.saturating_sub(created_ms);
        age_ms <= self.max_age.as_millis() as u64
    }
}

impl Filter<ScoredPostsQuery, PostCandidate> for AgeFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (kept, removed): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(|c| self.is_within_age(c));

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{pid, uid};

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn keeps_recent_created_at_and_drops_old_or_missing() {
        let filter = AgeFilter::new(Duration::from_secs(48 * 3600));
        let now = now_ms();
        let from_parts_id =
            crate::models::ObjectId::from_parts(u32::try_from(now / 1000).unwrap(), 9);
        let recent = PostCandidate {
            tweet_id: pid(1),
            author_id: uid(2),
            created_at_ms: Some(now - 60_000),
            ..Default::default()
        };
        let old = PostCandidate {
            tweet_id: pid(3),
            created_at_ms: Some(now - 10 * 24 * 3600 * 1000),
            ..Default::default()
        };
        let missing = PostCandidate {
            tweet_id: pid(4),
            created_at_ms: None,
            ..Default::default()
        };
        let from_parts = PostCandidate {
            tweet_id: from_parts_id,
            created_at_ms: None,
            ..Default::default()
        };

        let result = filter.filter(
            &ScoredPostsQuery::default(),
            vec![recent, old, missing, from_parts],
        );
        let kept: Vec<_> = result.kept.iter().map(|c| c.tweet_id).collect();
        assert_eq!(kept, vec![pid(1), from_parts_id]);
    }
}
