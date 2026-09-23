use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Drops candidates the business first-stage check marked ineligible.
///
/// `None` is treated as keep so Demo TES (which does not set the flag) still
/// produces a feed. Production TES adapters must set `Some(false)` to drop.
pub struct FirstStageEligibleFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for FirstStageEligibleFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (kept, removed) = candidates
            .into_iter()
            .partition(|candidate| candidate.recommendation_eligible != Some(false));
        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::pid;

    #[test]
    fn drops_explicit_false_and_keeps_none_or_true() {
        let result = FirstStageEligibleFilter.filter(
            &ScoredPostsQuery::test_default(),
            vec![
                PostCandidate {
                    tweet_id: pid(1),
                    recommendation_eligible: None,
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: pid(2),
                    recommendation_eligible: Some(true),
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: pid(3),
                    recommendation_eligible: Some(false),
                    ..Default::default()
                },
            ],
        );
        let kept: Vec<_> = result.kept.iter().map(|c| c.tweet_id).collect();
        assert_eq!(kept, vec![pid(1), pid(2)]);
        assert_eq!(result.removed[0].tweet_id, pid(3));
    }
}
