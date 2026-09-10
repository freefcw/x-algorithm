use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::visibility::models::{Action, FilteredReason, VisibilityDecision};
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct VFFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for VFFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (removed, kept): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(should_drop_candidate);

        FilterResult { kept, removed }
    }
}

fn should_drop_candidate(candidate: &PostCandidate) -> bool {
    if let Some(action) = &candidate.visibility_action {
        return matches!(action, Action::Drop(_));
    }
    match &candidate.visibility_decision {
        VisibilityDecision::Allowed => false,
        VisibilityDecision::Restricted(reason) => should_drop_reason(reason),
        VisibilityDecision::Unchecked | VisibilityDecision::Unavailable(_) => {
            !candidate.in_network.unwrap_or(false)
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

    #[test]
    fn unavailable_visibility_keeps_network_posts_and_drops_recommendations() {
        let result = VFFilter.filter(
            &ScoredPostsQuery::default(),
            vec![
                PostCandidate {
                    tweet_id: 1,
                    in_network: Some(true),
                    visibility_decision: VisibilityDecision::Unavailable("vf down".to_string()),
                    ..Default::default()
                },
                PostCandidate {
                    tweet_id: 2,
                    in_network: Some(false),
                    visibility_decision: VisibilityDecision::Unavailable("vf down".to_string()),
                    ..Default::default()
                },
            ],
        );

        assert_eq!(result.kept[0].tweet_id, 1);
        assert_eq!(result.removed[0].tweet_id, 2);
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

        let result = VFFilter.filter(&ScoredPostsQuery::default(), vec![candidate]);
        assert_eq!(result.kept.len(), 1);
        assert!(result.removed.is_empty());
    }
}
