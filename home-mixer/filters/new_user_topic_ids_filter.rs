use crate::filters::topic_ids_filter::TopicIdExpansion;
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct NewUserTopicIdsFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for NewUserTopicIdsFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.topic_recall_mode() == TopicRecallMode::ColdStart
    }

    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let expanded =
            TopicIdExpansion::expand(&query.new_user_topic_ids.iter().copied().collect());

        let (kept, removed) = candidates.into_iter().partition(|candidate| {
            candidate.in_network == Some(true)
                || candidate
                    .filtered_topic_ids
                    .iter()
                    .any(|topic_id| expanded.contains(topic_id))
        });

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_in_network_or_matching_candidates_for_new_users() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                in_network: Some(true),
                filtered_topic_ids: vec![30],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                in_network: Some(false),
                filtered_topic_ids: vec![10],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3.into(),
                in_network: Some(false),
                filtered_topic_ids: vec![30],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 4.into(),
                in_network: Some(false),
                retrieval_topic_ids: vec![10],
                ..Default::default()
            },
        ];

        let result = NewUserTopicIdsFilter.filter(&query, candidates);

        assert_eq!(
            result
                .kept
                .iter()
                .map(|candidate| candidate.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(1), crate::models::pid(2)]
        );
        assert_eq!(result.removed.len(), 2);
    }
}
