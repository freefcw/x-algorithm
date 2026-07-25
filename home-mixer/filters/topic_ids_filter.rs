use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use std::collections::HashSet;
use tonic::async_trait;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct TopicIdsFilter;

#[async_trait]
impl Filter<ScoredPostsQuery, PostCandidate> for TopicIdsFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.topic_ids.is_empty()
            || !query.excluded_topic_ids.is_empty()
            || !query.new_user_topic_ids.is_empty()
    }

    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> Result<FilterResult<PostCandidate>, String> {
        let included: HashSet<i64> = query
            .topic_ids
            .iter()
            .chain(&query.new_user_topic_ids)
            .copied()
            .collect();
        let excluded: HashSet<i64> = query.excluded_topic_ids.iter().copied().collect();

        let (kept, removed) = candidates.into_iter().partition(|candidate| {
            let content_topics = if candidate.filtered_topic_ids.is_empty() {
                &candidate.unfiltered_topic_ids
            } else {
                &candidate.filtered_topic_ids
            };
            let mut candidate_topics = candidate.retrieval_topic_ids.iter().chain(content_topics);
            let includes_requested = included.is_empty()
                || candidate_topics
                    .clone()
                    .any(|topic| included.contains(topic));
            let includes_excluded = candidate_topics.any(|topic| excluded.contains(topic));
            includes_requested && !includes_excluded
        });
        Ok(FilterResult { kept, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_requested_topics_and_removes_excluded_topics() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10, 20],
            excluded_topic_ids: vec![99],
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                filtered_topic_ids: vec![10],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                filtered_topic_ids: vec![30],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3,
                filtered_topic_ids: vec![20, 99],
                ..Default::default()
            },
        ];
        let result = TopicIdsFilter
            .filter(&query, candidates)
            .expect("topic filter");

        assert_eq!(
            result
                .kept
                .iter()
                .map(|candidate| candidate.tweet_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(result.removed.len(), 2);
    }
}
