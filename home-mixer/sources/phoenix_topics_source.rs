use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::topic_retrieval_client::TopicRetrievalClient;
use crate::params;
use std::sync::Arc;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixTopicsSource {
    pub client: Arc<dyn TopicRetrievalClient>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixTopicsSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        (!query.topic_ids.is_empty() || !query.new_user_topic_ids.is_empty())
            && !query.in_network_only
            && !query.has_cached_posts
    }

    async fn get_candidates(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let topic_ids = if query.topic_ids.is_empty() {
            &query.new_user_topic_ids
        } else {
            &query.topic_ids
        };
        let posts = self
            .client
            .retrieve(query.user_id, topic_ids, params::TOPIC_MAX_RESULTS)
            .await?;

        Ok(posts
            .into_iter()
            .map(|post| PostCandidate {
                tweet_id: post.tweet_id,
                author_id: post.author_id,
                retrieval_topic_ids: post.matched_topic_ids,
                served_type: Some(pb::ServedType::ForYouPhoenixTopics),
                ..Default::default()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::topic_retrieval_client::DemoTopicRetrievalClient;

    #[test]
    fn new_user_topics_route_through_topic_source() {
        let source = PhoenixTopicsSource {
            client: Arc::new(DemoTopicRetrievalClient),
        };
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10, 20],
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(source.get_candidates(&query))
            .expect("topic source");

        assert_eq!(candidates.len(), params::TOPIC_MAX_RESULTS);
        assert_eq!(candidates[0].retrieval_topic_ids, vec![10]);
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouPhoenixTopics)
        );
    }
}
