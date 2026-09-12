use crate::clients::topic_retrieval_client::TopicRetrievalClient;
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixTopicsSource {
    pub client: Arc<dyn TopicRetrievalClient>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixTopicsSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.topic_recall_mode() != TopicRecallMode::None
            && !query.in_network_only
            && !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let posts = retrieve_topics_with_timeout(
            self.client.as_ref(),
            query.user_id,
            query.selected_topic_ids(),
            params::TOPIC_MAX_RESULTS,
            Duration::from_millis(params::TOPIC_RETRIEVAL_TIMEOUT_MS),
        )
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

async fn retrieve_topics_with_timeout(
    client: &dyn TopicRetrievalClient,
    user_id: crate::models::UserId,
    topic_ids: &[i64],
    max_results: usize,
    timeout: Duration,
) -> Result<Vec<crate::clients::topic_retrieval_client::TopicPost>, String> {
    tokio::time::timeout(timeout, client.retrieve(user_id, topic_ids, max_results))
        .await
        .map_err(|_| {
            format!(
                "PhoenixTopicsSource: timed out after {}ms",
                timeout.as_millis()
            )
        })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::topic_retrieval_client::DemoTopicRetrievalClient;

    #[tokio::test]
    async fn topic_deadline_bounds_slow_adapter() {
        struct SlowTopicClient;

        #[async_trait]
        impl TopicRetrievalClient for SlowTopicClient {
            async fn retrieve(
                &self,
                _user_id: crate::models::UserId,
                _topic_ids: &[i64],
                _max_results: usize,
            ) -> Result<Vec<crate::clients::topic_retrieval_client::TopicPost>, String>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(Vec::new())
            }
        }

        let error = retrieve_topics_with_timeout(
            &SlowTopicClient,
            crate::models::uid(1),
            &[10],
            10,
            Duration::from_millis(1),
        )
        .await
        .expect_err("slow topic retrieval must time out");

        assert!(error.contains("timed out"));
    }

    #[test]
    fn supplemental_topics_route_through_topic_source() {
        let source = PhoenixTopicsSource {
            client: Arc::new(DemoTopicRetrievalClient),
        };
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10, 20],
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(source.source(&query))
            .expect("topic source");

        assert_eq!(candidates.len(), params::TOPIC_MAX_RESULTS);
        assert_eq!(candidates[0].retrieval_topic_ids, vec![10]);
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouPhoenixTopics)
        );
    }
}
