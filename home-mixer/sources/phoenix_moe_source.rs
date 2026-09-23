use crate::clients::phoenix_retrieval_client::{retrieve_with_timeout, PhoenixRetrievalClient};
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params;
use crate::sources::phoenix_source::candidates_from_retrieval_response;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixMoeSource {
    pub phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixMoeSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.enable_phoenix_moe
            && !matches!(
                query.topic_recall_mode(),
                TopicRecallMode::Strict | TopicRecallMode::ColdStart
            )
            && !query.in_network_only
            && !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let sequence = query
            .retrieval_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
            .ok_or_else(|| "PhoenixMoeSource: missing retrieval sequence".to_string())?;
        let response = retrieve_with_timeout(
            self.phoenix_retrieval_client.as_ref(),
            query.user_id,
            sequence.clone(),
            params::PHOENIX_MOE_MAX_RESULTS,
            Duration::from_millis(params::PHOENIX_RETRIEVAL_TIMEOUT_MS),
        )
        .await
        .map_err(|error| format!("PhoenixMoeSource: {error}"))?;

        Ok(candidates_from_retrieval_response(
            response,
            pb::ServedType::ForYouPhoenixRetrievalMoe,
            "PhoenixMoeSource",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::recsys;

    struct FakeRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for FakeRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: crate::models::UserId,
            _sequence: recsys::UserActionSequence,
            max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            assert_eq!(max_results, params::PHOENIX_MOE_MAX_RESULTS);
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![recsys::ScoredCandidate {
                        candidate: Some(recsys::TweetInfo {
                            tweet_id: 100,
                            author_id: 200,
                            ..Default::default()
                        }),
                        score: 0.9,
                        source_idx: Some(0),
                        dataset_type: Some(1),
                    }],
                }],
            })
        }
    }

    #[test]
    fn enabled_moe_source_marks_its_candidates() {
        let source = PhoenixMoeSource {
            phoenix_retrieval_client: Arc::new(FakeRetrievalClient),
        };
        let query = ScoredPostsQuery {
            enable_phoenix_moe: true,
            retrieval_sequence: Some(recsys::UserActionSequence::default()),
            ..ScoredPostsQuery::test_default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(source.source(&query))
            .expect("MoE candidates");

        assert!(source.enable(&query));
        assert_eq!(candidates[0].tweet_id, crate::models::pid(100));
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouPhoenixRetrievalMoe)
        );
        assert_eq!(candidates[0].retrieval_sources[0].score, Some(0.9));
        assert_eq!(candidates[0].retrieval_sources[0].source_idx, Some(0));
    }

    #[test]
    fn new_user_topics_disable_moe_retrieval() {
        let source = PhoenixMoeSource {
            phoenix_retrieval_client: Arc::new(FakeRetrievalClient),
        };
        let query = ScoredPostsQuery {
            enable_phoenix_moe: true,
            new_user_topic_ids: vec![10],
            ..ScoredPostsQuery::test_default()
        };

        assert!(!source.enable(&query));
    }
}
