use crate::clients::phoenix_retrieval_client::{retrieve_with_timeout, PhoenixRetrievalClient};
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params;
// TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
use crate::sources::phoenix_source::parse_bridged_tweet_info;
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
            params::PHOENIX_MAX_RESULTS,
            Duration::from_millis(params::PHOENIX_RETRIEVAL_TIMEOUT_MS),
        )
        .await
        .map_err(|error| format!("PhoenixMoeSource: {error}"))?;

        // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
        // 协议里的 ID 是字符串；这里只接受十进制 u64，其余候选整条丢弃并计数告警。
        let mut unparsable_ids = 0usize;
        let mut unparsable_example: Option<String> = None;
        let candidates: Vec<PostCandidate> = response
            .top_k_candidates
            .into_iter()
            .flat_map(|group| group.candidates)
            .filter_map(|candidate| candidate.candidate)
            .filter_map(|tweet| {
                let parsed = parse_bridged_tweet_info(&tweet);
                if parsed.is_none() {
                    unparsable_ids += 1;
                    unparsable_example.get_or_insert_with(|| tweet.tweet_id.clone());
                }
                parsed
            })
            .map(
                |(tweet_id, author_id, in_reply_to_tweet_id)| PostCandidate {
                    tweet_id,
                    author_id,
                    in_reply_to_tweet_id,
                    served_type: Some(pb::ServedType::ForYouPhoenixRetrievalMoe),
                    ..Default::default()
                },
            )
            .collect();
        if unparsable_ids > 0 {
            log::warn!(
                "PhoenixMoeSource: dropped {} retrieved candidate(s) whose ids are not decimal u64 (e.g. {:?})",
                unparsable_ids,
                unparsable_example.unwrap_or_default()
            );
        }

        Ok(candidates)
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
            _user_id: u64,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![recsys::ScoredCandidate {
                        candidate: Some(recsys::TweetInfo {
                            // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
                            tweet_id: "100".to_string(),
                            author_id: "200".to_string(),
                            ..Default::default()
                        }),
                        score: 0.9,
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
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(source.source(&query))
            .expect("MoE candidates");

        assert!(source.enable(&query));
        assert_eq!(candidates[0].tweet_id, 100);
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouPhoenixRetrievalMoe)
        );
    }

    #[test]
    fn new_user_topics_disable_moe_retrieval() {
        let source = PhoenixMoeSource {
            phoenix_retrieval_client: Arc::new(FakeRetrievalClient),
        };
        let query = ScoredPostsQuery {
            enable_phoenix_moe: true,
            new_user_topic_ids: vec![10],
            ..Default::default()
        };

        assert!(!source.enable(&query));
    }
}
