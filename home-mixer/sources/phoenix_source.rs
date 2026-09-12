use crate::clients::phoenix_retrieval_client::{retrieve_with_timeout, PhoenixRetrievalClient};
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params as p;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixSource {
    pub phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.in_network_only
            && !query.has_cached_posts
            && !matches!(
                query.topic_recall_mode(),
                TopicRecallMode::Strict | TopicRecallMode::ColdStart
            )
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let user_id = query.user_id;

        let sequence = query
            .retrieval_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
            .ok_or_else(|| "PhoenixSource: missing retrieval sequence".to_string())?;

        let response = retrieve_with_timeout(
            self.phoenix_retrieval_client.as_ref(),
            user_id,
            sequence.clone(),
            p::PHOENIX_MAX_RESULTS,
            Duration::from_millis(p::PHOENIX_RETRIEVAL_TIMEOUT_MS),
        )
        .await
        .map_err(|e| format!("PhoenixSource: {e}"))?;

        // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
        // 协议里的 ID 是字符串；这里只接受十进制 u64，其余候选整条丢弃并计数告警。
        // 已知后果：演示网关的召回语料 ID 是 24 位 hex，P0 阶段本 Source 在演示模式下返回 0 条。
        let mut unparsable_ids = 0usize;
        let mut unparsable_example: Option<String> = None;
        let candidates: Vec<PostCandidate> = response
            .top_k_candidates
            .into_iter()
            .flat_map(|scored_candidates| scored_candidates.candidates)
            .filter_map(|scored_candidate| scored_candidate.candidate)
            .filter_map(|tweet_info| {
                let parsed = parse_bridged_tweet_info(&tweet_info);
                if parsed.is_none() {
                    unparsable_ids += 1;
                    unparsable_example.get_or_insert_with(|| tweet_info.tweet_id.clone());
                }
                parsed
            })
            .map(
                |(tweet_id, author_id, in_reply_to_tweet_id)| PostCandidate {
                    tweet_id,
                    author_id,
                    in_reply_to_tweet_id,
                    served_type: Some(pb::ServedType::ForYouPhoenixRetrieval),
                    ..Default::default()
                },
            )
            .collect();
        if unparsable_ids > 0 {
            log::warn!(
                "PhoenixSource: dropped {} retrieved candidate(s) whose ids are not decimal u64 (e.g. {:?})",
                unparsable_ids,
                unparsable_example.unwrap_or_default()
            );
        }

        Ok(candidates)
    }
}

/// TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
///
/// 把 Phoenix 返回的字符串 ID 解析为流水线内部的 u64：`tweet_id` / `author_id` 必须是十进制
/// u64；`in_reply_to_tweet_id` 空串或 "0" 表示"不是回复"（沿用旧协议"0 = 非回复"的语义，
/// 直接 Some(0) 会让下游把它当成真实祖先帖），否则也必须能解析。
/// 任一字段解析失败返回 `None`，由调用方丢弃该候选——绝不回退成 0。
pub(crate) fn parse_bridged_tweet_info(
    tweet_info: &x_algorithm_proto::recsys::TweetInfo,
) -> Option<(u64, u64, Option<u64>)> {
    let tweet_id = tweet_info.tweet_id.parse::<u64>().ok()?;
    let author_id = tweet_info.author_id.parse::<u64>().ok()?;
    let in_reply_to_tweet_id = if tweet_info.in_reply_to_tweet_id.is_empty() {
        None
    } else {
        tweet_info
            .in_reply_to_tweet_id
            .parse::<u64>()
            .ok()
            .map(|id| (id != 0).then_some(id))?
    };
    Some((tweet_id, author_id, in_reply_to_tweet_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::recsys;

    struct UnusedRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for UnusedRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: u64,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            unreachable!("enablement tests do not retrieve candidates")
        }
    }

    fn source() -> PhoenixSource {
        PhoenixSource {
            phoenix_retrieval_client: Arc::new(UnusedRetrievalClient),
        }
    }

    // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
    struct MixedIdRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for MixedIdRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: u64,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            let tweet =
                |tweet_id: &str, author_id: &str, in_reply_to: &str| recsys::ScoredCandidate {
                    candidate: Some(recsys::TweetInfo {
                        tweet_id: tweet_id.to_string(),
                        author_id: author_id.to_string(),
                        in_reply_to_tweet_id: in_reply_to.to_string(),
                        ..Default::default()
                    }),
                    score: 0.5,
                };
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![
                        tweet("100", "200", ""),
                        tweet("101", "200", "0"),
                        tweet("102", "200", "99"),
                        // 24 位 hex（ObjectId 形状）：演示网关语料的真实形态，必须被丢弃。
                        tweet("5f1a2b3c4d5e6f7a8b9c0d1e", "200", ""),
                        tweet("103", "not-a-u64", ""),
                        tweet("104", "200", "not-a-u64"),
                    ],
                }],
            })
        }
    }

    // TEMP(U4-P1): 十进制 u64 桥接，P1 迁移到 PostId 后删除
    #[tokio::test]
    async fn non_decimal_ids_are_dropped_instead_of_becoming_zero() {
        let source = PhoenixSource {
            phoenix_retrieval_client: Arc::new(MixedIdRetrievalClient),
        };
        let query = ScoredPostsQuery {
            retrieval_sequence: Some(recsys::UserActionSequence::default()),
            ..Default::default()
        };

        let candidates = source.source(&query).await.expect("retrieval succeeds");

        let ids: Vec<(u64, u64, Option<u64>)> = candidates
            .iter()
            .map(|c| (c.tweet_id, c.author_id, c.in_reply_to_tweet_id))
            .collect();
        assert_eq!(
            ids,
            vec![(100, 200, None), (101, 200, None), (102, 200, Some(99))]
        );
        assert!(candidates
            .iter()
            .all(|c| c.tweet_id != 0 && c.author_id != 0));
        assert!(candidates
            .iter()
            .all(|c| c.served_type == Some(pb::ServedType::ForYouPhoenixRetrieval)));
    }

    #[test]
    fn supplemental_topics_keep_standard_retrieval_enabled() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            ..Default::default()
        };

        assert!(source().enable(&query));
    }

    #[test]
    fn new_user_topics_disable_standard_retrieval() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..Default::default()
        };

        assert!(!source().enable(&query));
    }

    #[test]
    fn requested_topic_page_disables_standard_retrieval() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10],
            ..Default::default()
        };

        assert!(!source().enable(&query));
    }
}
