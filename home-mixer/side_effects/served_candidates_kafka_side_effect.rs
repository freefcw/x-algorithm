//! 上游同构的最终下发候选发布 SideEffect（SE-11）。
//!
//! 上游把 timeline_logging thrift 记录发到 Kafka，用于训练样本与审计。本地把它
//! 拆成两层：本模块把领域对象映射成版本化的 [`ServedCandidatesEvent`]（曝光事件
//! schema 的唯一定义），[`ServedCandidatesSink`]（U1）只负责传输——topic、序列化、
//! 重试与幂等由 Adapter 承担（`clients/served_candidates_sink.rs`）。
//!
//! 装配即启用：上游按 `is_prod` + feature switch 决定是否发布，本地把这个决策放在
//! 装配层（配置了 sink 才装配），不再以 `is_shadow_traffic` 作为请求级门槛；影子
//! 流量以事件字段透传，由离线消费方决定是否纳入训练。事件记录的是**服务端下发**
//! 的最终列表（post-selection 过滤、截断之后），不等于客户端真实展示，客户端曝光
//! 仍需埋点回传后按 `request_id` + `post_id` 关联。
//!
//! 事件契约见 `docs/implementation/served-candidates-event-contract.md`。

use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

/// 曝光事件 schema 版本。字段只增不改；不兼容变更必须递增，消费方按版本解码。
pub const SERVED_CANDIDATES_EVENT_SCHEMA_VERSION: u8 = 1;

/// 一次 ScoredPosts 请求的服务端下发记录（一条事件 = 一次请求）。
///
/// 幂等键是 `request_id`；同一 `request_id` 重复投递时消费方按整条覆盖。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ServedCandidatesEvent {
    pub schema_version: u8,
    /// 请求标识，与服务端日志、`prediction_request_id` 和响应中的 `request_id` 一致。
    pub request_id: String,
    /// 发给 Phoenix 精排的请求 ID，用于把曝光和模型侧日志关联。
    pub prediction_request_id: u64,
    /// viewer 的皮 ID（24 位小写 hex ObjectId）。
    pub viewer_id: String,
    /// 请求时间（毫秒），与 served 历史落库使用的时间戳相同。
    pub request_time_ms: i64,
    pub is_shadow_traffic: bool,
    pub in_network_only: bool,
    pub is_bottom_request: bool,
    pub client_app_id: i32,
    /// 按下发顺序排列；`position` 从 0 开始，是最终列表中的名次。
    pub candidates: Vec<ServedCandidateRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ServedCandidateRecord {
    pub position: u32,
    /// 帖子 ID（24 位小写 hex ObjectId）。
    pub post_id: String,
    /// 作者皮 ID（`creator_member_id`）。
    pub author_id: String,
    /// 转帖时的原帖 ID；精排对转帖打的是原帖，训练关联时以此为模型侧键。
    pub retweeted_post_id: Option<String>,
    /// proto `ServedType` 枚举名，例如 `FOR_YOU_PHOENIX_RETRIEVAL`。
    pub served_type: Option<String>,
    pub in_network: Option<bool>,
    /// 参与选择的最终分。
    pub score: Option<f64>,
    /// 多目标加权分（多样性 / 网外降权之前）。
    pub weighted_score: Option<f64>,
    /// 非空表示这条候选不是模型排序的结果（整批规则兜底等）。
    pub degraded_reason: Option<String>,
    pub created_at_ms: Option<u64>,
}

impl ServedCandidatesEvent {
    pub fn from_served(query: &ScoredPostsQuery, candidates: &[PostCandidate]) -> Self {
        Self {
            schema_version: SERVED_CANDIDATES_EVENT_SCHEMA_VERSION,
            request_id: query.request_id.clone(),
            prediction_request_id: query.prediction_id,
            viewer_id: query.user_id.to_string(),
            request_time_ms: query.request_time_ms,
            is_shadow_traffic: query.is_shadow_traffic,
            in_network_only: query.in_network_only,
            is_bottom_request: query.is_bottom_request,
            client_app_id: query.client_app_id,
            candidates: candidates
                .iter()
                .enumerate()
                .map(|(position, candidate)| ServedCandidateRecord {
                    position: u32::try_from(position).unwrap_or(u32::MAX),
                    post_id: candidate.tweet_id.to_string(),
                    author_id: candidate.author_id.to_string(),
                    retweeted_post_id: candidate.retweeted_tweet_id.map(|id| id.to_string()),
                    served_type: candidate
                        .served_type
                        .map(|served_type| served_type.as_str_name().to_string()),
                    in_network: candidate.in_network,
                    score: candidate.score,
                    weighted_score: candidate.weighted_score,
                    degraded_reason: candidate.degraded_reason.clone(),
                    created_at_ms: candidate.created_at_ms,
                })
                .collect(),
        }
    }
}

/// 传输端口。实现负责 topic / 分区键 / 序列化 / 重试；schema 由事件类型固定。
#[async_trait]
pub trait ServedCandidatesSink: Send + Sync {
    async fn publish(&self, event: &ServedCandidatesEvent) -> Result<(), String>;

    /// 进程关停时调用一次：在 `timeout` 内把传输层缓冲的事件刷出去。默认无事可做
    /// （同步写文件、测试桩）；Kafka 实现在这里 flush producer，否则进程退出会丢掉
    /// 已 `publish` 成功返回之前尚在 librdkafka 队列里的消息。
    async fn shutdown(&self, _timeout: std::time::Duration) {}
}

pub struct ServedCandidatesKafkaSideEffect {
    sink: Arc<dyn ServedCandidatesSink>,
}

impl ServedCandidatesKafkaSideEffect {
    pub fn new(sink: Arc<dyn ServedCandidatesSink>) -> Self {
        Self { sink }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, PostCandidate> for ServedCandidatesKafkaSideEffect {
    /// 装配层决定是否发布；装配了就对每个请求生效（含影子流量）。
    fn enable(&self, _query: Arc<ScoredPostsQuery>) -> bool {
        true
    }

    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, PostCandidate>>,
    ) -> Result<(), String> {
        if input.selected_candidates.is_empty() {
            return Ok(());
        }

        let event = ServedCandidatesEvent::from_served(&input.query, &input.selected_candidates);
        self.sink
            .publish(&event)
            .await
            .map_err(|error| format!("Served-candidates publish failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{pid, uid};
    use std::sync::Mutex;
    use x_algorithm_proto::home_mixer as pb;

    #[derive(Default)]
    struct RecordingSink {
        published: Mutex<Vec<ServedCandidatesEvent>>,
    }

    #[async_trait]
    impl ServedCandidatesSink for RecordingSink {
        async fn publish(&self, event: &ServedCandidatesEvent) -> Result<(), String> {
            self.published
                .lock()
                .expect("publish lock")
                .push(event.clone());
            Ok(())
        }
    }

    fn query() -> ScoredPostsQuery {
        ScoredPostsQuery {
            user_id: uid(42),
            request_id: "req-1".to_string(),
            prediction_id: 77,
            request_time_ms: 1_700_000_000_000,
            client_app_id: 3,
            is_bottom_request: true,
            ..Default::default()
        }
    }

    fn candidates() -> Vec<PostCandidate> {
        vec![
            PostCandidate {
                tweet_id: pid(9),
                author_id: uid(8),
                served_type: Some(pb::ServedType::ForYouPhoenixRetrieval),
                in_network: Some(false),
                score: Some(0.75),
                weighted_score: Some(0.9),
                created_at_ms: Some(1_699_999_000_000),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: pid(10),
                author_id: uid(11),
                retweeted_tweet_id: Some(pid(5)),
                served_type: Some(pb::ServedType::ForYouInNetwork),
                in_network: Some(true),
                score: Some(0.5),
                degraded_reason: Some("phoenix_missing_sequence".to_string()),
                ..Default::default()
            },
        ]
    }

    #[tokio::test]
    async fn assembled_side_effect_publishes_every_request_including_shadow_traffic() {
        let sink = Arc::new(RecordingSink::default());
        let side_effect = ServedCandidatesKafkaSideEffect::new(
            Arc::clone(&sink) as Arc<dyn ServedCandidatesSink>
        );

        assert!(side_effect.enable(Arc::new(ScoredPostsQuery::default())));
        let shadow = ScoredPostsQuery {
            is_shadow_traffic: true,
            ..query()
        };
        assert!(side_effect.enable(Arc::new(shadow.clone())));

        let input = Arc::new(SideEffectInput {
            query: Arc::new(shadow),
            selected_candidates: candidates(),
            non_selected_candidates: Vec::new(),
        });
        side_effect.side_effect(input).await.expect("side effect");

        let published = sink.published.lock().expect("publish lock");
        assert_eq!(published.len(), 1);
        let event = &published[0];
        assert_eq!(event.schema_version, SERVED_CANDIDATES_EVENT_SCHEMA_VERSION);
        assert_eq!(event.request_id, "req-1");
        assert_eq!(event.prediction_request_id, 77);
        assert_eq!(event.viewer_id, uid(42).to_string());
        assert_eq!(event.request_time_ms, 1_700_000_000_000);
        assert!(event.is_shadow_traffic && event.is_bottom_request);
        assert_eq!(event.client_app_id, 3);
        assert_eq!(
            event.candidates,
            vec![
                ServedCandidateRecord {
                    position: 0,
                    post_id: pid(9).to_string(),
                    author_id: uid(8).to_string(),
                    retweeted_post_id: None,
                    served_type: Some("FOR_YOU_PHOENIX_RETRIEVAL".to_string()),
                    in_network: Some(false),
                    score: Some(0.75),
                    weighted_score: Some(0.9),
                    degraded_reason: None,
                    created_at_ms: Some(1_699_999_000_000),
                },
                ServedCandidateRecord {
                    position: 1,
                    post_id: pid(10).to_string(),
                    author_id: uid(11).to_string(),
                    retweeted_post_id: Some(pid(5).to_string()),
                    served_type: Some("FOR_YOU_IN_NETWORK".to_string()),
                    in_network: Some(true),
                    score: Some(0.5),
                    weighted_score: None,
                    degraded_reason: Some("phoenix_missing_sequence".to_string()),
                    created_at_ms: None,
                },
            ]
        );
    }

    #[tokio::test]
    async fn empty_responses_are_not_published() {
        let sink = Arc::new(RecordingSink::default());
        let side_effect = ServedCandidatesKafkaSideEffect::new(
            Arc::clone(&sink) as Arc<dyn ServedCandidatesSink>
        );
        let input = Arc::new(SideEffectInput {
            query: Arc::new(query()),
            selected_candidates: Vec::new(),
            non_selected_candidates: Vec::new(),
        });
        side_effect.side_effect(input).await.expect("side effect");
        assert!(sink.published.lock().expect("publish lock").is_empty());
    }

    #[test]
    fn event_json_is_the_stable_wire_shape() {
        let event = ServedCandidatesEvent::from_served(&query(), &candidates()[..1]);
        let json: serde_json::Value = serde_json::to_value(&event).expect("serialize");

        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["request_id"], "req-1");
        assert_eq!(json["viewer_id"], uid(42).to_string());
        let first = &json["candidates"][0];
        assert_eq!(first["position"], 0);
        assert_eq!(first["post_id"], pid(9).to_string());
        assert_eq!(first["served_type"], "FOR_YOU_PHOENIX_RETRIEVAL");
        // Optional fields stay present as null so consumers see one shape.
        assert!(first["degraded_reason"].is_null());
        assert!(first["retweeted_post_id"].is_null());

        let decoded: ServedCandidatesEvent = serde_json::from_value(json).expect("round trip");
        assert_eq!(decoded, event);
    }
}
