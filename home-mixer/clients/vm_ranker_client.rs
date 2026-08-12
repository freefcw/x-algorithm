//! VM Ranker 客户端端口（RANK-03）。
//!
//! 上游 `scorers/vm_ranker.rs` 通过 `xai_vm_ranker_proto` 调用内部 VM Ranker
//! gRPC 服务，并从 feature switch 读取集群、value model、DPP 等参数。
//! 该服务没有公开的 RPC 或模型合同，因此这里只定义领域级端口（对暂缓
//! 能力 U3 的 U1 接缝）：真实 Adapter 必须自行负责传输、认证、集群选择、
//! 超时与线上 schema，之后 `VMRanker` Scorer 才能进入装配。

use crate::models::candidate::PhoenixScores;
use tonic::async_trait;

/// 对应上游 `RankCandidate`。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankCandidate {
    pub tweet_id: u64,
    pub author_id: u64,
    pub in_network: bool,
    pub is_retweet: bool,
    pub is_reply: bool,
    pub author_followers_count: i32,
    pub vqv_ineligible: bool,
    pub retweeted_tweet_id: Option<u64>,
    pub score: Option<f64>,
    pub phoenix_scores: PhoenixScores,
}

/// 对应上游 `DppParams`（多样性重排参数）。
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DppParams {
    pub theta: f64,
    pub max_selected_rank: u32,
}

/// 对应上游 `RankRequest`。上游从私有 feature switch 读取的 value model、
/// DPP 与新用户阈值改由装配/Adapter 配置显式传入。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankRequest {
    pub viewer_id: u64,
    pub request_timestamp_ms: i64,
    pub viewer_following_count: usize,
    pub value_model_id: Option<String>,
    pub dpp_params: Option<DppParams>,
    pub candidates: Vec<VmRankCandidate>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankedCandidate {
    pub tweet_id: u64,
    pub score: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmRankResponse {
    pub candidates: Vec<VmRankedCandidate>,
}

#[async_trait]
pub trait VMRankerClient: Send + Sync {
    async fn rank(&self, request: VmRankRequest) -> Result<VmRankResponse, String>;
}
