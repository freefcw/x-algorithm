// Phoenix 双塔召回客户端
//
// 替代原始被阉割的 Phoenix 召回客户端模块。
//
// 原始功能说明：
// PhoenixRetrievalClient 是 Home Mixer 与 Phoenix 双塔召回模型服务
// 之间的 gRPC 客户端。它负责：
//   1. 将用户行为序列发送给 Phoenix
//   2. Phoenix 使用 User Tower 计算用户向量表示
//   3. 在全局帖子索引（Item Tower 预计算）中做最近邻搜索
//   4. 返回与用户兴趣最匹配的 top-K 全局候选帖子
//
// 这条召回路与 Thunder 的网络内召回互补：
//   - Thunder: 返回用户关注者的帖子（In-Network）
//   - Phoenix Retrieval: 返回全局相关帖子（Out-of-Network）
//
// 连接方式：
//   - 设置环境变量 PHOENIX_RETRIEVAL_GRPC_ADDR（如 http://localhost:50053）
//     时，走真实 gRPC 调用 PhoenixRetrievalService.Retrieve；
//   - 未设置时返回显式不可用错误，由 Source 隔离并保留其他召回路。

use crate::clients::phoenix_prediction_client::validate_serving_metadata;
use crate::metrics::ClientCallRecorder;
use crate::models::ids::UserId;
use log::{info, warn};
use std::time::{Duration, Instant};
use tonic::async_trait;
use tonic::transport::Channel;
use x_algorithm_proto::recsys;
use x_algorithm_proto::recsys::phoenix_retrieval_service_client::PhoenixRetrievalServiceClient;

/// Phoenix 双塔召回客户端 trait
///
/// 定义了调用 Phoenix 双塔召回模型的标准接口。
/// Home Mixer 的 PhoenixSource 通过此 trait 获取全局候选帖子。
#[async_trait]
pub trait PhoenixRetrievalClient: Send + Sync {
    /// 调用 Phoenix 双塔召回模型
    ///
    /// # Arguments
    /// * `user_id` - 目标用户 ID
    /// * `sequence` - 用户最近的行为序列（User Tower 的输入）
    /// * `max_results` - 最大召回数量
    ///
    /// # Returns
    /// 召回响应，包含按相似度排序的候选帖子列表
    async fn retrieve(
        &self,
        user_id: UserId,
        sequence: recsys::UserActionSequence,
        max_results: u32,
    ) -> Result<recsys::RetrieveResponse, anyhow::Error>;
}

pub async fn retrieve_with_timeout(
    client: &(dyn PhoenixRetrievalClient + Send + Sync),
    user_id: UserId,
    sequence: recsys::UserActionSequence,
    max_results: u32,
    timeout: Duration,
) -> Result<recsys::RetrieveResponse, anyhow::Error> {
    tokio::time::timeout(timeout, client.retrieve(user_id, sequence, max_results))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Phoenix retrieval timed out after {}ms",
                timeout.as_millis()
            )
        })?
}

/// 生产环境 Phoenix 召回客户端
///
/// 设置 `PHOENIX_RETRIEVAL_GRPC_ADDR` 后调用真实的 Phoenix Retrieval gRPC 服务；
/// 未设置时返回显式不可用错误，由 Source 记录并跳过该召回路。
pub struct ProdPhoenixRetrievalClient {
    channel: Option<Channel>,
    pub allow_random: bool,
    calls: ClientCallRecorder,
}

impl ProdPhoenixRetrievalClient {
    pub fn from_addr(addr: String) -> Result<Self, anyhow::Error> {
        Self::from_addr_with_allow_random(addr, false)
    }

    pub fn from_addr_with_allow_random(
        addr: String,
        allow_random: bool,
    ) -> Result<Self, anyhow::Error> {
        info!("PhoenixRetrievalClient: connecting to {}", addr);
        Ok(Self {
            channel: Some(Channel::from_shared(addr)?.connect_lazy()),
            allow_random,
            calls: ClientCallRecorder::default(),
        })
    }

    pub async fn new() -> Result<Self, anyhow::Error> {
        Self::new_with_allow_random(false).await
    }

    pub async fn new_with_allow_random(allow_random: bool) -> Result<Self, anyhow::Error> {
        let channel = match std::env::var("PHOENIX_RETRIEVAL_GRPC_ADDR") {
            Ok(addr) => return Self::from_addr_with_allow_random(addr, allow_random),
            Err(_) => {
                warn!(
                    "PhoenixRetrievalClient: PHOENIX_RETRIEVAL_GRPC_ADDR not set; retrieval is unavailable"
                );
                None
            }
        };
        Ok(Self {
            channel,
            allow_random,
            calls: ClientCallRecorder::default(),
        })
    }

    /// Attach the process call metrics; the default records nothing.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }
}

#[async_trait]
impl PhoenixRetrievalClient for ProdPhoenixRetrievalClient {
    async fn retrieve(
        &self,
        user_id: UserId,
        sequence: recsys::UserActionSequence,
        max_results: u32,
    ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
        let Some(channel) = &self.channel else {
            anyhow::bail!("PHOENIX_RETRIEVAL_GRPC_ADDR is not configured");
        };

        let mut client = PhoenixRetrievalServiceClient::new(channel.clone());
        let request = recsys::RetrieveRequest {
            user_id,
            user_action_sequence: Some(sequence),
            max_results,
        };

        let started = Instant::now();
        let response = match client.retrieve(request).await {
            Ok(response) => response,
            Err(status) => {
                warn!(
                    "phoenix rpc Retrieve max_results={max_results} elapsed_ms={} code={} error={}",
                    started.elapsed().as_millis(),
                    status.code(),
                    status.message(),
                );
                self.calls
                    .record("phoenix_retrieval", "Retrieve", "error", started);
                return Err(status.into());
            }
        };
        // 网络耗时与契约校验耗时分开：调用慢和模型答非所问是两种故障。
        let elapsed_ms = started.elapsed().as_millis();

        if let Err(error) = validate_serving_metadata(response.metadata(), self.allow_random) {
            warn!(
                "phoenix rpc Retrieve max_results={max_results} elapsed_ms={elapsed_ms} rejected={error:#}"
            );
            self.calls
                .record("phoenix_retrieval", "Retrieve", "rejected", started);
            return Err(error);
        }
        let inner = response.into_inner();
        if let Err(error) = validate_retrieve_response(max_results, &inner) {
            warn!(
                "phoenix rpc Retrieve max_results={max_results} elapsed_ms={elapsed_ms} rejected={error:#}"
            );
            self.calls
                .record("phoenix_retrieval", "Retrieve", "rejected", started);
            return Err(error);
        }

        info!(
            "phoenix rpc Retrieve max_results={max_results} elapsed_ms={elapsed_ms} candidates={}",
            inner
                .top_k_candidates
                .iter()
                .map(|group| group.candidates.len())
                .sum::<usize>(),
        );
        self.calls
            .record("phoenix_retrieval", "Retrieve", "ok", started);
        Ok(inner)
    }
}

/// Adapter-internal validation, mirroring `validate_predict_response`.
/// PhoenixSource only sees `Result`.
fn validate_retrieve_response(
    max_results: u32,
    inner: &recsys::RetrieveResponse,
) -> Result<(), anyhow::Error> {
    let returned: Vec<_> = inner
        .top_k_candidates
        .iter()
        .flat_map(|group| group.candidates.iter())
        .collect();
    anyhow::ensure!(
        returned.len() <= max_results as usize,
        "Phoenix retrieval returned more candidates than requested"
    );
    let mut ids = std::collections::HashSet::new();
    for scored in &returned {
        let Some(candidate) = scored.candidate.as_ref() else {
            anyhow::bail!("Phoenix retrieval response has a candidate without TweetInfo");
        };
        anyhow::ensure!(
            candidate.tweet_id != 0,
            "Phoenix retrieval response has empty tweet_id"
        );
        anyhow::ensure!(
            ids.insert(candidate.tweet_id),
            "Phoenix retrieval response has duplicate tweet_id {}",
            candidate.tweet_id
        );
    }
    if returned.iter().any(|scored| !scored.score.is_finite()) {
        anyhow::bail!("Phoenix retrieval response contains NaN/Inf score");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SlowRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for SlowRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: UserId,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Default::default())
        }
    }

    #[tokio::test]
    async fn missing_retrieval_endpoint_is_explicitly_unavailable() {
        let error = ProdPhoenixRetrievalClient {
            channel: None,
            allow_random: false,
            calls: ClientCallRecorder::default(),
        }
        .retrieve(crate::models::uid(1), Default::default(), 10)
        .await
        .expect_err("missing endpoint must not report successful retrieval");

        assert!(error.to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn retrieval_deadline_bounds_slow_adapter() {
        let error = retrieve_with_timeout(
            &SlowRetrievalClient,
            crate::models::uid(1),
            Default::default(),
            10,
            Duration::from_millis(1),
        )
        .await
        .expect_err("slow retrieval must time out");

        assert!(error.to_string().contains("timed out"));
    }
}
