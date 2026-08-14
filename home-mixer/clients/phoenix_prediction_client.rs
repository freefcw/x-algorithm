// Phoenix 精排预测客户端
//
// 替代原始被阉割的 Phoenix 预测客户端模块。
//
// 原始功能说明：
// PhoenixPredictionClient 是 Home Mixer 与 Phoenix 精排模型服务
// 之间的 gRPC 客户端。它负责：
//   1. 将候选帖子列表和用户行为序列发送给 Phoenix
//   2. Phoenix 运行 Grok Transformer 模型，预估用户对每条帖子
//      执行各种互动行为的概率
//   3. 返回预测结果，供 RankingScorer 计算最终排序分数
//
// 预测输入：
//   - user_id: 当前用户
//   - user_action_sequence: 用户最近的行为序列（特征）
//   - candidates: 待评分的帖子列表
//
// 预测输出：
//   - PredictNextActionsResponse: 每条帖子上各行为的概率分布
//
// 连接方式：
//   - 设置环境变量 PHOENIX_PREDICT_GRPC_ADDR（如 http://localhost:50053）
//     时，走真实 gRPC 调用 PhoenixPredictionService.PredictNextActions；
//   - 未设置时返回显式不可用错误，由 Scorer 隔离并保留规则排序。

use log::{info, warn};
use std::time::Duration;
use tonic::async_trait;
use tonic::transport::Channel;
use x_algorithm_proto::recsys;
use x_algorithm_proto::recsys::phoenix_prediction_service_client::PhoenixPredictionServiceClient;

/// Phoenix 精排预测客户端 trait
///
/// 定义了调用 Phoenix 精排模型的标准接口。
/// Home Mixer 的 PhoenixScorer 通过此 trait 获取模型预测结果。
#[async_trait]
pub trait PhoenixPredictionClient: Send + Sync {
    /// 调用 Phoenix 精排模型进行预测
    ///
    /// # Arguments
    /// * `user_id` - 目标用户 ID
    /// * `sequence` - 用户最近的行为序列（模型的核心输入特征）
    /// * `candidates` - 待评分的候选帖子列表
    ///
    /// # Returns
    /// 预测响应，包含每条帖子上各行为的概率分布
    async fn predict(
        &self,
        user_id: u64,
        sequence: recsys::UserActionSequence,
        candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error>;
}

pub async fn predict_with_timeout(
    client: &(dyn PhoenixPredictionClient + Send + Sync),
    user_id: u64,
    sequence: recsys::UserActionSequence,
    candidates: Vec<recsys::TweetInfo>,
    timeout: Duration,
) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
    tokio::time::timeout(timeout, client.predict(user_id, sequence, candidates))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Phoenix prediction timed out after {}ms",
                timeout.as_millis()
            )
        })?
}

/// 生产环境 Phoenix 精排客户端
///
/// 设置 `PHOENIX_PREDICT_GRPC_ADDR` 后调用真实的 Phoenix gRPC 服务；
/// 未设置时返回显式不可用错误，由 Scorer 记录并保留规则 fallback。
pub struct ProdPhoenixPredictionClient {
    channel: Option<Channel>,
}

impl ProdPhoenixPredictionClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let channel = match std::env::var("PHOENIX_PREDICT_GRPC_ADDR") {
            Ok(addr) => {
                info!("PhoenixPredictionClient: connecting to {}", addr);
                Some(Channel::from_shared(addr)?.connect_lazy())
            }
            Err(_) => {
                warn!(
                    "PhoenixPredictionClient: PHOENIX_PREDICT_GRPC_ADDR not set; prediction is unavailable"
                );
                None
            }
        };
        Ok(Self { channel })
    }
}

#[async_trait]
impl PhoenixPredictionClient for ProdPhoenixPredictionClient {
    async fn predict(
        &self,
        user_id: u64,
        sequence: recsys::UserActionSequence,
        candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
        let Some(channel) = &self.channel else {
            anyhow::bail!("PHOENIX_PREDICT_GRPC_ADDR is not configured");
        };

        let mut client = PhoenixPredictionServiceClient::new(channel.clone());
        let request = recsys::PredictNextActionsRequest {
            user_id,
            user_action_sequence: Some(sequence),
            candidates,
        };
        let response = client.predict_next_actions(request).await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SlowPredictionClient;

    #[async_trait]
    impl PhoenixPredictionClient for SlowPredictionClient {
        async fn predict(
            &self,
            _user_id: u64,
            _sequence: recsys::UserActionSequence,
            _candidates: Vec<recsys::TweetInfo>,
        ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Default::default())
        }
    }

    #[tokio::test]
    async fn missing_prediction_endpoint_is_explicitly_unavailable() {
        let error = ProdPhoenixPredictionClient { channel: None }
            .predict(1, Default::default(), Vec::new())
            .await
            .expect_err("missing endpoint must not report successful prediction");

        assert!(error.to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn prediction_deadline_bounds_slow_adapter() {
        let error = predict_with_timeout(
            &SlowPredictionClient,
            1,
            Default::default(),
            Vec::new(),
            Duration::from_millis(1),
        )
        .await
        .expect_err("slow prediction must time out");

        assert!(error.to_string().contains("timed out"));
    }
}
