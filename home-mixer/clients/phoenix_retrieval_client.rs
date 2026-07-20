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
//   - 未设置时退化为 stub，返回空候选列表（Feed 中只有网内帖子）。

use log::{info, warn};
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
        user_id: u64,
        sequence: recsys::UserActionSequence,
        max_results: u32,
    ) -> Result<recsys::RetrieveResponse, anyhow::Error>;
}

/// 生产环境 Phoenix 召回客户端
///
/// 设置 `PHOENIX_RETRIEVAL_GRPC_ADDR` 后调用真实的 Phoenix Retrieval gRPC 服务；
/// 未设置时退化为 stub（返回空候选，Feed 中只有 Thunder 网内帖子）。
pub struct ProdPhoenixRetrievalClient {
    channel: Option<Channel>,
}

impl ProdPhoenixRetrievalClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let channel = match std::env::var("PHOENIX_RETRIEVAL_GRPC_ADDR") {
            Ok(addr) => {
                info!("PhoenixRetrievalClient: connecting to {}", addr);
                Some(Channel::from_shared(addr)?.connect_lazy())
            }
            Err(_) => {
                warn!(
                    "PhoenixRetrievalClient: PHOENIX_RETRIEVAL_GRPC_ADDR not set, \
                     using stub (no out-of-network candidates)"
                );
                None
            }
        };
        Ok(Self { channel })
    }
}

#[async_trait]
impl PhoenixRetrievalClient for ProdPhoenixRetrievalClient {
    async fn retrieve(
        &self,
        user_id: u64,
        sequence: recsys::UserActionSequence,
        max_results: u32,
    ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
        let Some(channel) = &self.channel else {
            return Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![],
            });
        };

        let mut client = PhoenixRetrievalServiceClient::new(channel.clone());
        let request = recsys::RetrieveRequest {
            user_id,
            user_action_sequence: Some(sequence),
            max_results,
        };
        let response = client.retrieve(request).await?;
        Ok(response.into_inner())
    }
}
