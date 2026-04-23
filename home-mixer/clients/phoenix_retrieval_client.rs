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
// 当前为 stub 实现，返回空候选列表。
// TODO: 当 Phoenix 模型训练完成后，连接真实的 Phoenix Retrieval gRPC 服务

use tonic::async_trait;
use x_algorithm_proto::recsys;

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

/// 生产环境 Phoenix 召回客户端（Stub 实现）
///
/// 当前返回空候选列表（即不产生 Out-of-Network 候选帖子）。
/// 这意味着 Feed 中只包含用户关注者的帖子（来自 Thunder）。
///
/// 当 Phoenix 模型训练完成后，替换为实际的 gRPC 客户端。
pub struct ProdPhoenixRetrievalClient;

impl ProdPhoenixRetrievalClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        // TODO: 从环境变量读取 Phoenix retrieval 服务地址
        Ok(Self)
    }
}

#[async_trait]
impl PhoenixRetrievalClient for ProdPhoenixRetrievalClient {
    async fn retrieve(
        &self,
        _user_id: u64,
        _sequence: recsys::UserActionSequence,
        _max_results: u32,
    ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
        // Stub: 返回空的候选列表
        Ok(recsys::RetrieveResponse {
            top_k_candidates: vec![],
        })
    }
}
