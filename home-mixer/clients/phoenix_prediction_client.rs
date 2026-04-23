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
//   3. 返回预测结果，供 WeightedScorer 计算最终排序分数
//
// 预测输入：
//   - user_id: 当前用户
//   - user_action_sequence: 用户最近的行为序列（特征）
//   - candidates: 待评分的帖子列表
//
// 预测输出：
//   - PredictNextActionsResponse: 每条帖子上各行为的概率分布
//
// 当前为 stub 实现，返回空预测结果。
// TODO: 当 Phoenix 模型训练完成后，连接真实的 Phoenix gRPC 服务

use tonic::async_trait;
use x_algorithm_proto::recsys;

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

/// 生产环境 Phoenix 精排客户端（Stub 实现）
///
/// 当前返回空预测结果（所有帖子得分为默认值）。
/// 当 Phoenix 模型训练完成并部署后，此实现应替换为
/// 实际的 gRPC 客户端调用 `PhoenixPredictionService.PredictNextActions`。
pub struct ProdPhoenixPredictionClient;

impl ProdPhoenixPredictionClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        // TODO: 从环境变量读取 Phoenix prediction 服务地址
        // let addr = std::env::var("PHOENIX_PREDICT_GRPC_ADDR")
        //     .unwrap_or_else(|_| "http://localhost:50053".to_string());
        Ok(Self)
    }
}

#[async_trait]
impl PhoenixPredictionClient for ProdPhoenixPredictionClient {
    async fn predict(
        &self,
        _user_id: u64,
        _sequence: recsys::UserActionSequence,
        _candidates: Vec<recsys::TweetInfo>,
    ) -> Result<recsys::PredictNextActionsResponse, anyhow::Error> {
        // Stub: 返回空预测结果
        // 这意味着所有帖子的 PhoenixScores 为默认值 (None)
        // WeightedScorer 会将 None 视为 0.0
        Ok(recsys::PredictNextActionsResponse {
            distribution_sets: vec![],
        })
    }
}
