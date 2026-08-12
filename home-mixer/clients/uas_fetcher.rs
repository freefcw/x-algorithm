// 用户行为序列获取器 (UAS Fetcher)
//
// 替代原始被阉割的 UAS 获取客户端。
//
// 原始功能说明：
// UserActionSequenceFetcher 从 X 内部的 UAS 存储服务
// 获取指定用户的行为序列数据（Thrift 格式）。
//
// 数据流：
//   用户在客户端的各种操作（点赞、回复等）
//     → 客户端/服务端埋点
//     → Kafka 行为事件流
//     → UAS 聚合服务 → UAS 存储（Manhattan KV）
//     → Home Mixer 通过 UAS Fetcher 获取
//
// 这是 Phoenix 精排模型最核心的输入特征来源。
// 没有用户行为序列，Phoenix 无法做个性化排序。
//
// 当前为 stub 实现，返回空行为序列。
// TODO: 当你的平台用户行为追踪系统就绪后，对接真实数据

use crate::uas_compat;
use tonic::async_trait;

/// 用户行为序列操作 trait
///
/// 定义了获取用户行为序列数据的标准接口。
/// 生产实现应从用户行为存储（如 Redis、自建 KV）中获取。
#[async_trait]
pub trait UserActionSequenceOps: Send + Sync {
    /// 根据用户 ID 获取行为序列
    ///
    /// # Arguments
    /// * `user_id` - 用户 ID
    ///
    /// # Returns
    /// Thrift 格式的用户行为序列
    async fn get_by_user_id(
        &self,
        user_id: u64,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error>;
}

/// 禁用的 UAS 集成占位实现。
///
/// 返回空行为序列；生产模式在真实适配器接入前拒绝启动。
pub struct DisabledUserActionSequenceFetcher;

impl DisabledUserActionSequenceFetcher {
    /// 创建 UAS Fetcher
    ///
    /// 原始实现在此处初始化到 UAS 存储服务的连接
    /// （通常是 Manhattan KV 或类似的分布式存储）。
    pub fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl UserActionSequenceOps for DisabledUserActionSequenceFetcher {
    async fn get_by_user_id(
        &self,
        _user_id: u64,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error> {
        // Stub: 返回空的行为序列
        // 这意味着 Phoenix 模型将无法使用个性化行为特征，
        // 但管道仍然可以正常运行（使用默认权重打分）
        Ok(uas_compat::UserActionSequence {
            metadata: Some(uas_compat::UserActionSequenceMeta {
                last_modified_epoch_ms: Some(0),
                last_kafka_publish_epoch_ms: Some(0),
            }),
            user_actions: Some(vec![]),
        })
    }
}

/// 演示环境 UAS 获取器
///
/// 返回一段合成的行为序列：过去 6 小时内浏览/互动过 32 条帖子，
/// 帖子 ID 用 Snowflake 格式合成，作者在演示账号集合中轮转。
/// 没有行为序列时 Phoenix 召回/精排会被整体跳过，
/// 所以这是打通模型链路的必要输入。由装配层在 `HOME_MIXER_MODE=demo` 时注入。
pub struct DemoUserActionSequenceFetcher;

#[async_trait]
impl UserActionSequenceOps for DemoUserActionSequenceFetcher {
    async fn get_by_user_id(
        &self,
        _user_id: u64,
    ) -> Result<uas_compat::UserActionSequence, anyhow::Error> {
        let now_ms = x_algorithm_proto::demo::now_ms();
        let count = 32;
        let step_ms = 6 * 60 * 60 * 1000 / count;

        let user_actions = (0..count)
            .map(|i| {
                let action_time_ms = now_ms - (count - i) * step_ms;
                let authors = x_algorithm_proto::demo::DEMO_AUTHOR_IDS;
                uas_compat::UserAction {
                    tweet_id: Some(x_algorithm_proto::demo::snowflake_id(
                        action_time_ms,
                        1000 + i,
                    )),
                    author_id: Some(authors[(i as usize) % authors.len()]),
                    action_time_ms: Some(action_time_ms),
                    action_type: Some(1),
                }
            })
            .collect();

        Ok(uas_compat::UserActionSequence {
            metadata: Some(uas_compat::UserActionSequenceMeta {
                last_modified_epoch_ms: Some(now_ms),
                last_kafka_publish_epoch_ms: Some(now_ms),
            }),
            user_actions: Some(user_actions),
        })
    }
}
