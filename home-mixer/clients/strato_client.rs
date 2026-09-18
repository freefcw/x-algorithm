// Strato 分布式缓存客户端
//
// 替代原始 `xai_strato` 私有 crate。
//
// 原始功能说明：
// Strato 是 X 内部的分布式缓存/存储层，充当了多种数据源的统一访问入口。
// Home Mixer 通过 Strato 客户端获取：
//   1. 用户特征 (UserFeatures) — 包括关注列表、屏蔽列表、静音列表等
//   2. 请求信息缓存 — 存储已投递的帖子列表（用于去重）
//
// Strato 的设计理念类似于 GraphQL：提供统一的数据访问层，
// 底层可以对接 Manhattan (KV存储)、Gizmoduck (用户服务) 等多种数据源。
//
// 替换方案建议：
//   - 用户特征: 从你平台的用户微服务或 Redis 获取
//   - 请求缓存: 使用 Redis 或本地缓存
//
// 当前为 stub 实现。
// TODO: 替换为你平台的用户数据服务

use crate::models::ids::{PostId, UserId};
use tonic::async_trait;

// =============================================================================
// Strato 类型定义（替代 xai_strato crate 中的类型）
// =============================================================================

/// Strato 返回结果类型
///
/// 封装了从 Strato 获取数据的结果，
/// 区分"数据获取成功"和"Strato 服务端返回错误"两种情况
#[derive(Debug)]
pub enum StratoResult<T> {
    Ok(T),
    Err(String),
}

/// Strato 值容器
///
/// 包装了实际的数据值，支持 None（数据不存在）的情况
#[derive(Debug)]
pub struct StratoValue<T> {
    pub v: Option<T>,
}

/// 解码 Strato 返回的原始字节为业务类型
///
/// 原始实现中使用 Thrift 或自定义二进制格式反序列化。
/// 当前 stub 实现使用 JSON 反序列化。
///
/// # Arguments
/// * `data` - Strato 返回的原始字节
///
/// # Returns
/// 解码后的 StratoResult
pub fn decode<T: serde::de::DeserializeOwned>(data: &[u8]) -> StratoResult<StratoValue<T>> {
    match serde_json::from_slice::<T>(data) {
        Ok(value) => StratoResult::Ok(StratoValue { v: Some(value) }),
        Err(_) => {
            // 如果数据为空，返回 None 而不是错误
            if data.is_empty() {
                StratoResult::Ok(StratoValue { v: None })
            } else {
                StratoResult::Err("Failed to decode strato response".to_string())
            }
        }
    }
}

// =============================================================================
// Strato 客户端 trait 和实现
// =============================================================================

/// Strato 客户端 trait
///
/// 定义了 Home Mixer 需要的 Strato 数据访问接口。
/// 两个方法对应两种不同的数据读取需求：
///   - get_user_features: 获取用户特征（关注列表、屏蔽列表等）
///   - store_request_info: 缓存已投递的帖子列表
#[async_trait]
pub trait StratoClient: Send + Sync {
    /// 获取用户特征
    ///
    /// 从 Strato 获取指定用户的关注列表、屏蔽列表等特征信息。
    /// 这些特征在管道中被多个组件使用：
    ///   - InNetworkCandidateHydrator: 判断帖子是否来自关注者
    ///   - AuthorSocialgraphFilter: 过滤被屏蔽/静音的作者
    ///   - ViewerMutedKeywordFilter: 使用屏蔽关键词列表
    ///
    /// # Arguments
    /// * `user_id` - 用户 ID
    ///
    /// # Returns
    /// 序列化的用户特征数据（原始字节）
    async fn get_user_features(&self, user_id: UserId) -> Result<Vec<u8>, anyhow::Error>;

    /// 存储请求信息（已投递帖子缓存）
    ///
    /// 将本次请求投递的帖子 ID 列表写入 Strato 缓存，
    /// 用于后续请求中避免重复投递相同帖子。
    ///
    /// # Arguments
    /// * `user_id` - 用户 ID
    /// * `post_ids` - 本次投递的帖子 ID 列表
    async fn store_request_info(
        &self,
        user_id: UserId,
        post_ids: Vec<PostId>,
    ) -> Result<Vec<u8>, anyhow::Error>;
}

/// 禁用的 Strato 集成占位实现。
///
/// 返回空用户特征并丢弃写入；生产模式在真实适配器接入前拒绝启动。
pub struct DisabledStratoClient;

impl DisabledStratoClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl StratoClient for DisabledStratoClient {
    async fn get_user_features(&self, _user_id: UserId) -> Result<Vec<u8>, anyhow::Error> {
        // Stub: 返回空的用户特征 JSON
        let empty_features = serde_json::json!({
            "mutedKeywords": [],
            "blockedUserIds": [],
            "mutedUserIds": [],
            "followedUserIds": [],
            "subscribedUserIds": []
        });
        Ok(serde_json::to_vec(&empty_features)?)
    }

    async fn store_request_info(
        &self,
        _user_id: UserId,
        _post_ids: Vec<PostId>,
    ) -> Result<Vec<u8>, anyhow::Error> {
        anyhow::bail!("Strato request-info persistence is disabled")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_persistent_adapters_never_report_write_success() {
        assert!(DisabledStratoClient
            .store_request_info(crate::models::uid(1), vec![crate::models::pid(10)])
            .await
            .is_err());
    }
}
