// Gizmoduck 用户资料客户端
//
// 替代原始被阉割的 Gizmoduck 客户端模块。
//
// 原始功能说明：
// Gizmoduck 是 X 内部的用户资料微服务（据传得名于一只迪士尼鸭子角色），
// 是所有用户元数据的权威来源。它提供：
//   1. 用户基本信息 (screen_name, display_name 等)
//   2. 用户计数信息 (followers_count, following_count 等)
//   3. 用户设置和偏好
//   4. 账号状态 (active, suspended, deactivated 等)
//
// 在 Home Mixer 管道中的使用：
//   GizmoduckCandidateHydrator 使用此客户端批量获取候选帖子作者的资料，
//   补全以下信息：
//     - author_followers_count: 用于分数归一化
//     - author_screen_name: 用于返回给客户端展示
//     - retweeted_screen_name: 转发帖的原作者名称
//
// 替换建议：对接你平台的用户微服务 API
// 当前为 stub 实现。

use crate::models::candidate_features::GizmoduckUserResult;
use std::collections::HashMap;
use tonic::async_trait;

/// Request-level viewer policy used by `QueryBuilder`.
///
/// The public replacement currently knows only whether For You recommendations
/// are allowed. Additional upstream fields must be added only with a verified
/// user-service contract and an owning query field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ViewerEligibility {
    Allowed,
    Denied,
    #[default]
    Unknown,
}

impl ViewerEligibility {
    pub fn allows_for_you(self) -> bool {
        self == Self::Allowed
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewerData {
    pub for_you_eligibility: ViewerEligibility,
}

/// Gizmoduck 用户资料客户端 trait
///
/// 定义了批量获取用户资料的标准接口。
/// 生产实现应连接你平台的用户微服务。
#[async_trait]
pub trait GizmoduckClient: Send + Sync {
    /// Fetch request-level viewer policy.
    ///
    /// The neutral default keeps the main recommendation path running until a
    /// public user-service adapter is supplied and manually verified.
    async fn get_viewer_data(&self, _viewer_id: u64) -> Result<ViewerData, anyhow::Error> {
        Ok(ViewerData::default())
    }

    /// 批量获取用户资料
    ///
    /// # Arguments
    /// * `user_ids` - 需要查询的用户 ID 列表
    ///
    /// # Returns
    /// Map<user_id -> Option<GizmoduckUserResult>>
    /// - Some(result): 用户存在，包含资料信息
    /// - None: 用户不存在或被停用
    async fn get_users(
        &self,
        user_ids: Vec<u64>,
    ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error>;
}

/// Demo viewer and profile adapter. Demo users explicitly permit For You
/// recommendations so local end-to-end runs exercise both network sources.
pub struct DemoGizmoduckClient;

#[async_trait]
impl GizmoduckClient for DemoGizmoduckClient {
    async fn get_viewer_data(&self, _viewer_id: u64) -> Result<ViewerData, anyhow::Error> {
        Ok(ViewerData {
            for_you_eligibility: ViewerEligibility::Allowed,
        })
    }

    async fn get_users(
        &self,
        user_ids: Vec<u64>,
    ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
        Ok(user_ids.into_iter().map(|id| (id, None)).collect())
    }
}

/// Disabled integration placeholder. It deliberately reports viewer policy
/// as unknown; the application boundary degrades unknown policy to in-network.
pub struct DisabledGizmoduckClient;

impl DisabledGizmoduckClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl GizmoduckClient for DisabledGizmoduckClient {
    async fn get_users(
        &self,
        user_ids: Vec<u64>,
    ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
        // Stub: 返回所有用户为 None（未找到）
        // 这意味着 author_screen_name 和 author_followers_count 将为 None
        let results: HashMap<u64, Option<GizmoduckUserResult>> =
            user_ids.into_iter().map(|id| (id, None)).collect();
        Ok(results)
    }
}
