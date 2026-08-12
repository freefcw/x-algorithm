// 可见性过滤客户端 (VF Client)
//
// 替代原始 `xai_visibility_filtering::vf_client` 模块。
//
// 原始功能说明：
// VF Client 是一个 gRPC 客户端，连接 X 内部的 Visibility Filtering Service
// （全球部署的内容安全审核微服务）。它负责：
//   1. 批量提交帖子 ID 进行安全检查
//   2. 根据不同的安全级别 (SafetyLevel) 执行不同的审核策略
//   3.返回每条帖子的审核结论 (FilteredReason)
//
// 安全级别说明：
// - TimelineHome: 用于用户关注者的帖子，审核标准相对宽松
// - TimelineHomeRecommendations: 用于算法推荐的帖子，审核标准更严格
//   （因为用户没有主动选择关注这些作者）
//
// 当前提供 Demo 显式 Allow 和 Disabled 显式 Unavailable 两种实现。
// Disabled 结果由 Home Mixer 策略层保守降级，不会被解释为审核通过。

use super::models::FilteredReason;
use std::collections::HashMap;
use std::path::PathBuf;
use tonic::async_trait;

/// 安全级别枚举
///
/// 不同的展示场景使用不同的安全级别,
/// 安全级别越高, 审核标准越严格。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SafetyLevel {
    /// 首页时间线 — 用于用户关注者的帖子
    /// 审核标准相对宽松, 因为用户主动选择了关注这些作者
    TimelineHome,

    /// 首页推荐 — 用于算法推荐的帖子
    /// 审核标准更严格, 防止推荐不良内容给未主动选择的用户
    TimelineHomeRecommendations,

    /// 搜索结果 — 用于搜索场景
    Search,
}

/// Twitter 上下文查看者信息
///
/// 替代原始 `xai_twittercontext_proto::TwitterContextViewer`。
/// 提供当前查看者的上下文信息，用于安全检查时做个性化判断。
///
/// 例如：某些内容在 A 国合法但在 B 国违法，
/// 需要根据查看者的国家代码做出不同的过滤决策。
#[derive(Clone, Debug, Default)]
pub struct TwitterContextViewer {
    /// 查看者的用户 ID
    pub user_id: u64,
    /// 客户端应用 ID（iOS/Android/Web 等）
    pub client_application_id: i64,
    /// 请求发起国家代码 (ISO 3166-1 alpha-2)
    pub request_country_code: String,
    /// 请求语言代码 (BCP 47)
    pub request_language_code: String,
}

/// 获取 Twitter 上下文查看者的 trait
///
/// 替代原始 `xai_twittercontext_proto::GetTwitterContextViewer`。
/// 由 ScoredPostsQuery 实现，使得安全检查代码可以从查询中提取查看者信息。
pub trait GetTwitterContextViewer {
    fn get_viewer(&self) -> Option<TwitterContextViewer>;
}

/// 可见性过滤客户端 trait
///
/// 定义了内容安全检查的统一接口。
/// 生产实现应连接实际的内容安全审核服务。
#[async_trait]
pub trait VisibilityFilteringClient: Send + Sync {
    /// 批量检查帖子的可见性
    ///
    /// # Arguments
    /// * `tweet_ids` - 待检查的帖子 ID 列表
    /// * `safety_level` - 安全级别（决定审核严格程度）
    /// * `for_user_id` - 查看者用户 ID
    /// * `context` - 可选的查看者上下文信息
    ///
    /// # Returns
    /// Map<tweet_id -> Option<FilteredReason>>
    /// - None 表示帖子通过安全检查
    /// - Some(reason) 表示帖子被标记，附带原因
    async fn get_result(
        &self,
        tweet_ids: Vec<u64>,
        safety_level: SafetyLevel,
        for_user_id: u64,
        context: Option<TwitterContextViewer>,
    ) -> Result<HashMap<u64, Option<FilteredReason>>, anyhow::Error>;
}

/// Demo visibility adapter. It produces an explicit allow decision for every
/// requested post so the local mixed-source flow remains testable.
pub struct DemoVisibilityFilteringClient;

#[async_trait]
impl VisibilityFilteringClient for DemoVisibilityFilteringClient {
    async fn get_result(
        &self,
        tweet_ids: Vec<u64>,
        _safety_level: SafetyLevel,
        _for_user_id: u64,
        _context: Option<TwitterContextViewer>,
    ) -> Result<HashMap<u64, Option<FilteredReason>>, anyhow::Error> {
        Ok(tweet_ids.into_iter().map(|id| (id, None)).collect())
    }
}

/// Disabled production integration. Returning an error keeps "not checked"
/// distinct from an explicit allow; the application policy then retains only
/// in-network candidates.
pub struct DisabledVisibilityFilteringClient;

impl DisabledVisibilityFilteringClient {
    /// 创建 VF 客户端
    ///
    /// 原始实现使用 S2S (Service-to-Service) 双向 TLS 证书认证。
    /// 证书路径参数保留以兼容现有调用签名。
    ///
    /// # Arguments
    /// * `_chain_path` - CA 证书链路径（当前未使用）
    /// * `_crt_path` - 客户端证书路径（当前未使用）
    /// * `_key_path` - 客户端私钥路径（当前未使用）
    pub async fn new(
        _chain_path: PathBuf,
        _crt_path: PathBuf,
        _key_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl VisibilityFilteringClient for DisabledVisibilityFilteringClient {
    async fn get_result(
        &self,
        tweet_ids: Vec<u64>,
        _safety_level: SafetyLevel,
        _for_user_id: u64,
        _context: Option<TwitterContextViewer>,
    ) -> Result<HashMap<u64, Option<FilteredReason>>, anyhow::Error> {
        let _ = tweet_ids;
        anyhow::bail!("production visibility adapter is not configured")
    }
}
