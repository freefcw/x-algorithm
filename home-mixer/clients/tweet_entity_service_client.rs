// Tweet Entity Service (TES) 客户端
//
// 替代原始被阉割的 TES 客户端模块。
//
// 原始功能说明：
// TES (Tweet Entity Service) 是 X 内部的帖子实体元数据服务，
// 提供帖子的详细属性信息。与 Thunder 不同：
//   - Thunder: 只存储轻量级帖子信息（ID、作者、时间），用于快速召回
//   - TES: 存储完整帖子元数据（文本、媒体、回复关系等），用于补全
//
// 在 Home Mixer 管道中的使用：
//   1. CoreDataCandidateHydrator: 获取帖子核心数据（文本、转发/回复关系）
//   2. VideoDurationCandidateHydrator: 获取视频时长（用于 VQV 权重判定）
// `get_subscription_author_ids` 仍留在 trait 上，适配器返回空即可（U5，无付费订阅组件）。
//
// 替换建议：对接你平台的帖子/内容微服务 API
// 当前为 stub 实现。

use crate::id::IdentityRegistrationContext;
use crate::models::candidate_features::{MediaEntities, PureCoreData};
use crate::models::ids::{PostId, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::async_trait;

/// TES 客户端 trait
///
/// 定义了获取帖子元数据的标准接口。
/// 每个方法对应帖子的不同属性维度。
#[async_trait]
pub trait TESClient: Send + Sync {
    /// 批量获取帖子核心数据
    ///
    /// 包括帖子文本、作者、转发/回复关系等基本信息。
    /// 这是最关键的补全步骤，多个过滤器依赖这些数据。
    ///
    /// # Arguments
    /// * `tweet_ids` - 帖子 ID 列表
    ///
    /// # Returns
    /// Map<tweet_id -> Option<PureCoreData>>
    async fn get_tweet_core_datas(
        &self,
        tweet_ids: Vec<PostId>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<PureCoreData>>, anyhow::Error>;

    /// 批量获取帖子媒体实体
    ///
    /// 包括帖子中的图片、视频等媒体信息。
    /// VideoDurationCandidateHydrator 使用此方法获取视频时长。
    ///
    /// # Arguments
    /// * `tweet_ids` - 帖子 ID 列表
    ///
    /// # Returns
    /// Map<tweet_id -> Option<MediaEntities>>
    async fn get_tweet_media_entities(
        &self,
        tweet_ids: Vec<PostId>,
        identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<MediaEntities>>, anyhow::Error>;

    /// 批量获取帖子的付费订阅作者 ID
    ///
    /// 如果帖子是付费订阅专属内容（类似 X Premium 的订阅功能），
    /// 返回该帖子的订阅作者 ID。非订阅帖子返回 None。
    ///
    /// # Arguments
    /// * `tweet_ids` - 帖子 ID 列表
    ///
    /// # Returns
    /// Map<tweet_id -> Option<u64>>
    async fn get_subscription_author_ids(
        &self,
        tweet_ids: Vec<PostId>,
    ) -> Result<HashMap<PostId, Option<UserId>>, anyhow::Error>;
}

/// 禁用的 TES 集成占位实现。
///
/// 返回空的帖子元数据；生产模式在真实适配器接入前拒绝启动。
pub struct DisabledTESClient;

impl DisabledTESClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl TESClient for DisabledTESClient {
    async fn get_tweet_core_datas(
        &self,
        tweet_ids: Vec<PostId>,
        _identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<PureCoreData>>, anyhow::Error> {
        Ok(tweet_ids.into_iter().map(|id| (id, None)).collect())
    }

    async fn get_tweet_media_entities(
        &self,
        tweet_ids: Vec<PostId>,
        _identity: Arc<IdentityRegistrationContext>,
    ) -> Result<HashMap<PostId, Option<MediaEntities>>, anyhow::Error> {
        Ok(tweet_ids.into_iter().map(|id| (id, None)).collect())
    }

    async fn get_subscription_author_ids(
        &self,
        tweet_ids: Vec<PostId>,
    ) -> Result<HashMap<PostId, Option<UserId>>, anyhow::Error> {
        Ok(tweet_ids.into_iter().map(|id| (id, None)).collect())
    }
}
