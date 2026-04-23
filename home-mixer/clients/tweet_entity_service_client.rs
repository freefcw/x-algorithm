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
//   3. SubscriptionHydrator: 获取付费订阅帖子的作者 ID
//
// 替换建议：对接你平台的帖子/内容微服务 API
// 当前为 stub 实现。

use crate::candidate_pipeline::candidate_features::{MediaEntities, PureCoreData};
use std::collections::HashMap;
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
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<PureCoreData>>, anyhow::Error>;

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
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<MediaEntities>>, anyhow::Error>;

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
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<u64>>, anyhow::Error>;
}

/// 生产环境 TES 客户端（Stub 实现）
///
/// 当前返回空的帖子元数据。
/// TODO: 替换为你平台的帖子微服务客户端
pub struct ProdTESClient;

impl ProdTESClient {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[async_trait]
impl TESClient for ProdTESClient {
    async fn get_tweet_core_datas(
        &self,
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<PureCoreData>>, anyhow::Error> {
        // Stub: 所有帖子无核心数据
        let results: HashMap<i64, Option<PureCoreData>> =
            tweet_ids.into_iter().map(|id| (id, None)).collect();
        Ok(results)
    }

    async fn get_tweet_media_entities(
        &self,
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<MediaEntities>>, anyhow::Error> {
        // Stub: 所有帖子无媒体实体
        let results: HashMap<i64, Option<MediaEntities>> =
            tweet_ids.into_iter().map(|id| (id, None)).collect();
        Ok(results)
    }

    async fn get_subscription_author_ids(
        &self,
        tweet_ids: Vec<i64>,
    ) -> Result<HashMap<i64, Option<u64>>, anyhow::Error> {
        // Stub: 所有帖子非订阅内容
        let results: HashMap<i64, Option<u64>> =
            tweet_ids.into_iter().map(|id| (id, None)).collect();
        Ok(results)
    }
}
