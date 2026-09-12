//! TweetMixer 客户端端口（SRC-05）。
//!
//! 上游 `sources/tweet_mixer_source.rs` 通过内部 thrift 合同
//! （`xai_x_thrift::tweet_mixer`）调用另一套网外候选服务。仓库中没有可
//! 运行的公开合同，因此这里只保留领域级请求/响应形状（U1 接缝）：真实
//! Adapter 负责传输、产品上下文映射、认证与超时，之后 `TweetMixerSource`
//! 才能进入装配。

use crate::models::ids::{PostId, UserId};
use tonic::async_trait;

/// 对应上游 `TweetMixerRequest` 中 Home 推荐产品可携带的公开字段。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TweetMixerRequest {
    pub user_id: UserId,
    pub client_app_id: i32,
    pub user_agent: Option<String>,
    pub country_code: Option<String>,
    pub language_code: Option<String>,
    /// 已见帖子，Adapter 应映射为服务端排除列表。
    pub excluded_tweet_ids: Vec<crate::models::PostId>,
    pub max_results: u32,
}

/// 服务返回的最小候选形状；后续补全由标准 Hydrator 负责。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TweetMixerCandidate {
    pub tweet_id: PostId,
    pub author_id: Option<UserId>,
    pub in_reply_to_tweet_id: Option<PostId>,
}

#[async_trait]
pub trait TweetMixerClient: Send + Sync {
    async fn get_recommendations(
        &self,
        request: TweetMixerRequest,
    ) -> Result<Vec<TweetMixerCandidate>, String>;
}
