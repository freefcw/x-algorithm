//! Local domain model for final-feed items (U2).
//!
//! Upstream represents feed items directly as `xai_home_mixer_proto::FeedItem`.
//! The local public proto stays additive-only, so this domain enum owns feed
//! composition and maps to the wire type at the RPC boundary via `into_proto`.

use x_algorithm_proto::home_mixer as pb;
use x_algorithm_proto::home_mixer::ScoredPost;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FeedItemKind {
    Post,
    Advertisement,
    WhoToFollow,
    Prompt,
    PushToHome,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Advertisement {
    pub ad_id: String,
    pub requested_position: usize,
    pub brand_safety_risk: x_algorithm_proto::home_mixer::BrandSafetyRiskLevel,
    pub avoid_handles: Vec<crate::models::UserId>,
    pub avoid_keywords: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WhoToFollowModule {
    pub module_id: String,
    pub user_ids: Vec<crate::models::UserId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Prompt {
    pub prompt_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PushToHomePost {
    pub notification_id: String,
    pub post: ScoredPost,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeedItemContent {
    Post(ScoredPost),
    Advertisement(Advertisement),
    WhoToFollow(WhoToFollowModule),
    Prompt(Prompt),
    PushToHome(PushToHomePost),
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeedItem {
    pub position: usize,
    pub content: FeedItemContent,
    /// Domain identity of the contained post, set at construction so
    /// attribution never re-parses wire strings. `None` for non-post content.
    pub post_id: Option<crate::models::PostId>,
}

impl FeedItem {
    pub fn post(post: ScoredPost, post_id: crate::models::PostId) -> Self {
        Self {
            position: 0,
            content: FeedItemContent::Post(post),
            post_id: Some(post_id),
        }
    }

    pub fn advertisement(ad_id: impl Into<String>, requested_position: usize) -> Self {
        Self {
            position: 0,
            content: FeedItemContent::Advertisement(Advertisement {
                ad_id: ad_id.into(),
                requested_position,
                brand_safety_risk: pb::BrandSafetyRiskLevel::Unspecified,
                avoid_handles: Vec::new(),
                avoid_keywords: Vec::new(),
            }),
            post_id: None,
        }
    }

    pub fn who_to_follow(
        module_id: impl Into<String>,
        user_ids: Vec<crate::models::UserId>,
    ) -> Self {
        Self {
            position: 0,
            content: FeedItemContent::WhoToFollow(WhoToFollowModule {
                module_id: module_id.into(),
                user_ids,
            }),
            post_id: None,
        }
    }

    pub fn prompt(prompt_id: impl Into<String>) -> Self {
        Self {
            position: 0,
            content: FeedItemContent::Prompt(Prompt {
                prompt_id: prompt_id.into(),
            }),
            post_id: None,
        }
    }

    pub fn push_to_home(
        notification_id: impl Into<String>,
        post: ScoredPost,
        post_id: crate::models::PostId,
    ) -> Self {
        Self {
            position: 0,
            content: FeedItemContent::PushToHome(PushToHomePost {
                notification_id: notification_id.into(),
                post,
            }),
            post_id: Some(post_id),
        }
    }

    pub fn kind(&self) -> FeedItemKind {
        match self.content {
            FeedItemContent::Post(_) => FeedItemKind::Post,
            FeedItemContent::Advertisement(_) => FeedItemKind::Advertisement,
            FeedItemContent::WhoToFollow(_) => FeedItemKind::WhoToFollow,
            FeedItemContent::Prompt(_) => FeedItemKind::Prompt,
            FeedItemContent::PushToHome(_) => FeedItemKind::PushToHome,
        }
    }

    pub fn post_id(&self) -> Option<crate::models::PostId> {
        match &self.content {
            FeedItemContent::Post(_) => self.post_id,
            _ => None,
        }
    }

    pub fn served_post_id(&self) -> Option<crate::models::PostId> {
        match &self.content {
            FeedItemContent::Post(_) | FeedItemContent::PushToHome(_) => self.post_id,
            _ => None,
        }
    }

    pub fn into_proto(self) -> pb::FeedItem {
        let item = match self.content {
            FeedItemContent::Post(post) => pb::feed_item::Item::Post(post),
            FeedItemContent::Advertisement(advertisement) => {
                pb::feed_item::Item::Advertisement(pb::Advertisement {
                    ad_id: advertisement.ad_id,
                    requested_position: as_proto_position(advertisement.requested_position),
                    brand_safety_risk: advertisement.brand_safety_risk.into(),
                    avoid_handles: advertisement
                        .avoid_handles
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect(),
                    avoid_keywords: advertisement.avoid_keywords,
                })
            }
            FeedItemContent::WhoToFollow(module) => {
                pb::feed_item::Item::WhoToFollow(pb::WhoToFollowModule {
                    module_id: module.module_id,
                    user_ids: module
                        .user_ids
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect(),
                })
            }
            FeedItemContent::Prompt(prompt) => pb::feed_item::Item::Prompt(pb::Prompt {
                prompt_id: prompt.prompt_id,
            }),
            FeedItemContent::PushToHome(push) => {
                pb::feed_item::Item::PushToHome(pb::PushToHomePost {
                    notification_id: push.notification_id,
                    post: Some(push.post),
                })
            }
        };
        pb::FeedItem {
            position: as_proto_position(self.position),
            item: Some(item),
        }
    }
}

fn as_proto_position(position: usize) -> u32 {
    u32::try_from(position).unwrap_or(u32::MAX)
}
