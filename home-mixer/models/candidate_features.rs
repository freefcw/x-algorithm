use crate::models::ids::{PostId, UserId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct PureCoreData {
    pub author_id: UserId,
    pub text: String,
    pub source_tweet_id: Option<PostId>,
    pub source_user_id: Option<UserId>,
    pub quoted_tweet_id: Option<PostId>,
    pub quoted_user_id: Option<UserId>,
    pub in_reply_to_tweet_id: Option<PostId>,
    pub in_reply_to_user_id: Option<UserId>,
    /// Authoritative post creation time in unix milliseconds.
    pub created_at_ms: Option<u64>,
    pub recommendation_eligible: Option<bool>,
    pub language_code: Option<String>,
    pub favorite_count: Option<i64>,
    pub view_count: Option<u64>,
    pub reply_count: Option<i64>,
    pub repost_count: Option<i64>,
    pub quote_count: Option<i64>,
    pub filtered_topic_ids: Vec<i64>,
    pub unfiltered_topic_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExclusiveTweetControl {
    pub conversation_author_id: UserId,
}

pub type MediaEntities = Vec<MediaEntity>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MediaEntity {
    pub media_info: Option<MediaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum MediaInfo {
    VideoInfo(VideoInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VideoInfo {
    pub duration_millis: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Share {
    pub source_tweet_id: PostId,
    pub source_user_id: UserId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Reply {
    pub in_reply_to_tweet_id: Option<PostId>,
    pub in_reply_to_user_id: UserId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct GizmoduckUserCounts {
    pub followers_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct GizmoduckUserProfile {
    pub screen_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct GizmoduckUser {
    pub user_id: UserId,
    pub profile: GizmoduckUserProfile,
    pub counts: GizmoduckUserCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct GizmoduckUserResult {
    pub user: Option<GizmoduckUser>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_data_without_view_count_remains_compatible() {
        let core: PureCoreData = serde_json::from_str(r#"{"authorId":7,"text":"post"}"#)
            .expect("core data without view_count should deserialize");

        assert_eq!(core.author_id, crate::models::uid(7));
        assert_eq!(core.view_count, None);
        assert_eq!(core.created_at_ms, None);
    }
}
