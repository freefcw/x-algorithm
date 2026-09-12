use crate::models::ids::UserId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct UserFeatures {
    pub muted_keywords: Vec<String>,
    pub blocked_user_ids: Vec<UserId>,
    pub blocked_by_user_ids: Vec<UserId>,
    pub muted_user_ids: Vec<UserId>,
    pub followed_user_ids: Vec<UserId>,
    pub subscribed_user_ids: Vec<UserId>,
    /// viewer 粉丝数（上游 47c1bcd 字段）；VQV 权重的粉丝门槛使用。
    /// 本地 UserFeatures 适配器暂不提供该值时为 None，门槛不触发。
    pub follower_count: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn additive_fields_default_when_reading_legacy_payloads() {
        let features: UserFeatures = serde_json::from_str(
            r#"{"followedUserIds":["000000000000000000000065"],"blockedUserIds":[],"mutedUserIds":[],"mutedKeywords":[],"subscribedUserIds":[]}"#,
        )
        .expect("legacy user features");

        assert_eq!(features.followed_user_ids, vec![crate::models::uid(101)]);
        assert!(features.blocked_by_user_ids.is_empty());
    }
}
