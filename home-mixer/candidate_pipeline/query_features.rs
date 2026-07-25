use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct UserFeatures {
    pub muted_keywords: Vec<String>,
    pub blocked_user_ids: Vec<i64>,
    pub blocked_by_user_ids: Vec<i64>,
    pub muted_user_ids: Vec<i64>,
    pub followed_user_ids: Vec<i64>,
    pub subscribed_user_ids: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn additive_fields_default_when_reading_legacy_payloads() {
        let features: UserFeatures = serde_json::from_str(
            r#"{"followedUserIds":[101],"blockedUserIds":[],"mutedUserIds":[],"mutedKeywords":[],"subscribedUserIds":[]}"#,
        )
        .expect("legacy user features");

        assert_eq!(features.followed_user_ids, vec![101]);
        assert!(features.blocked_by_user_ids.is_empty());
    }
}
