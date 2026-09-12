// 候选帖子工具函数，对齐上游 `47c1bcd` home-mixer/util/candidates_util.rs。
//
// 差异说明（U1）：上游候选的视频时长字段是 `min_video_duration_ms`
// （多视频取最短，由 MediaInfoHydrator 写入）；本地仍是
// `video_duration_ms` / `quoted_video_duration_ms`（VideoDuration/Quote
// Hydrator 写入），字段改名随 media_info 对齐批次统一。

use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;

/// viewer 粉丝数达到该阈值后不再计入 VQV 权重（上游真值）。
const MAX_FOLLOWERS_THRESHOLD: i64 = 10_000;

/// 获取帖子及其关联帖子（转发原帖、被回复帖）的 ID 列表。
/// 去重过滤使用：用户看过原帖时，其转发/回复也应被过滤。
pub fn get_related_post_ids(candidate: &PostCandidate) -> Vec<crate::models::PostId> {
    let mut ids = vec![candidate.tweet_id];
    ids.extend(candidate.retweeted_tweet_id);
    ids.extend(candidate.in_reply_to_tweet_id);
    ids
}

pub fn related_post_ids_iter(
    candidate: &PostCandidate,
) -> impl Iterator<Item = crate::models::PostId> {
    std::iter::once(candidate.tweet_id)
        .chain(candidate.retweeted_tweet_id)
        .chain(candidate.in_reply_to_tweet_id)
}

/// VQV（视频有效观看）头的有效权重：
/// viewer 粉丝数达到阈值、或视频时长不超过门槛时为 0。
pub fn vqv_weight(
    query: &ScoredPostsQuery,
    candidate: &PostCandidate,
    min_video_duration_ms: i32,
    vqv_weight_value: f64,
) -> f64 {
    let exceeds_followers = query
        .user_features
        .follower_count
        .map(|count| count >= MAX_FOLLOWERS_THRESHOLD)
        .unwrap_or(false);

    if !exceeds_followers
        && candidate
            .video_duration_ms
            .is_some_and(|ms| ms > min_video_duration_ms)
    {
        vqv_weight_value
    } else {
        0.0
    }
}

/// 引用帖 VQV 头的有效权重：时长检查关闭时恒为权重值。
pub fn quoted_vqv_weight(
    candidate: &PostCandidate,
    min_video_duration_ms: i32,
    quoted_vqv_weight_value: f64,
    enable_duration_check: bool,
) -> f64 {
    if !enable_duration_check {
        return quoted_vqv_weight_value;
    }

    if candidate
        .quoted_video_duration_ms
        .is_some_and(|ms| ms > min_video_duration_ms)
    {
        quoted_vqv_weight_value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_features::UserFeatures;

    #[test]
    fn test_get_related_post_ids() {
        let candidate = PostCandidate {
            tweet_id: 100.into(),
            retweeted_tweet_id: Some(101.into()),
            in_reply_to_tweet_id: Some(102.into()),
            ..Default::default()
        };
        let ids = get_related_post_ids(&candidate);
        assert_eq!(
            ids,
            vec![
                crate::models::pid(100),
                crate::models::pid(101),
                crate::models::pid(102)
            ]
        );
        let iter_ids: Vec<_> = related_post_ids_iter(&candidate).collect();
        assert_eq!(iter_ids, ids);
    }

    #[test]
    fn vqv_weight_requires_duration_above_threshold() {
        let query = ScoredPostsQuery::default();
        let short_video = PostCandidate {
            video_duration_ms: Some(9_999),
            ..Default::default()
        };
        let long_video = PostCandidate {
            video_duration_ms: Some(10_001),
            ..Default::default()
        };

        assert_eq!(vqv_weight(&query, &short_video, 10_000, 0.05), 0.0);
        assert_eq!(vqv_weight(&query, &long_video, 10_000, 0.05), 0.05);
    }

    #[test]
    fn vqv_weight_is_zero_for_high_follower_viewers() {
        let query = ScoredPostsQuery {
            user_features: UserFeatures {
                follower_count: Some(MAX_FOLLOWERS_THRESHOLD),
                ..Default::default()
            },
            ..Default::default()
        };
        let long_video = PostCandidate {
            video_duration_ms: Some(60_000),
            ..Default::default()
        };

        assert_eq!(vqv_weight(&query, &long_video, 10_000, 0.05), 0.0);
    }

    #[test]
    fn quoted_vqv_returns_weight_when_check_disabled() {
        let candidate = PostCandidate {
            quoted_video_duration_ms: None,
            ..Default::default()
        };
        assert!((quoted_vqv_weight(&candidate, 10_000, 0.5, false) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn quoted_vqv_requires_duration_when_check_enabled() {
        let below = PostCandidate {
            quoted_video_duration_ms: Some(10_000),
            ..Default::default()
        };
        let above = PostCandidate {
            quoted_video_duration_ms: Some(10_001),
            ..Default::default()
        };
        assert_eq!(quoted_vqv_weight(&below, 10_000, 0.5, true), 0.0);
        assert!((quoted_vqv_weight(&above, 10_000, 0.5, true) - 0.5).abs() < 1e-9);
    }
}
