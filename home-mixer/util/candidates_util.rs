// 候选帖子工具函数
//
// 提供与候选帖子相关的通用辅助函数。

use crate::candidate_pipeline::candidate::PostCandidate;

/// 获取帖子及其关联帖子的 ID 列表
///
/// 一条帖子可能关联多个 ID：
///   - 帖子本身的 ID
///   - 如果是转发帖，原始帖子的 ID
///   - 如果是回复帖，被回复帖子的 ID
///
/// 这在去重过滤器中使用：如果用户已经看过/回复过原帖，
/// 那么这条转发/回复也应该被过滤掉。
///
/// # Arguments
/// * `candidate` - 候选帖子
///
/// # Returns
/// 帖子本身及其关联帖子的 ID 列表
pub fn get_related_post_ids(candidate: &PostCandidate) -> Vec<i64> {
    let mut ids = vec![candidate.tweet_id];
    if let Some(retweeted_id) = candidate.retweeted_tweet_id {
        ids.push(retweeted_id as i64);
    }
    if let Some(reply_to_id) = candidate.in_reply_to_tweet_id {
        ids.push(reply_to_id as i64);
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_related_post_ids() {
        let candidate = PostCandidate {
            tweet_id: 100,
            retweeted_tweet_id: Some(101),
            in_reply_to_tweet_id: Some(102),
            ..Default::default()
        };
        let ids = get_related_post_ids(&candidate);
        assert_eq!(ids, vec![100, 101, 102]);
    }
}
