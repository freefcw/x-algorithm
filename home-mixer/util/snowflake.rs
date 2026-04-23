// Twitter Snowflake ID 工具
//
// Twitter 使用 Snowflake 格式的 ID，其中高位包含创建时间戳:
//   - Bit 63 (MSB): 保留 (0)
//   - Bit 62-22 (41 bits): 毫秒时间戳 (relative to Twitter epoch)
//   - Bit 21-12 (10 bits): 机器 ID
//   - Bit 11-0 (12 bits): 序列号
//
// Twitter Epoch: 2010-11-04T01:42:54.657Z = 1288834974657 ms since Unix epoch
//
// 此工具用于从帖子 ID 中提取创建时间，
// 在 AgeFilter 中判断帖子是否过期。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Twitter Snowflake epoch (毫秒 since Unix epoch)
/// 2010-11-04T01:42:54.657Z
const TWITTER_EPOCH_MS: u64 = 1288834974657;

/// 从 Snowflake ID 中提取创建时间距今的 Duration
///
/// # Arguments
/// * `tweet_id` - Twitter Snowflake 格式的帖子 ID
///
/// # Returns
/// Some(duration) 如果能成功解析创建时间
/// None 如果 ID 无效或时间戳解析失败
pub fn duration_since_creation_opt(tweet_id: i64) -> Option<Duration> {
    if tweet_id <= 0 {
        return None;
    }

    // 提取高 41 位中的毫秒时间戳
    let timestamp_ms = ((tweet_id as u64) >> 22) + TWITTER_EPOCH_MS;

    let creation_time = UNIX_EPOCH + Duration::from_millis(timestamp_ms);
    SystemTime::now().duration_since(creation_time).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_id() {
        assert!(duration_since_creation_opt(0).is_none());
        assert!(duration_since_creation_opt(-1).is_none());
    }

    #[test]
    fn test_old_tweet_returns_duration() {
        // A known old tweet ID: 1 (very early Snowflake ID)
        let result = duration_since_creation_opt(1);
        assert!(result.is_some());
        // Should be many years old
        let duration = result.unwrap();
        assert!(duration.as_secs() > 365 * 24 * 3600); // more than 1 year
    }
}
