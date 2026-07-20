//! 演示模式共享契约
//!
//! thunder 与 home-mixer 的演示数据必须彼此咬合才能跑通链路：
//! thunder 灌入的帖子作者，必须出现在 home-mixer 演示用户的关注列表里。
//! 这份契约由两个服务共同遵守，因此常量放在双方共同依赖的 proto crate 中，
//! 避免两边各写一份后悄悄漂移。

/// 演示宇宙中的账号集合。
/// thunder 演示模式用它作为帖子作者；home-mixer 演示模式用它作为 viewer 的关注列表。
pub const DEMO_AUTHOR_IDS: [i64; 5] = [101, 102, 103, 104, 105];

/// Twitter Snowflake 纪元（毫秒）：2010-11-04T01:42:54.657Z。
/// 与 `home-mixer/util/snowflake.rs` 的解析逻辑对应。
pub const TWITTER_EPOCH_MS: i64 = 1288834974657;

/// 用毫秒时间戳合成一个 Snowflake 风格的帖子 ID（高 41 位为时间戳）。
///
/// 演示数据的帖子 ID 必须长得像"最近发布"，
/// 否则 home-mixer 的 `AgeFilter` 会按 ID 解出远古时间并把帖子全部过滤掉。
/// 低 22 位填入序号，保证同一毫秒生成的 ID 也不重复。
pub fn snowflake_id(timestamp_ms: i64, sequence: i64) -> i64 {
    ((timestamp_ms - TWITTER_EPOCH_MS) << 22) | (sequence & 0x3F_FFFF)
}

/// 当前毫秒时间戳。
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_id_roundtrip() {
        // 高 41 位必须能还原出原始时间戳，这是 AgeFilter 解析的前提
        let ts = 1_800_000_000_000_i64;
        let id = snowflake_id(ts, 7);
        assert_eq!((id >> 22) + TWITTER_EPOCH_MS, ts);
    }

    #[test]
    fn test_snowflake_id_sequence_uniqueness() {
        let ts = 1_800_000_000_000_i64;
        assert_ne!(snowflake_id(ts, 1), snowflake_id(ts, 2));
    }
}
