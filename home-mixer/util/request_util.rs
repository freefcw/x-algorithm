// 请求 ID 生成工具
//
// 为每次推荐管道执行生成唯一的请求 ID，
// 用于链路追踪、日志关联和调试。
//
// 原始 X 实现使用内部的 `xai_request_util` 生成基于 Snowflake 的 ID，
// 这里使用简单的随机 u64 作为 stub 替代。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// 全局递增计数器，确保同一毫秒内生成的 ID 也不重复
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 生成唯一的请求 ID
///
/// 格式: 高 42 位为毫秒时间戳, 低 22 位为递增计数器
/// 这与 Twitter Snowflake ID 的设计思路一致
///
/// # Returns
/// 一个 u64 请求标识符
pub fn generate_request_id() -> u64 {
    let now_ms = u64::try_from(current_time_ms()).unwrap_or(0);

    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);

    // 高 42 位: 毫秒时间戳（可用约 139 年）
    // 低 22 位: 序列号（每毫秒约 400 万个不同 ID）
    (now_ms << 22) | (seq & 0x3F_FFFF)
}

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unique_ids() {
        let id1 = generate_request_id();
        let id2 = generate_request_id();
        assert_ne!(id1, id2, "Two generated IDs should be unique");
    }

    #[test]
    fn test_generate_nonzero_id() {
        let id = generate_request_id();
        assert!(id > 0, "Generated ID should be non-zero");
    }
}
