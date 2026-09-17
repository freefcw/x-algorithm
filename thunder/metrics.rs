// Thunder 服务 Prometheus 监控指标定义
//
// 所有指标名称和标签均从代码中实际的使用方式逆向推导而来。

use lazy_static::lazy_static;
use prometheus::{
    register_gauge, register_gauge_vec, register_histogram, register_histogram_vec,
    register_int_counter, register_int_gauge, Gauge, GaugeVec, Histogram, HistogramVec, IntCounter,
    IntGauge,
};
use std::time::Instant;

lazy_static! {
    // 进程生命周期指标（由 /metrics 处理器在抓取时刷新）
    pub static ref THUNDER_READY: IntGauge =
        register_int_gauge!("thunder_ready", "1 while the process serves queries, 0 while starting or draining").unwrap();
    // ═══════════════════════════════════════════════════════════════
    // gRPC 服务指标
    // ═══════════════════════════════════════════════════════════════

    /// GetInNetworkPosts 返回的帖子数分布
    pub static ref GET_IN_NETWORK_POSTS_COUNT: Histogram =
        register_histogram!("thunder_get_in_network_posts_count", "Posts returned per request").unwrap();

    /// GetInNetworkPosts 总延迟
    pub static ref GET_IN_NETWORK_POSTS_DURATION: Histogram =
        register_histogram!("thunder_get_in_network_posts_duration_seconds", "Total request duration").unwrap();

    /// GetInNetworkPosts 除去 Strato 调用后的延迟
    pub static ref GET_IN_NETWORK_POSTS_DURATION_WITHOUT_STRATO: Histogram =
        register_histogram!("thunder_get_in_network_posts_duration_without_strato_seconds", "Duration without strato").unwrap();

    /// 请求中 following_user_ids 列表大小
    pub static ref GET_IN_NETWORK_POSTS_FOLLOWING_SIZE: Histogram =
        register_histogram!("thunder_get_in_network_posts_following_size", "Following list size").unwrap();

    /// 请求中 exclude_tweet_ids 列表大小
    pub static ref GET_IN_NETWORK_POSTS_EXCLUDED_SIZE: Histogram =
        register_histogram!("thunder_get_in_network_posts_excluded_size", "Excluded tweets size").unwrap();

    /// 最新帖子的新鲜度（秒）
    pub static ref GET_IN_NETWORK_POSTS_FOUND_FRESHNESS_SECONDS: HistogramVec =
        register_histogram_vec!(
            "thunder_get_in_network_posts_found_freshness_seconds",
            "Time since most recent post",
            &["stage"]
        ).unwrap();

    /// 找到的帖子时间范围（秒）
    pub static ref GET_IN_NETWORK_POSTS_FOUND_TIME_RANGE_SECONDS: HistogramVec =
        register_histogram_vec!(
            "thunder_get_in_network_posts_found_time_range_seconds",
            "Time range of found posts",
            &["stage"]
        ).unwrap();

    /// 帖子中回复的占比
    pub static ref GET_IN_NETWORK_POSTS_FOUND_REPLY_RATIO: HistogramVec =
        register_histogram_vec!(
            "thunder_get_in_network_posts_found_reply_ratio",
            "Ratio of replies in found posts",
            &["stage"]
        ).unwrap();

    /// 唯一作者数
    pub static ref GET_IN_NETWORK_POSTS_FOUND_UNIQUE_AUTHORS: HistogramVec =
        register_histogram_vec!(
            "thunder_get_in_network_posts_found_unique_authors",
            "Number of unique authors",
            &["stage"]
        ).unwrap();

    /// 每个作者的平均帖子数
    pub static ref GET_IN_NETWORK_POSTS_FOUND_POSTS_PER_AUTHOR: HistogramVec =
        register_histogram_vec!(
            "thunder_get_in_network_posts_found_posts_per_author",
            "Posts per author",
            &["stage"]
        ).unwrap();

    /// 请求的 max_results 值
    pub static ref GET_IN_NETWORK_POSTS_MAX_RESULTS: Histogram =
        register_histogram!("thunder_get_in_network_posts_max_results", "Max results per request").unwrap();

    /// 当前正在处理的请求数
    pub static ref IN_FLIGHT_REQUESTS: Gauge =
        register_gauge!("thunder_in_flight_requests", "Number of in-flight requests").unwrap();

    /// 被拒绝的请求数（服务过载时）
    pub static ref REJECTED_REQUESTS: IntCounter =
        register_int_counter!("thunder_rejected_requests_total", "Rejected requests due to capacity").unwrap();

    // ═══════════════════════════════════════════════════════════════
    // PostStore 存储指标
    // ═══════════════════════════════════════════════════════════════

    /// 用户数
    pub static ref POST_STORE_USER_COUNT: Gauge =
        register_gauge!("thunder_post_store_user_count", "Number of users in store").unwrap();

    /// 帖子总数
    pub static ref POST_STORE_TOTAL_POSTS: Gauge =
        register_gauge!("thunder_post_store_total_posts", "Total posts in store").unwrap();

    /// 已删除帖子数
    pub static ref POST_STORE_DELETED_POSTS: Gauge =
        register_gauge!("thunder_post_store_deleted_posts", "Deleted posts count").unwrap();

    /// 被过滤的已删除帖子
    pub static ref POST_STORE_DELETED_POSTS_FILTERED: IntCounter =
        register_int_counter!("thunder_post_store_deleted_posts_filtered_total", "Deleted posts filtered during query").unwrap();

    /// 各类实体计数（按标签分组：users/posts/original/secondary/video/deleted）
    pub static ref POST_STORE_ENTITY_COUNT: GaugeVec =
        register_gauge_vec!("thunder_post_store_entity_count", "Entity counts by type", &["type"]).unwrap();

    /// 每次请求返回的帖子数
    pub static ref POST_STORE_POSTS_RETURNED: Histogram =
        register_histogram!("thunder_post_store_posts_returned", "Posts returned per query").unwrap();

    /// 返回帖子占可用帖子的比例
    pub static ref POST_STORE_POSTS_RETURNED_RATIO: Histogram =
        register_histogram!("thunder_post_store_posts_returned_ratio", "Ratio of returned vs eligible posts").unwrap();

    /// 请求超时次数
    pub static ref POST_STORE_REQUEST_TIMEOUTS: IntCounter =
        register_int_counter!("thunder_post_store_request_timeouts_total", "PostStore request timeouts").unwrap();

    /// PostStore 请求总数
    pub static ref POST_STORE_REQUESTS: IntCounter =
        register_int_counter!("thunder_post_store_requests_total", "Total PostStore requests").unwrap();

    // ═══════════════════════════════════════════════════════════════
    // Kafka 指标
    // ═══════════════════════════════════════════════════════════════

    /// Kafka 分区 lag
    pub static ref KAFKA_PARTITION_LAG: GaugeVec =
        register_gauge_vec!("thunder_kafka_partition_lag", "Kafka partition lag", &["topic", "partition"]).unwrap();

    /// Kafka 轮询错误次数
    pub static ref KAFKA_POLL_ERRORS: IntCounter =
        register_int_counter!("thunder_kafka_poll_errors_total", "Kafka poll errors").unwrap();

    /// Kafka 消息解析失败次数
    pub static ref KAFKA_MESSAGES_FAILED_PARSE: IntCounter =
        register_int_counter!("thunder_kafka_messages_failed_parse_total", "Failed message parse count").unwrap();

    /// 批处理耗时
    pub static ref BATCH_PROCESSING_TIME: Histogram =
        register_histogram!("thunder_batch_processing_seconds", "Batch processing duration").unwrap();
}

/// 计时器辅助结构体 —— 在 Drop 时自动记录耗时到 Histogram
pub struct Timer {
    histogram: Histogram,
    start: Instant,
}

impl Timer {
    pub fn new(histogram: Histogram) -> Self {
        Timer {
            histogram,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.histogram.observe(elapsed);
    }
}
