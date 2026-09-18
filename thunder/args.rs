use clap::Parser;

/// Thunder 服务命令行参数
#[derive(Parser, Debug, Clone)]
#[command(about = "Thunder: 实时帖子内存缓存服务")]
pub struct Args {
    // ─── 服务端口 ───
    /// gRPC 服务端口
    #[arg(long, default_value_t = 50052)]
    pub grpc_port: u16,

    /// HTTP 服务端口（健康检查 / metrics）
    #[arg(long, default_value_t = 8080)]
    pub http_port: u16,

    // ─── 帖子存储 ───
    /// 帖子保留时间（秒），超过此时间的帖子将被自动清除
    #[arg(long, default_value_t = 172800)] // 2 天
    pub post_retention_seconds: u64,

    /// 请求超时时间（毫秒），0 表示不限时
    #[arg(long, default_value_t = 500)]
    pub request_timeout_ms: u64,

    /// 最大并发请求数
    #[arg(long, default_value_t = 100)]
    pub max_concurrent_requests: usize,

    // ─── Kafka 消费者 ───
    /// Kafka broker 地址列表（逗号分隔）
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_brokers: String,

    /// Kafka 消费者组 ID
    #[arg(long, default_value = "thunder-consumer")]
    pub kafka_group_id: String,

    /// Kafka 消费线程数
    #[arg(long, default_value_t = 4)]
    pub kafka_num_threads: usize,

    /// Kafka 批处理大小
    #[arg(long, default_value_t = 1000)]
    pub kafka_batch_size: usize,

    /// 消费者自动偏移重置策略
    #[arg(long, default_value = "earliest")]
    pub auto_offset_reset: String,

    /// 拉取超时（毫秒）
    #[arg(long, default_value_t = 1000)]
    pub fetch_timeout_ms: u64,

    /// 是否跳到最新偏移
    #[arg(long, default_value_t = false)]
    pub skip_to_latest: bool,

    /// Tweet events topic 的分区数（v1 管道）
    #[arg(long, default_value_t = 64)]
    pub tweet_events_num_partitions: usize,

    /// InNetworkEvents topic 的分区数（v2 管道）
    #[arg(long, default_value_t = 64)]
    pub kafka_tweet_events_v2_num_partitions: usize,

    /// InNetworkEvents 消费者目的地
    #[arg(long, default_value = "")]
    pub in_network_events_consumer_dest: String,

    /// 分区 lag 监控间隔（秒）
    #[arg(long, default_value_t = 30)]
    pub lag_monitor_interval_secs: u64,

    // ─── Kafka 认证（SASL）───
    /// 安全协议
    #[arg(long, default_value = "PLAINTEXT")]
    pub security_protocol: String,

    /// SASL 机制（消费者）
    #[arg(long, default_value = "PLAIN")]
    pub sasl_mechanism: String,

    /// SASL 用户名（消费者）
    #[arg(long, default_value = "")]
    pub sasl_username: String,

    /// SASL 密码（消费者）
    #[arg(long)]
    pub sasl_password: Option<String>,

    /// SASL 机制（生产者）
    #[arg(long, default_value = "PLAIN")]
    pub producer_sasl_mechanism: String,

    /// SASL 用户名（生产者）
    #[arg(long, default_value = "")]
    pub producer_sasl_username: String,

    /// SASL 密码（生产者）
    #[arg(long)]
    pub producer_sasl_password: Option<String>,

    // ─── 运行模式 ───
    /// 是否为 serving 模式（消费 v2 管道，提供 gRPC 查询）
    // `SetTrue` (clap's default for bools) cannot express `false` when the
    // default is true. Use value-based parsing so deployments can disable
    // serving explicitly with `--is-serving=false`.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub is_serving: bool,

    /// 是否启用性能分析
    #[arg(long, default_value_t = false)]
    pub enable_profiling: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_home_mixer_client_boundary() {
        let args = Args::parse_from(["thunder"]);
        assert_eq!(args.grpc_port, 50052);
        assert_eq!(args.request_timeout_ms, 500);
        assert!(args.is_serving);
    }

    #[test]
    fn serving_mode_can_be_disabled_explicitly() {
        let args = Args::parse_from(["thunder", "--is-serving=false"]);
        assert!(!args.is_serving);
    }
}
