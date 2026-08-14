use anyhow::Result;
use log::info;
use std::sync::Arc;

use crate::{args, kafka::tweet_events_listener_v2::start_tweet_event_processing_v2};

/// 初始化并启动 Kafka 消费管道
///
/// 在 serving 模式下，启动 v2 管道（Protobuf 格式的 InNetworkEvents）。
/// v1 管道（Thrift 格式的 TweetEvents）已移除，如需恢复请启用 `legacy` feature。
pub async fn start_kafka(
    args: &args::Args,
    post_store: Arc<crate::posts::post_store::PostStore>,
    _user: &str,
    tx: tokio::sync::mpsc::Sender<i64>,
) -> Result<()> {
    if args.is_serving {
        info!(
            "Starting Kafka v2 consumer pipeline (brokers: {}, group: {})",
            args.kafka_brokers, args.kafka_group_id
        );

        start_tweet_event_processing_v2(args, post_store, tx).await;
    } else {
        info!("Not in serving mode, skipping Kafka consumer startup");
        // v1 管道（Thrift feeder → Proto producer）已移除
        // 如需要原始 Thrift 格式消费，请启用 `legacy` feature
    }

    Ok(())
}
