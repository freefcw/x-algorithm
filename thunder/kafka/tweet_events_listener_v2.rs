use anyhow::{anyhow, Result};
use log::{info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use x_algorithm_proto::thunder::{in_network_event, LightPost, TweetDeleteEvent};

use crate::{
    args::Args,
    deserializer::deserialize_tweet_event_v2,
    kafka::utils::{deserialize_kafka_messages, KafkaMessage},
    metrics,
    posts::post_store::PostStore,
};

/// Counter for logging deserialization every Nth time
static DESER_LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Start the tweet event processing loop in the background with configurable number of threads
pub async fn start_tweet_event_processing_v2(
    args: &Args,
    post_store: Arc<PostStore>,
    tx: tokio::sync::mpsc::Sender<i64>,
) {
    // A zero thread/batch configuration would either start no consumers or flush
    // every message unexpectedly. Keep the service live even with malformed CLI
    // values (clap validation may be bypassed by programmatic callers).
    let kafka_num_threads = args.kafka_num_threads.max(1);

    info!(
        "Starting {} Kafka v2 consumer threads (brokers: {})",
        kafka_num_threads, args.kafka_brokers
    );

    // All consumers must share one group so Kafka assigns partitions among them.
    // Giving each thread a distinct group causes every event to be consumed N times.
    let group_id = format!("{}-v2", args.kafka_group_id);
    let topic = if args.in_network_events_consumer_dest.is_empty() {
        "in-network-events".to_owned()
    } else {
        args.in_network_events_consumer_dest.clone()
    };

    for thread_id in 0..kafka_num_threads {
        let post_store_clone = Arc::clone(&post_store);
        let brokers = args.kafka_brokers.clone();
        let batch_size = args.kafka_batch_size.max(1);
        let tx_clone = tx.clone();
        let security_protocol = args.security_protocol.clone();
        let sasl_mechanism = args.sasl_mechanism.clone();
        let sasl_username = args.sasl_username.clone();
        let sasl_password = args.sasl_password.clone();
        let auto_offset_reset = if args.skip_to_latest {
            "latest".to_owned()
        } else {
            args.auto_offset_reset.clone()
        };
        let fetch_timeout_ms = args.fetch_timeout_ms;
        let topic = topic.clone();
        let group_id = group_id.clone();

        tokio::spawn(async move {
            info!("Starting v2 consumer thread {}", thread_id,);

            // Build rdkafka consumer
            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group_id)
                .set("auto.offset.reset", &auto_offset_reset)
                .set("enable.auto.commit", "false")
                .set("security.protocol", &security_protocol)
                .set("fetch.wait.max.ms", fetch_timeout_ms.to_string());

            if security_protocol != "PLAINTEXT" {
                config.set("sasl.mechanism", &sasl_mechanism);
                config.set("sasl.username", &sasl_username);
                if let Some(ref password) = sasl_password {
                    config.set("sasl.password", password);
                }
            }

            let consumer: StreamConsumer =
                config.create().expect("Failed to create Kafka consumer");

            // Subscribe to in-network events topic
            consumer
                .subscribe(&[topic.as_str()])
                .expect("Failed to subscribe to topic");

            if let Err(e) =
                process_tweet_events_v2(consumer, post_store_clone, batch_size, tx_clone).await
            {
                panic!(
                    "Tweet events v2 processing thread {} exited unexpectedly: {:#}",
                    thread_id, e
                );
            }
        });
    }
}

/// Deserialize a batch of InNetworkEvent messages
fn deserialize_batch(
    messages: Vec<KafkaMessage>,
) -> Result<(Vec<LightPost>, Vec<TweetDeleteEvent>)> {
    let start_time = Instant::now();
    let num_messages = messages.len();
    let results = deserialize_kafka_messages(messages, deserialize_tweet_event_v2)?;
    let deser_elapsed = start_time.elapsed();
    if DESER_LOG_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(1000)
    {
        info!(
            "Deserialized {} messages in {:?} ({:.2} msgs/sec)",
            num_messages,
            deser_elapsed,
            num_messages as f64 / deser_elapsed.as_secs_f64()
        );
    }

    let mut create_tweets = Vec::with_capacity(results.len());
    let mut delete_tweets = Vec::with_capacity(10);

    for tweet_event in results {
        if let Some(variant) = tweet_event.event_variant {
            match variant {
                in_network_event::EventVariant::TweetCreateEvent(create_event) => {
                    create_tweets.push(LightPost {
                        post_id: create_event.post_id,
                        author_id: create_event.author_id,
                        created_at: create_event.created_at,
                        in_reply_to_post_id: create_event.in_reply_to_post_id,
                        in_reply_to_user_id: create_event.in_reply_to_user_id,
                        is_retweet: create_event.is_retweet,
                        is_reply: create_event.is_reply
                            || create_event.in_reply_to_post_id.is_some()
                            || create_event.in_reply_to_user_id.is_some(),
                        source_post_id: create_event.source_post_id,
                        source_user_id: create_event.source_user_id,
                        has_video: create_event.has_video,
                        conversation_id: create_event.conversation_id,
                    });
                }
                in_network_event::EventVariant::TweetDeleteEvent(delete_event) => {
                    delete_tweets.push(delete_event);
                }
            }
        }
    }

    Ok((create_tweets, delete_tweets))
}

/// Main message processing loop using rdkafka StreamConsumer
async fn process_tweet_events_v2(
    consumer: StreamConsumer,
    post_store: Arc<PostStore>,
    batch_size: usize,
    tx: tokio::sync::mpsc::Sender<i64>,
) -> Result<()> {
    let mut message_buffer: Vec<KafkaMessage> = Vec::new();
    let mut init_data_downloaded = false;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = msg.payload().map(|p| p.to_vec());
                message_buffer.push(KafkaMessage { payload });

                // Process batch when we have enough messages
                if message_buffer.len() >= batch_size {
                    let messages = std::mem::take(&mut message_buffer);
                    let post_store_clone = Arc::clone(&post_store);

                    tokio::task::spawn_blocking(move || {
                        match deserialize_batch(messages) {
                            Err(e) => warn!("Error processing batch: {:#}", e),
                            Ok((light_posts, delete_posts)) => {
                                post_store_clone.insert_posts(light_posts);
                                post_store_clone.mark_as_deleted(delete_posts);
                            }
                        };
                    })
                    .await
                    .map_err(|e| anyhow!("batch processing task failed: {e}"))?;

                    // Commit offsets after processing
                    if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                        warn!("Failed to commit offsets: {}", e);
                    }

                    // Signal init completion once caught up
                    if !init_data_downloaded {
                        init_data_downloaded = true;
                        info!("Completed kafka v2 init for a single thread");
                        if let Err(e) = tx.send(0).await {
                            log::error!("error sending init signal: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error receiving Kafka message: {}", e);
                metrics::KAFKA_POLL_ERRORS.inc();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
