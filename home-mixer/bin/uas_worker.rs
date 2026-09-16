//! UAS projection job.
//!
//! This process is deliberately separate from the recommendation RPC server:
//! it consumes the existing behavior topic and projects a small, bounded
//! seven-day sequence into Redis. When Kafka is not available, newline-delimited
//! JSON on stdin provides a development-only projection path for local bring-up.
//!
//! Delivery contract: an event is validated once, at the JSON boundary. A
//! settled event (stored, deliberately skipped, or invalid) lets the consumer
//! advance; a storage failure is retried in-process with backoff because the
//! Redis write is idempotent, and only when the retry budget is exhausted does
//! the job exit with that offset left uncommitted for replay after a restart.

use home_mixer::clients::uas_fetcher::{
    RecordOutcome, RedisUserActionSequenceStore, SkipReason, UserActionEvent, UserActionEventSink,
    ValidatedUserAction,
};
use home_mixer::params;
use home_mixer::runtime_config::UasConfig;
use std::env;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

fn parse_event(line: &str) -> Result<ValidatedUserAction, String> {
    let event: UserActionEvent =
        serde_json::from_str(line).map_err(|error| format!("invalid UAS event JSON: {error}"))?;
    event.validate()
}

#[derive(Debug, Eq, PartialEq)]
enum ProjectionResult {
    Projected,
    /// Accepted but not written (outside the window, future timestamp).
    Skipped(SkipReason),
    /// Poison message: never reaches storage and is dropped.
    Invalid(String),
    /// Storage kept failing past the retry budget; the record is not settled.
    StorageUnavailable(String),
}

impl ProjectionResult {
    /// Whether the consumer may advance past the record that produced this.
    fn is_settled(&self) -> bool {
        !matches!(self, Self::StorageUnavailable(_))
    }
}

/// In-process retry for idempotent Redis writes. Bounded well below Kafka's
/// `max.poll.interval.ms` so a stalled Redis surfaces as a job exit and
/// replay rather than as a silent consumer-group eviction.
#[derive(Clone, Copy, Debug)]
struct RetryPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
    budget: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            budget: Duration::from_millis(params::UAS_PROJECTION_RETRY_BUDGET_MS),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct ProjectionStats {
    projected: u64,
    skipped_outside_window: u64,
    skipped_future: u64,
    invalid: u64,
    storage_retries: u64,
    storage_failures: u64,
}

impl ProjectionStats {
    fn observe(&mut self, result: &ProjectionResult) {
        match result {
            ProjectionResult::Projected => self.projected += 1,
            ProjectionResult::Skipped(SkipReason::OutsideWindow) => {
                self.skipped_outside_window += 1
            }
            ProjectionResult::Skipped(SkipReason::FutureTimestamp) => self.skipped_future += 1,
            ProjectionResult::Invalid(_) => self.invalid += 1,
            ProjectionResult::StorageUnavailable(_) => self.storage_failures += 1,
        }
    }

    fn log(&self, context: &str) {
        log::info!(
            "{context}: projected={} skipped_outside_window={} skipped_future={} invalid={} storage_retries={} storage_failures={}",
            self.projected,
            self.skipped_outside_window,
            self.skipped_future,
            self.invalid,
            self.storage_retries,
            self.storage_failures
        );
    }
}

struct Projector<'a> {
    sink: &'a dyn UserActionEventSink,
    retry: RetryPolicy,
    stats: ProjectionStats,
}

impl<'a> Projector<'a> {
    fn new(sink: &'a dyn UserActionEventSink, retry: RetryPolicy) -> Self {
        Self {
            sink,
            retry,
            stats: ProjectionStats::default(),
        }
    }

    async fn project_line(&mut self, line: &str) -> ProjectionResult {
        let action = match parse_event(line) {
            Ok(action) => action,
            Err(error) => return self.reject(error),
        };
        let result = match self.record_with_retry(&action).await {
            Ok(RecordOutcome::Stored) => ProjectionResult::Projected,
            Ok(RecordOutcome::Skipped(reason)) => ProjectionResult::Skipped(reason),
            Err(error) => ProjectionResult::StorageUnavailable(error),
        };
        self.stats.observe(&result);
        result
    }

    /// Count and return an `Invalid` result for a record that could not even
    /// be read as text.
    fn reject(&mut self, error: String) -> ProjectionResult {
        let result = ProjectionResult::Invalid(error);
        self.stats.observe(&result);
        result
    }

    async fn record_with_retry(
        &mut self,
        action: &ValidatedUserAction,
    ) -> Result<RecordOutcome, String> {
        let started = Instant::now();
        let mut backoff = self.retry.initial_backoff;
        loop {
            match self.sink.record(action).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) => {
                    if started.elapsed() + backoff > self.retry.budget {
                        return Err(error);
                    }
                    self.stats.storage_retries += 1;
                    log::warn!(
                        "UAS Redis write for user {} failed ({error}); retrying in {backoff:?}",
                        action.user_id()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                }
            }
        }
    }
}

fn log_result(result: &ProjectionResult, source: &str) {
    match result {
        ProjectionResult::Projected => {}
        ProjectionResult::Skipped(reason) => {
            log::debug!("skipping UAS event at {source}: {}", reason.as_str());
        }
        ProjectionResult::Invalid(error) => {
            log::warn!("dropping invalid UAS event at {source}: {error}");
        }
        ProjectionResult::StorageUnavailable(error) => {
            log::error!("UAS projection failed at {source} after retries: {error}");
        }
    }
}

async fn run_stdin(store: &RedisUserActionSequenceStore) -> anyhow::Result<()> {
    let mut projector = Projector::new(store, RetryPolicy::default());
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut line_number = 0u64;
    while let Some(line) = lines.next_line().await? {
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }
        let result = projector.project_line(&line).await;
        log_result(&result, &format!("stdin:{line_number}"));
        if !result.is_settled() {
            projector.stats.log("stdin projection aborted");
            anyhow::bail!(
                "UAS projection failed at stdin:{line_number}; later lines were not read"
            );
        }
    }
    projector.stats.log("stdin projection finished");
    Ok(())
}

#[cfg(feature = "kafka")]
mod kafka {
    use super::*;
    use futures::StreamExt;
    use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
    use rdkafka::error::KafkaError;
    use rdkafka::message::Message;
    use rdkafka::types::RDKafkaErrorCode;
    use rdkafka::ClientConfig;

    const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(60);

    fn required_env(name: &str) -> anyhow::Result<String> {
        let value = env::var(name).unwrap_or_default();
        if value.trim().is_empty() {
            anyhow::bail!("{name} must be configured")
        }
        Ok(value)
    }

    /// Only the SASL protocols take credentials; plain `SSL` must not demand
    /// a username and password it has no use for.
    pub(super) fn requires_sasl(security_protocol: &str) -> bool {
        matches!(
            security_protocol.trim().to_ascii_uppercase().as_str(),
            "SASL_PLAINTEXT" | "SASL_SSL"
        )
    }

    fn consumer_config() -> anyhow::Result<(ClientConfig, String)> {
        let brokers = required_env("UAS_KAFKA_BROKERS")?;
        let topic = required_env("UAS_KAFKA_TOPIC")?;
        let group = env::var("UAS_KAFKA_GROUP_ID")
            .unwrap_or_else(|_| "home-mixer-uas-projector".to_string());
        let offset_reset =
            env::var("UAS_KAFKA_AUTO_OFFSET_RESET").unwrap_or_else(|_| "earliest".to_string());
        let security_protocol =
            env::var("UAS_KAFKA_SECURITY_PROTOCOL").unwrap_or_else(|_| "PLAINTEXT".to_string());
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            // Offsets are stored by hand once a record settles and committed
            // by librdkafka in the background (and on rebalance). A record
            // whose write never succeeded keeps its offset unstored, so it is
            // replayed after a restart.
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", offset_reset)
            .set("security.protocol", &security_protocol);
        if requires_sasl(&security_protocol) {
            config
                .set(
                    "sasl.mechanism",
                    env::var("UAS_KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "PLAIN".to_string()),
                )
                .set("sasl.username", required_env("UAS_KAFKA_SASL_USERNAME")?)
                .set("sasl.password", required_env("UAS_KAFKA_SASL_PASSWORD")?);
        }
        Ok((config, topic))
    }

    pub(super) async fn run(store: &RedisUserActionSequenceStore) -> anyhow::Result<()> {
        let (config, topic) = consumer_config()?;
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(&[&topic])?;
        log::info!("UAS projection job consuming Kafka topic {topic}");

        let mut projector = Projector::new(store, RetryPolicy::default());
        let mut stream = consumer.stream();
        let mut shutdown = std::pin::pin!(home_mixer::shutdown::signal());
        let mut last_report = Instant::now();
        let outcome = loop {
            let message = tokio::select! {
                _ = &mut shutdown => {
                    log::info!("shutdown signal received; committing settled offsets");
                    break Ok(());
                }
                message = stream.next() => match message {
                    Some(message) => message,
                    None => break Ok(()),
                },
            };
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    log::warn!("Kafka UAS consumer error: {error}");
                    continue;
                }
            };
            let source = format!(
                "{}[{}]@{}",
                message.topic(),
                message.partition(),
                message.offset()
            );
            let result = match message.payload_view::<str>() {
                Some(Ok(payload)) => projector.project_line(payload).await,
                Some(Err(error)) => projector.reject(format!("payload is not UTF-8: {error}")),
                None => projector.reject("record has no payload".to_string()),
            };
            log_result(&result, &source);
            if !result.is_settled() {
                // Leave this offset unstored so the record is replayed after
                // the restart. Later records on the partition are not
                // processed: storing their offsets would skip this one.
                break Err(anyhow::anyhow!(
                    "UAS projection failed at {source}; offset left uncommitted"
                ));
            }
            consumer.store_offset_from_message(&message)?;
            if last_report.elapsed() >= STATS_REPORT_INTERVAL {
                projector.stats.log("uas-worker");
                last_report = Instant::now();
            }
        };
        projector.stats.log("uas-worker exiting");
        commit_stored_offsets(&consumer);
        outcome
    }

    /// Flush the stored offsets before exiting so a clean shutdown does not
    /// replay up to `auto.commit.interval.ms` worth of settled records.
    fn commit_stored_offsets(consumer: &StreamConsumer) {
        match consumer.commit_consumer_state(CommitMode::Sync) {
            Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
            Err(error) => log::warn!("final Kafka offset commit failed: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    home_mixer::logging::init_from_env()?;
    let config = UasConfig::redis_from_env()?;
    let store = RedisUserActionSequenceStore::new(config)
        .await
        .map_err(anyhow::Error::msg)?;
    // A demo Home Mixer only reads this Redis when UAS_REDIS_URL is set on
    // its side too; say which variable chose the target so a local bring-up
    // that "projects but is not read" is easy to diagnose.
    let target_variable = if env::var("UAS_REDIS_URL").is_ok_and(|value| !value.trim().is_empty()) {
        "UAS_REDIS_URL"
    } else {
        "HOME_MIXER_REDIS_URL"
    };
    log::info!("UAS projection target: Redis from {target_variable}");
    let kafka_configured =
        env::var("UAS_KAFKA_BROKERS").is_ok_and(|value| !value.trim().is_empty());
    if kafka_configured {
        #[cfg(feature = "kafka")]
        {
            return kafka::run(&store).await;
        }
        #[cfg(not(feature = "kafka"))]
        anyhow::bail!(
            "UAS_KAFKA_BROKERS is set but this uas-worker was built without the `kafka` feature; rebuild with `cargo build -p home-mixer --features kafka --bin uas-worker`"
        );
    }
    log::info!("UAS_KAFKA_BROKERS is missing; reading newline-delimited JSON from stdin");
    run_stdin(&store).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fails the first `failures` writes, then reports `outcome`.
    struct StubSink {
        failures: usize,
        outcome: RecordOutcome,
        calls: AtomicUsize,
    }

    impl StubSink {
        fn healthy() -> Self {
            Self::failing(0)
        }

        fn failing(failures: usize) -> Self {
            Self {
                failures,
                outcome: RecordOutcome::Stored,
                calls: AtomicUsize::new(0),
            }
        }

        fn skipping(reason: SkipReason) -> Self {
            Self {
                failures: 0,
                outcome: RecordOutcome::Skipped(reason),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[tonic::async_trait]
    impl UserActionEventSink for StubSink {
        async fn record(&self, _action: &ValidatedUserAction) -> Result<RecordOutcome, String> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call < self.failures {
                Err("Redis unavailable".to_string())
            } else {
                Ok(self.outcome)
            }
        }
    }

    fn fast_retry() -> RetryPolicy {
        RetryPolicy {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            budget: Duration::from_millis(200),
        }
    }

    const VALID_EVENT: &str = r#"{"user_id":"000000000000000000000007","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1700000000000,"action_type":3}"#;

    #[test]
    fn event_parser_accepts_the_public_contract_and_ignores_unknown_fields() {
        let action = parse_event(VALID_EVENT).expect("valid UAS event");
        assert_eq!(action.user_id(), home_mixer::models::uid(7));
        assert_eq!(action.action_time_ms(), 1_700_000_000_000);

        let with_surface = VALID_EVENT.replacen('}', r#","product_surface":2}"#, 1);
        let action = parse_event(&with_surface).expect("integer product_surface is consumed");
        assert_eq!(action.user_id(), home_mixer::models::uid(7));

        let extended = VALID_EVENT.replacen('}', r#","event_id":"evt-1"}"#, 1);
        assert!(parse_event(&extended).is_ok(), "producers may add fields");

        let as_string = VALID_EVENT.replacen('}', r#","product_surface":"timeline"}"#, 1);
        assert!(
            parse_event(&as_string).is_err(),
            "product_surface must be an integer 0..=15"
        );
    }

    #[test]
    fn event_parser_rejects_poison_messages_before_projection() {
        for payload in [
            "not-json",
            r#"{"user_id":"bad","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1700000000000,"action_type":3}"#,
            r#"{"user_id":"000000000000000000000007","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1700000000000,"action_type":99}"#,
            r#"{"user_id":"000000000000000000000007","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1700000000000}"#,
        ] {
            assert!(
                parse_event(payload).is_err(),
                "payload should fail: {payload}"
            );
        }
    }

    #[tokio::test]
    async fn settled_results_advance_and_invalid_messages_never_reach_storage() {
        let healthy = StubSink::healthy();
        let mut projector = Projector::new(&healthy, fast_retry());
        assert_eq!(
            projector.project_line(VALID_EVENT).await,
            ProjectionResult::Projected
        );
        assert!(matches!(
            projector.project_line("not-json").await,
            ProjectionResult::Invalid(_)
        ));
        assert_eq!(
            healthy.calls(),
            1,
            "invalid messages must not reach storage"
        );
        assert_eq!(projector.stats.projected, 1);
        assert_eq!(projector.stats.invalid, 1);

        let skipping = StubSink::skipping(SkipReason::FutureTimestamp);
        let mut projector = Projector::new(&skipping, fast_retry());
        let result = projector.project_line(VALID_EVENT).await;
        assert_eq!(
            result,
            ProjectionResult::Skipped(SkipReason::FutureTimestamp)
        );
        assert!(
            result.is_settled(),
            "a skipped event must not block the partition"
        );
        assert_eq!(projector.stats.skipped_future, 1);
    }

    #[tokio::test]
    async fn transient_storage_failures_are_retried_within_the_budget() {
        let flaky = StubSink::failing(2);
        let mut projector = Projector::new(&flaky, fast_retry());
        assert_eq!(
            projector.project_line(VALID_EVENT).await,
            ProjectionResult::Projected
        );
        assert_eq!(flaky.calls(), 3);
        assert_eq!(projector.stats.storage_retries, 2);
        assert_eq!(projector.stats.storage_failures, 0);
    }

    #[tokio::test]
    async fn exhausted_retry_budget_surfaces_as_unsettled_storage_failure() {
        let down = StubSink::failing(usize::MAX);
        let mut projector = Projector::new(&down, fast_retry());
        let result = projector.project_line(VALID_EVENT).await;
        assert!(matches!(result, ProjectionResult::StorageUnavailable(_)));
        assert!(!result.is_settled(), "the offset must stay uncommitted");
        assert!(
            down.calls() > 1,
            "the write must be retried before giving up"
        );
        assert_eq!(projector.stats.storage_failures, 1);
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn only_sasl_protocols_require_credentials() {
        assert!(kafka::requires_sasl("SASL_SSL"));
        assert!(kafka::requires_sasl("sasl_plaintext"));
        assert!(!kafka::requires_sasl("SSL"));
        assert!(!kafka::requires_sasl("PLAINTEXT"));
    }
}
