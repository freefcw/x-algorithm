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

use home_mixer::admin_server::{self, AdminState, Readiness};
use home_mixer::clients::uas_fetcher::{
    RecordOutcome, RedisUserActionSequenceStore, SkipReason, UserActionEvent, UserActionEventSink,
    ValidatedUserAction,
};
use home_mixer::metrics::Metrics;
use home_mixer::params;
use home_mixer::runtime_config::UasConfig;
use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::watch;

/// Admin HTTP port (`/healthz`, `/readyz`, `/metrics`). `0` disables it, for
/// the stdin development path.
const METRICS_PORT_ENV: &str = "UAS_WORKER_METRICS_PORT";
const DEFAULT_METRICS_PORT: u16 = 9091;

/// Prometheus families of the projection job. Counters mirror
/// [`ProjectionStats`] so the 60-second log line and the scrape agree; the
/// gauges come from librdkafka's statistics callback.
struct WorkerMetrics {
    events: IntCounterVec,
    storage_retries: IntCounter,
    /// Unix seconds of the newest action written; its distance from now is
    /// how stale the projection is.
    last_projected_action: IntGauge,
    /// Messages behind the partition high watermark, per assigned partition.
    consumer_lag: IntGaugeVec,
}

impl WorkerMetrics {
    fn new() -> Self {
        Self {
            events: IntCounterVec::new(
                Opts::new(
                    "uas_worker_events_total",
                    "Behavior events by projection outcome",
                ),
                &["outcome"],
            )
            .expect("valid events opts"),
            storage_retries: IntCounter::new(
                "uas_worker_storage_retries_total",
                "Redis writes retried after a transient failure",
            )
            .expect("valid retries opts"),
            last_projected_action: IntGauge::new(
                "uas_worker_last_projected_action_timestamp_seconds",
                "action_time of the newest event written to Redis (unix seconds)",
            )
            .expect("valid last action opts"),
            consumer_lag: IntGaugeVec::new(
                Opts::new(
                    "uas_worker_consumer_lag",
                    "Kafka consumer lag in messages, per assigned partition",
                ),
                &["topic", "partition"],
            )
            .expect("valid lag opts"),
        }
    }

    fn register(&self, metrics: &Metrics) -> anyhow::Result<()> {
        for collector in [
            Box::new(self.events.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(self.storage_retries.clone()),
            Box::new(self.last_projected_action.clone()),
            Box::new(self.consumer_lag.clone()),
        ] {
            metrics
                .register_collector(collector)
                .map_err(|error| anyhow::anyhow!("register uas-worker metrics: {error}"))?;
        }
        Ok(())
    }

    fn observe(&self, result: &ProjectionResult, action: Option<&ValidatedUserAction>) {
        let outcome = match result {
            ProjectionResult::Projected => "projected",
            ProjectionResult::Skipped(SkipReason::OutsideWindow) => "skipped_outside_window",
            ProjectionResult::Skipped(SkipReason::FutureTimestamp) => "skipped_future",
            ProjectionResult::Invalid(_) => "invalid",
            ProjectionResult::StorageUnavailable(_) => "storage_failure",
        };
        self.events.with_label_values(&[outcome]).inc();
        if let (ProjectionResult::Projected, Some(action)) = (result, action) {
            let seconds = action.action_time_ms() / 1_000;
            if seconds > self.last_projected_action.get() {
                self.last_projected_action.set(seconds);
            }
        }
    }

    /// Replace the lag gauges with the partitions currently reported.
    /// Resetting first drops partitions that moved away in a rebalance, and
    /// librdkafka reports `-1` for partitions whose lag it does not know.
    /// Only the Kafka consumer has lag to report; the stdin path never calls it.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    fn set_consumer_lag<'a>(&self, lags: impl IntoIterator<Item = (&'a str, i32, i64)>) {
        self.consumer_lag.reset();
        for (topic, partition, lag) in lags {
            if partition < 0 || lag < 0 {
                continue;
            }
            self.consumer_lag
                .with_label_values(&[topic, &partition.to_string()])
                .set(lag);
        }
    }
}

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
    metrics: Arc<WorkerMetrics>,
}

impl<'a> Projector<'a> {
    fn new(
        sink: &'a dyn UserActionEventSink,
        retry: RetryPolicy,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        Self {
            sink,
            retry,
            stats: ProjectionStats::default(),
            metrics,
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
        self.metrics.observe(&result, Some(&action));
        result
    }

    /// Count and return an `Invalid` result for a record that could not even
    /// be read as text.
    fn reject(&mut self, error: String) -> ProjectionResult {
        let result = ProjectionResult::Invalid(error);
        self.stats.observe(&result);
        self.metrics.observe(&result, None);
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
                    self.metrics.storage_retries.inc();
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

async fn run_stdin(
    store: &RedisUserActionSequenceStore,
    metrics: Arc<WorkerMetrics>,
) -> anyhow::Result<()> {
    let mut projector = Projector::new(store, RetryPolicy::default(), metrics);
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
    use rdkafka::client::ClientContext;
    use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
    use rdkafka::error::KafkaError;
    use rdkafka::message::Message;
    use rdkafka::statistics::Statistics;
    use rdkafka::types::RDKafkaErrorCode;
    use rdkafka::ClientConfig;

    const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(60);
    /// How often librdkafka delivers its statistics (consumer lag) callback.
    const KAFKA_STATISTICS_INTERVAL_MS: &str = "15000";

    /// Receives librdkafka statistics and turns per-partition consumer lag
    /// into gauges. Everything else keeps the default context behavior.
    struct LagReportingContext {
        metrics: Arc<WorkerMetrics>,
    }

    impl ClientContext for LagReportingContext {
        fn stats(&self, statistics: Statistics) {
            self.metrics.set_consumer_lag(consumer_lags(&statistics));
        }
    }

    impl ConsumerContext for LagReportingContext {}

    /// `(topic, partition, lag)` for every partition in the statistics.
    fn consumer_lags(statistics: &Statistics) -> Vec<(&str, i32, i64)> {
        statistics
            .topics
            .iter()
            .flat_map(|(topic, stats)| {
                stats.partitions.values().map(move |partition| {
                    (topic.as_str(), partition.partition, partition.consumer_lag)
                })
            })
            .collect()
    }

    type LagReportingConsumer = StreamConsumer<LagReportingContext>;

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
            .set("statistics.interval.ms", KAFKA_STATISTICS_INTERVAL_MS)
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

    pub(super) async fn run(
        store: &RedisUserActionSequenceStore,
        metrics: Arc<WorkerMetrics>,
        readiness: Readiness,
    ) -> anyhow::Result<()> {
        let (config, topic) = consumer_config()?;
        let consumer: LagReportingConsumer = config.create_with_context(LagReportingContext {
            metrics: Arc::clone(&metrics),
        })?;
        consumer.subscribe(&[&topic])?;
        log::info!("UAS projection job consuming Kafka topic {topic}");
        readiness.set_ready();

        let mut projector = Projector::new(store, RetryPolicy::default(), metrics);
        let mut stream = consumer.stream();
        let mut shutdown = std::pin::pin!(home_mixer::shutdown::signal());
        let mut last_report = Instant::now();
        let outcome = loop {
            let message = tokio::select! {
                _ = &mut shutdown => {
                    log::info!("shutdown signal received; committing settled offsets");
                    readiness.set_draining();
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
    fn commit_stored_offsets(consumer: &LagReportingConsumer) {
        match consumer.commit_consumer_state(CommitMode::Sync) {
            Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
            Err(error) => log::warn!("final Kafka offset commit failed: {error}"),
        }
    }
}

/// `None` disables the admin port; unset or blank selects the default.
fn parse_metrics_port(value: Option<String>) -> anyhow::Result<Option<u16>> {
    match value.map(|value| value.trim().to_string()) {
        None => Ok(Some(DEFAULT_METRICS_PORT)),
        Some(value) if value.is_empty() => Ok(Some(DEFAULT_METRICS_PORT)),
        Some(value) => match value.parse::<u16>() {
            Ok(0) => Ok(None),
            Ok(port) => Ok(Some(port)),
            Err(_) => anyhow::bail!("{METRICS_PORT_ENV} must be a port number (0 disables)"),
        },
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    home_mixer::logging::init_from_env()?;
    // Configuration problems fail before any socket opens.
    let config = UasConfig::redis_from_env()?;
    let metrics_port = parse_metrics_port(env::var(METRICS_PORT_ENV).ok())?;

    // The registry and admin port come up before the Redis handshake so
    // probes answer during a slow start; /readyz reports `starting` until the
    // consumer is subscribed.
    let readiness = Readiness::new();
    let process_metrics = Arc::new(Metrics::process_only());
    let worker_metrics = Arc::new(WorkerMetrics::new());
    worker_metrics.register(&process_metrics)?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let admin = match metrics_port {
        Some(port) => {
            let addr: SocketAddr = ([0, 0, 0, 0], port).into();
            let listener = tokio::net::TcpListener::bind(addr).await.map_err(|error| {
                anyhow::anyhow!("failed to bind admin HTTP port {addr}: {error}")
            })?;
            log::info!("HTTP server listening on {addr} (/healthz, /readyz, /metrics)");
            let mut stop = stop_rx.clone();
            Some(tokio::spawn(admin_server::serve(
                listener,
                AdminState::new(readiness.clone(), Arc::clone(&process_metrics)),
                async move {
                    let _ = stop.wait_for(|stop| *stop).await;
                },
            )))
        }
        None => {
            log::info!("{METRICS_PORT_ENV}=0; admin HTTP port disabled");
            None
        }
    };

    let outcome = run(
        config,
        &readiness,
        worker_metrics,
        process_metrics.client_calls(),
    )
    .await;

    readiness.set_draining();
    let _ = stop_tx.send(true);
    if let Some(admin) = admin {
        match tokio::time::timeout(Duration::from_secs(2), admin).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => log::warn!("admin HTTP server failed: {error}"),
            Ok(Err(error)) => log::warn!("admin HTTP server task failed: {error}"),
            Err(_) => log::warn!("admin HTTP server did not stop within 2 s"),
        }
    }
    outcome
}

async fn run(
    config: home_mixer::clients::uas_fetcher::RedisUserActionSequenceConfig,
    readiness: &Readiness,
    metrics: Arc<WorkerMetrics>,
    client_calls: home_mixer::metrics::ClientCallRecorder,
) -> anyhow::Result<()> {
    let registry_url = env::var("HOME_MIXER_ID_REGISTRY_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:50070".to_string());
    let identity = home_mixer::id::RegistryClient::new(&registry_url)
        .map_err(|error| anyhow::anyhow!("invalid HOME_MIXER_ID_REGISTRY_URL: {error}"))?;
    let store =
        RedisUserActionSequenceStore::new_with_identity(config, std::sync::Arc::new(identity))
            .await
            .map(|store| store.with_calls(client_calls))
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
            return kafka::run(&store, metrics, readiness.clone()).await;
        }
        #[cfg(not(feature = "kafka"))]
        anyhow::bail!(
            "UAS_KAFKA_BROKERS is set but this uas-worker was built without the `kafka` feature; rebuild with `cargo build -p home-mixer --features kafka --bin uas-worker`"
        );
    }
    log::info!("UAS_KAFKA_BROKERS is missing; reading newline-delimited JSON from stdin");
    readiness.set_ready();
    run_stdin(&store, metrics).await
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
        assert_eq!(
            action.user_id(),
            home_mixer::models::ids::ObjectId::from_u64_be_padded(7)
        );
        assert_eq!(action.action_time_ms(), 1_700_000_000_000);

        let with_surface = VALID_EVENT.replacen('}', r#","product_surface":2}"#, 1);
        let action = parse_event(&with_surface).expect("integer product_surface is consumed");
        assert_eq!(
            action.user_id(),
            home_mixer::models::ids::ObjectId::from_u64_be_padded(7)
        );

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
        let mut projector = Projector::new(&healthy, fast_retry(), Arc::new(WorkerMetrics::new()));
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
        let mut projector = Projector::new(&skipping, fast_retry(), Arc::new(WorkerMetrics::new()));
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
        let mut projector = Projector::new(&flaky, fast_retry(), Arc::new(WorkerMetrics::new()));
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
        let mut projector = Projector::new(&down, fast_retry(), Arc::new(WorkerMetrics::new()));
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

    #[test]
    fn metrics_port_defaults_disables_on_zero_and_rejects_garbage() {
        assert_eq!(
            parse_metrics_port(None).unwrap(),
            Some(DEFAULT_METRICS_PORT)
        );
        assert_eq!(
            parse_metrics_port(Some("  ".to_string())).unwrap(),
            Some(DEFAULT_METRICS_PORT)
        );
        assert_eq!(parse_metrics_port(Some("0".to_string())).unwrap(), None);
        assert_eq!(
            parse_metrics_port(Some(" 9200 ".to_string())).unwrap(),
            Some(9200)
        );
        assert!(parse_metrics_port(Some("nine".to_string())).is_err());
        assert!(parse_metrics_port(Some("70000".to_string())).is_err());
    }

    fn exposition(metrics: &WorkerMetrics) -> String {
        let registry = Metrics::process_only();
        metrics.register(&registry).unwrap();
        registry.encode().unwrap()
    }

    #[tokio::test]
    async fn projection_outcomes_and_retries_are_exposed_as_counters() {
        let metrics = Arc::new(WorkerMetrics::new());
        let flaky = StubSink::failing(1);
        let mut projector = Projector::new(&flaky, fast_retry(), Arc::clone(&metrics));
        projector.project_line(VALID_EVENT).await;
        projector.project_line("not-json").await;

        let skipping = StubSink::skipping(SkipReason::OutsideWindow);
        let mut projector = Projector::new(&skipping, fast_retry(), Arc::clone(&metrics));
        projector.project_line(VALID_EVENT).await;

        let text = exposition(&metrics);
        for expected in [
            "uas_worker_events_total{outcome=\"projected\"} 1",
            "uas_worker_events_total{outcome=\"invalid\"} 1",
            "uas_worker_events_total{outcome=\"skipped_outside_window\"} 1",
            "uas_worker_storage_retries_total 1",
            // 1_700_000_000_000 ms → seconds of the projected event.
            "uas_worker_last_projected_action_timestamp_seconds 1700000000",
        ] {
            assert!(text.contains(expected), "missing {expected}\n{text}");
        }
    }

    #[test]
    fn consumer_lag_gauges_follow_the_latest_assignment_and_skip_unknown_lag() {
        let metrics = WorkerMetrics::new();
        metrics.set_consumer_lag(vec![
            ("uas", 0, 12),
            ("uas", 1, 0),
            ("uas", -1, 5),
            ("uas", 2, -1),
        ]);
        let text = exposition(&metrics);
        assert!(
            text.contains("uas_worker_consumer_lag{partition=\"0\",topic=\"uas\"} 12"),
            "{text}"
        );
        assert!(
            text.contains("uas_worker_consumer_lag{partition=\"1\",topic=\"uas\"} 0"),
            "{text}"
        );
        assert!(!text.contains("partition=\"-1\""), "{text}");
        assert!(
            !text.contains("partition=\"2\""),
            "unknown lag is not a zero\n{text}"
        );

        // After a rebalance only the partitions still assigned are reported.
        metrics.set_consumer_lag(vec![("uas", 1, 3)]);
        let text = exposition(&metrics);
        assert!(!text.contains("partition=\"0\""), "{text}");
        assert!(
            text.contains("uas_worker_consumer_lag{partition=\"1\",topic=\"uas\"} 3"),
            "{text}"
        );
    }
}
