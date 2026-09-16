//! `ServedCandidatesSink`（SE-11）的传输适配器。
//!
//! 事件 schema 由 `side_effects::served_candidates_kafka_side_effect` 定义，这里只把
//! 一条 [`ServedCandidatesEvent`] 序列化成 JSON 并交给目标：
//!
//! - [`JsonLinesServedCandidatesSink`]：追加写本地 JSON Lines 文件，用于本地联调和
//!   离线抓样本，不适合多副本部署；
//! - `KafkaServedCandidatesSink`：发到 Kafka topic（需要 `kafka` feature），分区键是
//!   `viewer_id`，同一用户的曝光落在同一分区，便于与行为事件按用户 join。
//!
//! 传输失败向流水线返回 `Err`，`CandidatePipeline` 把它记录为 side effect 失败；
//! side effect 在响应之后异步执行，不进入请求预算，也不重试（at-most-once）。
//! 需要更强的投递保证时由 Kafka 侧 `enable.idempotence` + 消费方按 `request_id`
//! 去重承担，而不是在请求路径上阻塞。

use crate::side_effects::served_candidates_kafka_side_effect::{
    ServedCandidatesEvent, ServedCandidatesSink,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tonic::async_trait;

pub const SERVED_EVENTS_JSONL_PATH_ENV: &str = "SERVED_EVENTS_JSONL_PATH";
pub const SERVED_EVENTS_KAFKA_BROKERS_ENV: &str = "SERVED_EVENTS_KAFKA_BROKERS";
pub const SERVED_EVENTS_KAFKA_TOPIC_ENV: &str = "SERVED_EVENTS_KAFKA_TOPIC";
pub const SERVED_EVENTS_KAFKA_SECURITY_PROTOCOL_ENV: &str = "SERVED_EVENTS_KAFKA_SECURITY_PROTOCOL";
pub const SERVED_EVENTS_KAFKA_SASL_MECHANISM_ENV: &str = "SERVED_EVENTS_KAFKA_SASL_MECHANISM";
pub const SERVED_EVENTS_KAFKA_SASL_USERNAME_ENV: &str = "SERVED_EVENTS_KAFKA_SASL_USERNAME";
pub const SERVED_EVENTS_KAFKA_SASL_PASSWORD_ENV: &str = "SERVED_EVENTS_KAFKA_SASL_PASSWORD";
pub const SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV: &str =
    "SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS";

const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_millis(5_000);

/// Which transport the assembly injects. Resolved once at startup; the default is
/// no sink at all, so a deployment that never configured exposure logging keeps
/// its current behavior instead of silently writing files.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ServedCandidatesSinkConfig {
    #[default]
    Disabled,
    JsonLines(PathBuf),
    Kafka(KafkaSinkConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    pub topic: String,
    pub security_protocol: String,
    pub sasl: Option<SaslCredentials>,
    /// Upper bound for one `send` including broker acknowledgement.
    pub delivery_timeout: Duration,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SaslCredentials {
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for SaslCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslCredentials")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Only the SASL protocols take credentials; plain `SSL` must not demand a
/// username and password it has no use for (same rule as `uas-worker`).
pub fn requires_sasl(security_protocol: &str) -> bool {
    matches!(
        security_protocol.trim().to_ascii_uppercase().as_str(),
        "SASL_PLAINTEXT" | "SASL_SSL"
    )
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl ServedCandidatesSinkConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let jsonl_path = non_empty(lookup(SERVED_EVENTS_JSONL_PATH_ENV));
        let brokers = non_empty(lookup(SERVED_EVENTS_KAFKA_BROKERS_ENV));
        let topic = non_empty(lookup(SERVED_EVENTS_KAFKA_TOPIC_ENV));

        if jsonl_path.is_some() && (brokers.is_some() || topic.is_some()) {
            anyhow::bail!(
                "configure either {SERVED_EVENTS_JSONL_PATH_ENV} or {SERVED_EVENTS_KAFKA_BROKERS_ENV}/{SERVED_EVENTS_KAFKA_TOPIC_ENV}, not both"
            );
        }
        if let Some(path) = jsonl_path {
            return Ok(Self::JsonLines(PathBuf::from(path)));
        }
        let (brokers, topic) = match (brokers, topic) {
            (None, None) => return Ok(Self::Disabled),
            (Some(brokers), Some(topic)) => (brokers, topic),
            _ => anyhow::bail!(
                "{SERVED_EVENTS_KAFKA_BROKERS_ENV} and {SERVED_EVENTS_KAFKA_TOPIC_ENV} must be configured together"
            ),
        };

        let security_protocol = non_empty(lookup(SERVED_EVENTS_KAFKA_SECURITY_PROTOCOL_ENV))
            .unwrap_or_else(|| "PLAINTEXT".to_string());
        let sasl = if requires_sasl(&security_protocol) {
            let username = non_empty(lookup(SERVED_EVENTS_KAFKA_SASL_USERNAME_ENV))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{SERVED_EVENTS_KAFKA_SASL_USERNAME_ENV} is required for {security_protocol}"
                    )
                })?;
            let password = non_empty(lookup(SERVED_EVENTS_KAFKA_SASL_PASSWORD_ENV))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{SERVED_EVENTS_KAFKA_SASL_PASSWORD_ENV} is required for {security_protocol}"
                    )
                })?;
            Some(SaslCredentials {
                mechanism: non_empty(lookup(SERVED_EVENTS_KAFKA_SASL_MECHANISM_ENV))
                    .unwrap_or_else(|| "PLAIN".to_string()),
                username,
                password,
            })
        } else {
            None
        };
        let delivery_timeout = match non_empty(lookup(SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV))
        {
            None => DEFAULT_DELIVERY_TIMEOUT,
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|millis| *millis > 0)
                .map(Duration::from_millis)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV} must be a positive integer"
                    )
                })?,
        };

        Ok(Self::Kafka(KafkaSinkConfig {
            brokers,
            topic,
            security_protocol,
            sasl,
            delivery_timeout,
        }))
    }

    /// Human-readable target for the startup log; never includes credentials.
    pub fn describe(&self) -> String {
        match self {
            Self::Disabled => "disabled".to_string(),
            Self::JsonLines(path) => format!("json-lines file {}", path.display()),
            Self::Kafka(config) => format!(
                "kafka topic {} via {} ({})",
                config.topic, config.brokers, config.security_protocol
            ),
        }
    }
}

/// Build the adapter selected by `config`. `Disabled` yields `None` so the
/// assembly leaves the side effect out entirely.
pub async fn build_served_candidates_sink(
    config: &ServedCandidatesSinkConfig,
) -> anyhow::Result<Option<Arc<dyn ServedCandidatesSink>>> {
    match config {
        ServedCandidatesSinkConfig::Disabled => Ok(None),
        ServedCandidatesSinkConfig::JsonLines(path) => Ok(Some(Arc::new(
            JsonLinesServedCandidatesSink::open(path).await?,
        ))),
        ServedCandidatesSinkConfig::Kafka(kafka) => build_kafka_sink(kafka),
    }
}

#[cfg(feature = "kafka")]
fn build_kafka_sink(
    config: &KafkaSinkConfig,
) -> anyhow::Result<Option<Arc<dyn ServedCandidatesSink>>> {
    Ok(Some(Arc::new(KafkaServedCandidatesSink::new(config)?)))
}

#[cfg(not(feature = "kafka"))]
fn build_kafka_sink(
    _config: &KafkaSinkConfig,
) -> anyhow::Result<Option<Arc<dyn ServedCandidatesSink>>> {
    anyhow::bail!(
        "{SERVED_EVENTS_KAFKA_BROKERS_ENV} is set but this home-mixer was built without the `kafka` feature; rebuild with `cargo build -p home-mixer --features kafka`"
    )
}

/// Appends one JSON object per request to a local file. Writes are serialized
/// through a mutex so concurrent requests never interleave partial lines.
pub struct JsonLinesServedCandidatesSink {
    path: PathBuf,
    file: tokio::sync::Mutex<tokio::fs::File>,
}

impl JsonLinesServedCandidatesSink {
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|error| {
                anyhow::anyhow!("cannot open {} for served events: {error}", path.display())
            })?;
        Ok(Self {
            path,
            file: tokio::sync::Mutex::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl ServedCandidatesSink for JsonLinesServedCandidatesSink {
    async fn publish(&self, event: &ServedCandidatesEvent) -> Result<(), String> {
        let mut line = serde_json::to_vec(event).map_err(|error| error.to_string())?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        file.write_all(&line)
            .await
            .map_err(|error| format!("write {}: {error}", self.path.display()))?;
        file.flush()
            .await
            .map_err(|error| format!("flush {}: {error}", self.path.display()))
    }
}

#[cfg(feature = "kafka")]
pub use kafka::KafkaServedCandidatesSink;

#[cfg(feature = "kafka")]
mod kafka {
    use super::*;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;

    pub struct KafkaServedCandidatesSink {
        producer: FutureProducer,
        topic: String,
        delivery_timeout: Duration,
    }

    impl KafkaServedCandidatesSink {
        pub fn new(config: &KafkaSinkConfig) -> anyhow::Result<Self> {
            let mut client = ClientConfig::new();
            client
                .set("bootstrap.servers", &config.brokers)
                .set("security.protocol", &config.security_protocol)
                // acks=all + idempotent producer: a broker-side retry cannot
                // duplicate or reorder one request's event.
                .set("enable.idempotence", "true")
                .set(
                    "message.timeout.ms",
                    config.delivery_timeout.as_millis().to_string(),
                )
                .set("compression.type", "zstd");
            if let Some(sasl) = &config.sasl {
                client
                    .set("sasl.mechanism", &sasl.mechanism)
                    .set("sasl.username", &sasl.username)
                    .set("sasl.password", &sasl.password);
            }
            let producer = client
                .create()
                .map_err(|error| anyhow::anyhow!("cannot create Kafka producer: {error}"))?;
            Ok(Self {
                producer,
                topic: config.topic.clone(),
                delivery_timeout: config.delivery_timeout,
            })
        }
    }

    #[async_trait]
    impl ServedCandidatesSink for KafkaServedCandidatesSink {
        async fn publish(&self, event: &ServedCandidatesEvent) -> Result<(), String> {
            let payload = serde_json::to_vec(event).map_err(|error| error.to_string())?;
            let record = FutureRecord::to(&self.topic)
                .key(event.viewer_id.as_str())
                .payload(&payload);
            self.producer
                .send(record, self.delivery_timeout)
                .await
                .map(|_| ())
                .map_err(|(error, _)| format!("Kafka send to {}: {error}", self.topic))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{pid, uid};
    use crate::side_effects::served_candidates_kafka_side_effect::ServedCandidateRecord;

    fn lookup_in(values: &[(&str, &str)], name: &str) -> Option<String> {
        values
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_string())
    }

    fn config(values: &[(&str, &str)]) -> anyhow::Result<ServedCandidatesSinkConfig> {
        ServedCandidatesSinkConfig::from_lookup(|name| lookup_in(values, name))
    }

    #[test]
    fn unconfigured_deployments_have_no_sink() {
        assert_eq!(config(&[]).unwrap(), ServedCandidatesSinkConfig::Disabled);
        assert_eq!(
            config(&[(SERVED_EVENTS_JSONL_PATH_ENV, "  ")]).unwrap(),
            ServedCandidatesSinkConfig::Disabled
        );
    }

    #[test]
    fn jsonl_and_kafka_are_exclusive_and_kafka_needs_both_variables() {
        assert_eq!(
            config(&[(SERVED_EVENTS_JSONL_PATH_ENV, " /tmp/served.jsonl ")]).unwrap(),
            ServedCandidatesSinkConfig::JsonLines(PathBuf::from("/tmp/served.jsonl"))
        );
        assert!(config(&[
            (SERVED_EVENTS_JSONL_PATH_ENV, "/tmp/served.jsonl"),
            (SERVED_EVENTS_KAFKA_BROKERS_ENV, "broker:9092"),
        ])
        .is_err());
        assert!(config(&[(SERVED_EVENTS_KAFKA_BROKERS_ENV, "broker:9092")]).is_err());
        assert!(config(&[(SERVED_EVENTS_KAFKA_TOPIC_ENV, "served")]).is_err());
    }

    #[test]
    fn kafka_settings_default_to_plaintext_and_require_sasl_credentials_when_needed() {
        let ServedCandidatesSinkConfig::Kafka(kafka) = config(&[
            (SERVED_EVENTS_KAFKA_BROKERS_ENV, "broker:9092"),
            (SERVED_EVENTS_KAFKA_TOPIC_ENV, "home-mixer.served"),
        ])
        .unwrap() else {
            panic!("brokers + topic select Kafka");
        };
        assert_eq!(kafka.security_protocol, "PLAINTEXT");
        assert_eq!(kafka.sasl, None);
        assert_eq!(kafka.delivery_timeout, DEFAULT_DELIVERY_TIMEOUT);

        let sasl_values = [
            (SERVED_EVENTS_KAFKA_BROKERS_ENV, "broker:9092"),
            (SERVED_EVENTS_KAFKA_TOPIC_ENV, "home-mixer.served"),
            (SERVED_EVENTS_KAFKA_SECURITY_PROTOCOL_ENV, "SASL_SSL"),
        ];
        assert!(config(&sasl_values).is_err(), "SASL needs credentials");

        let mut with_credentials = sasl_values.to_vec();
        with_credentials.push((SERVED_EVENTS_KAFKA_SASL_USERNAME_ENV, "svc"));
        with_credentials.push((SERVED_EVENTS_KAFKA_SASL_PASSWORD_ENV, "secret"));
        with_credentials.push((SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV, "1500"));
        let ServedCandidatesSinkConfig::Kafka(kafka) = config(&with_credentials).unwrap() else {
            panic!("SASL Kafka config");
        };
        let sasl = kafka.sasl.expect("credentials");
        assert_eq!(sasl.mechanism, "PLAIN");
        assert_eq!(sasl.username, "svc");
        assert_eq!(kafka.delivery_timeout, Duration::from_millis(1_500));
        assert!(!format!("{sasl:?}").contains("secret"), "debug must redact");

        for value in ["0", "-1", "soon"] {
            let mut values = with_credentials.clone();
            values.retain(|(key, _)| *key != SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV);
            values.push((SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS_ENV, value));
            assert!(config(&values).is_err(), "{value}");
        }
    }

    #[tokio::test]
    async fn jsonl_sink_appends_one_line_per_event() {
        let dir = std::env::temp_dir().join(format!(
            "home-mixer-served-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("served.jsonl");

        let sink = JsonLinesServedCandidatesSink::open(&path).await.unwrap();
        let event = |request_id: &str| ServedCandidatesEvent {
            schema_version: 1,
            request_id: request_id.to_string(),
            prediction_request_id: 1,
            viewer_id: uid(42).to_string(),
            request_time_ms: 1_700_000_000_000,
            is_shadow_traffic: false,
            in_network_only: false,
            is_bottom_request: false,
            client_app_id: 0,
            candidates: vec![ServedCandidateRecord {
                position: 0,
                post_id: pid(9).to_string(),
                author_id: uid(8).to_string(),
                retweeted_post_id: None,
                served_type: Some("FOR_YOU_IN_NETWORK".to_string()),
                in_network: Some(true),
                score: Some(0.5),
                weighted_score: None,
                degraded_reason: None,
                created_at_ms: None,
            }],
        };
        sink.publish(&event("req-1")).await.unwrap();
        sink.publish(&event("req-2")).await.unwrap();

        // Reopening appends instead of truncating what an earlier process wrote.
        let reopened = JsonLinesServedCandidatesSink::open(&path).await.unwrap();
        reopened.publish(&event("req-3")).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<ServedCandidatesEvent> = content
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is one event"))
            .collect();
        assert_eq!(
            lines
                .iter()
                .map(|e| e.request_id.as_str())
                .collect::<Vec<_>>(),
            ["req-1", "req-2", "req-3"]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(not(feature = "kafka"))]
    #[tokio::test]
    async fn kafka_without_the_feature_fails_at_startup_not_at_runtime() {
        let config = ServedCandidatesSinkConfig::Kafka(KafkaSinkConfig {
            brokers: "broker:9092".to_string(),
            topic: "served".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            sasl: None,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
        });
        let error = build_served_candidates_sink(&config)
            .await
            .err()
            .expect("kafka needs the feature");
        assert!(error.to_string().contains("--features kafka"));
    }

    #[test]
    fn only_sasl_protocols_require_credentials() {
        assert!(requires_sasl("SASL_SSL"));
        assert!(requires_sasl("sasl_plaintext"));
        assert!(!requires_sasl("SSL"));
        assert!(!requires_sasl("PLAINTEXT"));
    }
}
