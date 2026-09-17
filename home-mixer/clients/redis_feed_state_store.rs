//! Async Redis adapter for served-history and request-timestamp state.

use std::fmt;
use std::time::{Duration, Instant};

use redis::aio::{ConnectionManager, ConnectionManagerConfig};

use crate::feed_state::{FeedStateSnapshot, FeedStateStore};
use crate::metrics::ClientCallRecorder;
use crate::models::{PostId, UserId};

const DEFAULT_KEY_PREFIX: &str = "home_mixer:feed_state";
const DEFAULT_TTL_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Clone, Eq, PartialEq)]
pub struct RedisFeedStateConfig {
    pub url: String,
    pub key_prefix: String,
    pub max_served_ids: usize,
    pub max_request_timestamps: usize,
    pub ttl_secs: Option<u64>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl fmt::Debug for RedisFeedStateConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisFeedStateConfig")
            .field("url", &"<redacted>")
            .field("key_prefix", &self.key_prefix)
            .field("max_served_ids", &self.max_served_ids)
            .field("max_request_timestamps", &self.max_request_timestamps)
            .field("ttl_secs", &self.ttl_secs)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl RedisFeedStateConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            max_served_ids: crate::params::LOCAL_SERVED_HISTORY_LIMIT,
            max_request_timestamps: crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT,
            ttl_secs: Some(DEFAULT_TTL_SECS),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(500),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("Redis URL must not be empty".to_string());
        }
        redis::Client::open(self.url.as_str()).map_err(|_| "invalid Redis URL".to_string())?;
        if self.key_prefix.trim().is_empty() {
            return Err("Redis feed-state key prefix must not be empty".to_string());
        }
        if self.key_prefix.contains(['{', '}']) {
            return Err("Redis feed-state key prefix must not contain braces".to_string());
        }
        if self.max_served_ids > i64::MAX as usize {
            return Err("Redis served-id limit exceeds the supported range".to_string());
        }
        if self.max_request_timestamps > i64::MAX as usize {
            return Err("Redis request-timestamp limit exceeds the supported range".to_string());
        }
        if self.ttl_secs == Some(0) {
            return Err("Redis feed-state TTL must be greater than zero".to_string());
        }
        if self
            .ttl_secs
            .is_some_and(|ttl_secs| ttl_secs > i64::MAX as u64)
        {
            return Err("Redis feed-state TTL exceeds the supported range".to_string());
        }
        if self.connect_timeout.is_zero() {
            return Err("Redis connect timeout must be greater than zero".to_string());
        }
        if self.request_timeout.is_zero() {
            return Err("Redis request timeout must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Redis-backed feed state shared by all Home Mixer instances.
///
/// The connection multiplexes one transport per endpoint (single endpoint or
/// cluster seed; see [`ManagedRedisConnection`]). When a command finds that
/// connection dead (server restart, failover, idle close by a proxy), the
/// manager replaces it in the background and returns the failure to the
/// caller. Reads are idempotent, so `load` runs once more on the replacement
/// instead of failing open and letting the request serve posts it cannot see.
/// Writes are never retried or replayed: a write that timed out may already
/// have been executed.
pub struct RedisFeedStateStore {
    connection: ConnectionManager,
    key_prefix: String,
    max_served_ids: usize,
    max_request_timestamps: usize,
    ttl_secs: Option<u64>,
    request_timeout: Duration,
    calls: ClientCallRecorder,
}

impl RedisFeedStateStore {
    pub async fn new(config: RedisFeedStateConfig) -> Result<Self, String> {
        config.validate()?;

        let client = redis::Client::open(config.url.as_str())
            .map_err(|_| "invalid Redis URL".to_string())?;
        let manager_config = ConnectionManagerConfig::new()
            // One bounded connection attempt per reconnect. Later commands can
            // trigger a fresh attempt, but failed commands are never replayed.
            .set_number_of_retries(0)
            .set_connection_timeout(config.connect_timeout)
            .set_response_timeout(config.request_timeout);
        let connection = tokio::time::timeout(
            config.connect_timeout,
            ConnectionManager::new_with_config(client, manager_config),
        )
        .await
        .map_err(|_| "Redis connection timed out".to_string())?
        .map_err(|error| redis_error("Redis connection", &error))?;

        let mut health_connection = connection.clone();
        let pong = tokio::time::timeout(
            config.request_timeout,
            redis::cmd("PING").query_async::<String>(&mut health_connection),
        )
        .await
        .map_err(|_| "Redis health check timed out".to_string())?
        .map_err(|error| redis_error("Redis health check", &error))?;
        if pong != "PONG" {
            return Err("Redis health check returned an unexpected response".to_string());
        }

        Ok(Self {
            connection,
            key_prefix: config.key_prefix,
            max_served_ids: config.max_served_ids,
            max_request_timestamps: config.max_request_timestamps,
            ttl_secs: config.ttl_secs,
            request_timeout: config.request_timeout,
            calls: ClientCallRecorder::default(),
        })
    }

    /// Attach the process call metrics; the default records nothing.
    pub fn with_calls(mut self, calls: ClientCallRecorder) -> Self {
        self.calls = calls;
        self
    }

    fn key(&self, user_id: UserId, suffix: &str) -> String {
        // The hash tag keeps a user's pair collocated when the configured
        // Redis endpoint or proxy honors hash tags.
        format!("{}:{{{user_id}}}:{suffix}", self.key_prefix)
    }

    /// Run an idempotent read, retrying once if the first attempt failed
    /// because the connection manager discarded its connection. The caller
    /// bounds both attempts with one `request_timeout`.
    async fn query_read<T: redis::FromRedisValue>(
        &self,
        pipeline: &redis::Pipeline,
    ) -> redis::RedisResult<T> {
        let mut connection = self.connection.clone();
        match pipeline.query_async(&mut connection).await {
            Err(error) if connection_replaced(&error) => {
                pipeline.query_async(&mut connection).await
            }
            result => result,
        }
    }
}

#[tonic::async_trait]
impl FeedStateStore for RedisFeedStateStore {
    async fn load(&self, user_id: UserId) -> Result<FeedStateSnapshot, String> {
        let started = Instant::now();
        let result = self.load_inner(user_id).await;
        self.calls.record(
            "redis_feed_state",
            "load",
            if result.is_ok() { "ok" } else { "error" },
            started,
        );
        result
    }

    async fn record(
        &self,
        user_id: UserId,
        served_post_ids: Vec<PostId>,
        request_timestamp_ms: i64,
    ) -> Result<(), String> {
        let started = Instant::now();
        let result = self
            .record_inner(user_id, served_post_ids, request_timestamp_ms)
            .await;
        self.calls.record(
            "redis_feed_state",
            "record",
            if result.is_ok() { "ok" } else { "error" },
            started,
        );
        result
    }
}

impl RedisFeedStateStore {
    async fn load_inner(&self, user_id: UserId) -> Result<FeedStateSnapshot, String> {
        let served_key = self.key(user_id, "served");
        let timestamps_key = self.key(user_id, "timestamps");
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("LRANGE")
            .arg(served_key)
            .arg(0)
            .arg(-1)
            .cmd("LRANGE")
            .arg(timestamps_key)
            .arg(0)
            .arg(-1);

        let (served, request_timestamps_ms): (Vec<String>, Vec<i64>) =
            tokio::time::timeout(self.request_timeout, self.query_read(&pipeline))
                .await
                .map_err(|_| "Redis feed-state load timed out".to_string())?
                .map_err(|error| redis_error("Redis feed-state load", &error))?;

        let served_post_ids = served
            .into_iter()
            .map(|value| {
                PostId::parse(&value)
                    .map_err(|error| format!("invalid served post id in Redis: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FeedStateSnapshot {
            served_post_ids,
            request_timestamps_ms,
        })
    }

    async fn record_inner(
        &self,
        user_id: UserId,
        served_post_ids: Vec<PostId>,
        request_timestamp_ms: i64,
    ) -> Result<(), String> {
        let served_key = self.key(user_id, "served");
        let timestamps_key = self.key(user_id, "timestamps");
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        for post_id in served_post_ids {
            let post_id = post_id.to_string();
            pipeline
                .cmd("LREM")
                .arg(&served_key)
                .arg(0)
                .arg(&post_id)
                .cmd("RPUSH")
                .arg(&served_key)
                .arg(post_id);
        }
        trim_list(&mut pipeline, &served_key, self.max_served_ids);
        pipeline
            .cmd("RPUSH")
            .arg(&timestamps_key)
            .arg(request_timestamp_ms);
        trim_list(&mut pipeline, &timestamps_key, self.max_request_timestamps);
        set_retention(&mut pipeline, &served_key, &timestamps_key, self.ttl_secs);

        let mut connection = self.connection.clone();
        tokio::time::timeout(
            self.request_timeout,
            pipeline.query_async::<()>(&mut connection),
        )
        .await
        .map_err(|_| "Redis feed-state record timed out; write outcome is unknown".to_string())?
        .map_err(|error| redis_error("Redis feed-state record", &error))
    }
}

fn trim_list(pipeline: &mut redis::Pipeline, key: &str, limit: usize) {
    if limit == 0 {
        pipeline.cmd("DEL").arg(key);
    } else {
        pipeline.cmd("LTRIM").arg(key).arg(-(limit as i64)).arg(-1);
    }
}

fn set_retention(
    pipeline: &mut redis::Pipeline,
    served_key: &str,
    timestamps_key: &str,
    ttl_secs: Option<u64>,
) {
    let command = if ttl_secs.is_some() {
        "EXPIRE"
    } else {
        "PERSIST"
    };
    pipeline.cmd(command).arg(served_key);
    if let Some(ttl_secs) = ttl_secs {
        pipeline.arg(ttl_secs);
    }
    pipeline.cmd(command).arg(timestamps_key);
    if let Some(ttl_secs) = ttl_secs {
        pipeline.arg(ttl_secs);
    }
}

/// `ConnectionManager` swaps in a new connection after an I/O failure or an
/// unrecoverable protocol error, so the next command already targets the
/// replacement. A timeout keeps the current connection and is not retried:
/// the request budget is spent.
fn connection_replaced(error: &redis::RedisError) -> bool {
    !error.is_timeout() && (error.is_io_error() || error.is_unrecoverable_error())
}

fn redis_error(operation: &str, error: &redis::RedisError) -> String {
    // Redis errors may carry connection details. Reporting only the kind keeps
    // credentials from a configured URL out of logs and API responses.
    format!("{operation} failed ({:?})", error.kind())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_bounded() {
        let config = RedisFeedStateConfig::new("redis://localhost/");

        assert_eq!(config.key_prefix, DEFAULT_KEY_PREFIX);
        assert_eq!(
            config.max_served_ids,
            crate::params::LOCAL_SERVED_HISTORY_LIMIT
        );
        assert_eq!(
            config.max_request_timestamps,
            crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT
        );
        assert_eq!(config.ttl_secs, Some(604_800));
        assert_eq!(config.connect_timeout, Duration::from_secs(1));
        assert_eq!(config.request_timeout, Duration::from_millis(500));
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn config_accepts_zero_limits_and_no_ttl() {
        let mut config = RedisFeedStateConfig::new("redis://localhost/");
        config.max_served_ids = 0;
        config.max_request_timestamps = 0;
        config.ttl_secs = None;

        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn config_rejects_invalid_values_without_echoing_url() {
        let secret_url = "redis://secret-user:secret-password@localhost/";
        let mut config = RedisFeedStateConfig::new(secret_url);
        config.request_timeout = Duration::ZERO;

        let error = config.validate().expect_err("zero timeout must fail");
        assert!(!error.contains("secret-user"));
        assert!(!error.contains("secret-password"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-password"));
    }

    #[test]
    fn config_rejects_invalid_url_and_out_of_range_ttl() {
        let mut config = RedisFeedStateConfig::new("invalid://localhost/");
        assert_eq!(config.validate(), Err("invalid Redis URL".to_string()));

        config.url = "redis://localhost/".to_string();
        config.ttl_secs = Some(i64::MAX as u64 + 1);
        assert_eq!(
            config.validate(),
            Err("Redis feed-state TTL exceeds the supported range".to_string())
        );
    }
}
