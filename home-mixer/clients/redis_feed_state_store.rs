//! Async Redis adapter for served-history and request-timestamp state.

use std::fmt;
use std::time::{Duration, Instant};

use crate::clients::redis_conn::{
    parse_cluster_urls, redis_error, validate_redis_url, ManagedRedisConnection,
};
use crate::feed_state::{FeedStateSnapshot, FeedStateStore};
use crate::id::{EntityKind, SnowflakeId};
use crate::metrics::ClientCallRecorder;
use crate::models::{ObjectId, PostId, UserId};

const DEFAULT_KEY_PREFIX: &str = "home_mixer:feed_state";
const DEFAULT_TTL_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Clone, Eq, PartialEq)]
pub struct RedisFeedStateConfig {
    /// Single-endpoint URL; ignored when `cluster_urls` is set.
    pub url: String,
    /// Native-cluster seed URLs (`*_REDIS_CLUSTER_URLS`). `Some` switches the
    /// store to cluster routing; keys are hash-tagged per user, so every
    /// pipeline stays within one slot.
    pub cluster_urls: Option<Vec<String>>,
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
            .field(
                "cluster_urls",
                &self
                    .cluster_urls
                    .as_ref()
                    .map(|urls| format!("<{} redacted>", urls.len())),
            )
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
            cluster_urls: None,
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            max_served_ids: crate::params::LOCAL_SERVED_HISTORY_LIMIT,
            max_request_timestamps: crate::params::LOCAL_REQUEST_TIMESTAMP_LIMIT,
            ttl_secs: Some(DEFAULT_TTL_SECS),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(500),
        }
    }

    /// Set the cluster seed list from a comma-separated value; blank or
    /// unset values keep the single-endpoint shape.
    pub fn with_cluster_urls(mut self, value: &str) -> Self {
        self.cluster_urls = parse_cluster_urls(value);
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(urls) = &self.cluster_urls {
            if urls.is_empty() {
                return Err("Redis cluster URL list must not be empty".to_string());
            }
            for url in urls {
                validate_redis_url(url)?;
            }
        } else {
            if self.url.trim().is_empty() {
                return Err("Redis URL must not be empty".to_string());
            }
            validate_redis_url(&self.url)?;
        }
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
    connection: ManagedRedisConnection,
    identity: crate::id::SharedIdentityReader,
    key_prefix: String,
    max_served_ids: usize,
    max_request_timestamps: usize,
    ttl_secs: Option<u64>,
    request_timeout: Duration,
    calls: ClientCallRecorder,
}

impl RedisFeedStateStore {
    /// Test/compatibility constructor: resolves zero-padded ObjectIds without
    /// external services. Production must use [`Self::new_with_identity`].
    pub async fn new(config: RedisFeedStateConfig) -> Result<Self, String> {
        Self::new_with_identity(
            config,
            std::sync::Arc::new(crate::id::PaddedIdentityResolver::new()),
        )
        .await
    }

    pub async fn new_with_identity(
        config: RedisFeedStateConfig,
        identity: crate::id::SharedIdentityReader,
    ) -> Result<Self, String> {
        config.validate()?;

        let connection = ManagedRedisConnection::connect(
            Some(config.url.as_str()),
            config.cluster_urls.as_deref(),
            config.connect_timeout,
            config.request_timeout,
        )
        .await?;

        Ok(Self {
            connection,
            identity,
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

    /// Redis keys keep the external ObjectId form; `user_id` is the reversed
    /// 24-hex string, not the internal Snowflake.
    fn key(&self, user_id: &ObjectId, suffix: &str) -> String {
        // The hash tag keeps a user's pair collocated under cluster routing
        // and cluster-aware proxies alike.
        format!("{}:{{{user_id}}}:{suffix}", self.key_prefix)
    }

    /// Reverse the internal viewer Snowflake to the ObjectId the Redis key
    /// space is written in.
    async fn external_user(&self, user_id: UserId) -> Result<ObjectId, String> {
        let snowflake = SnowflakeId::new(user_id)
            .map_err(|error| format!("invalid internal user id: {error}"))?;
        let object_id = self
            .identity
            .reverse_batch(&[(snowflake, EntityKind::User)])
            .await
            .map_err(|error| format!("reverse feed-state user id: {error}"))?
            .into_iter()
            .next()
            .ok_or_else(|| "ID Registry returned no user mapping".to_string())?;
        ObjectId::parse(&object_id).map_err(|error| format!("ID Registry returned {error}"))
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
        let external_user = self.external_user(user_id).await?;
        let served_key = self.key(&external_user, "served");
        let timestamps_key = self.key(&external_user, "timestamps");
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

        let (served, request_timestamps_ms): (Vec<String>, Vec<i64>) = tokio::time::timeout(
            self.request_timeout,
            self.connection.query_idempotent(&pipeline),
        )
        .await
        .map_err(|_| "Redis feed-state load timed out".to_string())?
        .map_err(|error| redis_error("Redis feed-state load", &error))?;

        // Members are stored as external ObjectIds; resolve them to the
        // internal Snowflake IDs the domain model carries.
        let external_ids = served
            .into_iter()
            .map(|value| {
                ObjectId::parse(&value)
                    .map(|_| (value, EntityKind::Post))
                    .map_err(|error| format!("invalid served post id in Redis: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let served_post_ids = self
            .identity
            .resolve_batch(&external_ids)
            .await
            .map_err(|error| format!("resolve feed-state served ids: {error}"))?
            .into_iter()
            .map(|id| id.get())
            .collect();
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
        let external_user = self.external_user(user_id).await?;
        // Members are stored as external ObjectIds; reverse the internal
        // Snowflake IDs before writing.
        let internal_ids = served_post_ids
            .iter()
            .map(|id| {
                SnowflakeId::new(*id)
                    .map(|id| (id, EntityKind::Post))
                    .map_err(|error| format!("invalid served post id: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let external_post_ids = self
            .identity
            .reverse_batch(&internal_ids)
            .await
            .map_err(|error| format!("reverse feed-state served ids: {error}"))?;
        let served_key = self.key(&external_user, "served");
        let timestamps_key = self.key(&external_user, "timestamps");
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        for post_id in external_post_ids {
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

        tokio::time::timeout(
            self.request_timeout,
            self.connection.query_once::<()>(&pipeline),
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
