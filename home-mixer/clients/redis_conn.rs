//! Shared Redis endpoint parsing and connection management for the
//! feed-state and UAS stores.
//!
//! Two deployment shapes are supported:
//!
//! - **Single endpoint** (`*_REDIS_URL`): a standalone Redis or a proxy that
//!   presents one endpoint (Twemproxy, Envoy, a cloud proxy). All traffic
//!   goes to that endpoint; a [`ConnectionManager`] reconnects in the
//!   background when the connection drops.
//! - **Native cluster** (`*_REDIS_CLUSTER_URLS`, comma-separated seed URLs):
//!   routing follows `CLUSTER SLOTS`. Both stores hash-tag every key of one
//!   user (`{user_id}`), so each pipeline and `MULTI` touches a single slot
//!   and stays valid under cluster routing.
//!
//! The shape is fixed at startup: mixing or failing over between the two is
//! an operator concern (restart the process), not a client concern.

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::time::Duration;

/// Parse a comma-separated cluster seed list; blank entries are dropped.
/// `None` means the variable was unset or contained no usable URL, which
/// keeps the single-endpoint shape.
pub fn parse_cluster_urls(value: &str) -> Option<Vec<String>> {
    let urls = value
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    (!urls.is_empty()).then_some(urls)
}

/// Validate that `url` is a syntactically usable Redis connection URL.
pub fn validate_redis_url(url: &str) -> Result<(), String> {
    redis::Client::open(url)
        .map(|_| ())
        .map_err(|_| "invalid Redis URL".to_string())
}

/// A managed connection to either deployment shape. Cheap to clone: the
/// single-endpoint manager and the cluster connection both multiplex one
/// transport per node.
#[derive(Clone)]
pub enum ManagedRedisConnection {
    Single(Box<ConnectionManager>),
    Cluster(Box<redis::cluster_async::ClusterConnection>),
}

impl ManagedRedisConnection {
    /// Connect to `single` or the `cluster` seed list and run one `PING`
    /// health check, bounded by `connect_timeout`. `request_timeout` bounds
    /// the manager's per-command response timeout (single shape).
    pub async fn connect(
        single: Option<&str>,
        cluster: Option<&[String]>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        let connection = match (cluster, single) {
            (Some(urls), _) => {
                let client = redis::cluster::ClusterClient::new(urls.to_vec())
                    .map_err(|_| "invalid Redis cluster URLs".to_string())?;
                let connection =
                    tokio::time::timeout(connect_timeout, client.get_async_connection())
                        .await
                        .map_err(|_| "Redis cluster connection timed out".to_string())?
                        .map_err(|error| redis_error("Redis cluster connection", &error))?;
                Self::Cluster(Box::new(connection))
            }
            (None, Some(url)) => {
                let client =
                    redis::Client::open(url).map_err(|_| "invalid Redis URL".to_string())?;
                let manager_config = ConnectionManagerConfig::new()
                    // One bounded connection attempt per reconnect. Later
                    // commands can trigger a fresh attempt, but failed
                    // commands are never replayed.
                    .set_number_of_retries(0)
                    .set_connection_timeout(connect_timeout)
                    .set_response_timeout(request_timeout);
                let connection = tokio::time::timeout(
                    connect_timeout,
                    ConnectionManager::new_with_config(client, manager_config),
                )
                .await
                .map_err(|_| "Redis connection timed out".to_string())?
                .map_err(|error| redis_error("Redis connection", &error))?;
                Self::Single(Box::new(connection))
            }
            (None, None) => return Err("no Redis endpoint configured".to_string()),
        };

        let mut health_connection = connection.clone();
        let pong = tokio::time::timeout(request_timeout, async {
            match &mut health_connection {
                Self::Single(connection) => {
                    redis::cmd("PING")
                        .query_async::<String>(&mut **connection)
                        .await
                }
                Self::Cluster(connection) => {
                    redis::cmd("PING")
                        .query_async::<String>(&mut **connection)
                        .await
                }
            }
        })
        .await
        .map_err(|_| "Redis health check timed out".to_string())?
        .map_err(|error| redis_error("Redis health check", &error))?;
        if pong != "PONG" {
            return Err(format!("Redis health check returned {pong}"));
        }
        Ok(connection)
    }

    /// Run an idempotent pipeline, retrying once if the first attempt failed
    /// because a replaced connection explains the failure. The caller bounds
    /// both attempts with one timeout; a timeout is not retried because the
    /// budget is spent.
    pub async fn query_idempotent<T: redis::FromRedisValue>(
        &self,
        pipeline: &redis::Pipeline,
    ) -> redis::RedisResult<T> {
        let mut connection = self.clone();
        match pipeline.query_async(&mut connection).await {
            Err(error) if connection_replaced(&error) => {
                pipeline.query_async(&mut connection).await
            }
            result => result,
        }
    }

    /// Run a pipeline exactly once. Writes are never replayed: a write that
    /// timed out may already have been executed.
    pub async fn query_once<T: redis::FromRedisValue>(
        &self,
        pipeline: &redis::Pipeline,
    ) -> redis::RedisResult<T> {
        let mut connection = self.clone();
        pipeline.query_async(&mut connection).await
    }
}

/// `ConnectionManager` swaps in a new connection after an I/O failure or an
/// unrecoverable protocol error, so the next command already targets the
/// replacement; the cluster client applies the same reasoning per node. A
/// timeout keeps the current connection and is not retried.
fn connection_replaced(error: &redis::RedisError) -> bool {
    !error.is_timeout() && (error.is_io_error() || error.is_unrecoverable_error())
}

impl redis::aio::ConnectionLike for ManagedRedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Single(connection) => connection.req_packed_command(cmd),
            Self::Cluster(connection) => connection.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Single(connection) => connection.req_packed_commands(cmd, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Single(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

/// Redis errors may carry connection details. Reporting only the kind keeps
/// credentials from a configured URL out of logs and API responses.
pub(crate) fn redis_error(operation: &str, error: &redis::RedisError) -> String {
    format!("{operation} failed ({:?})", error.kind())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_lists_drop_blank_entries_and_reject_all_blank_values() {
        assert_eq!(
            parse_cluster_urls("redis://127.0.0.1:7000, redis://127.0.0.1:7001 ,,"),
            Some(vec![
                "redis://127.0.0.1:7000".to_string(),
                "redis://127.0.0.1:7001".to_string(),
            ])
        );
        assert_eq!(parse_cluster_urls(""), None);
        assert_eq!(parse_cluster_urls(" , "), None);
    }

    #[test]
    fn url_validation_distinguishes_syntax_from_reachability() {
        assert_eq!(validate_redis_url("redis://localhost/"), Ok(()));
        assert!(validate_redis_url("invalid://localhost/").is_err());
    }
}
