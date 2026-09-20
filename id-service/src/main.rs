use clap::Parser;
use id_service::{http, logging, RedisIdRegistry, RedisIdRegistryConfig, MAPPING_VERSION};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(about = "ObjectId ↔ Snowflake identity service")]
struct Args {
    #[arg(long, env = "ID_REGISTRY_LISTEN", default_value = "127.0.0.1:50070")]
    listen: SocketAddr,
    #[arg(long, env = "ID_REGISTRY_REDIS_URL")]
    redis_url: Option<String>,
    #[arg(long, env = "ID_REGISTRY_REDIS_CLUSTER_URLS", value_delimiter = ',')]
    redis_cluster_urls: Option<Vec<String>>,
    #[arg(
        long,
        env = "ID_REGISTRY_REDIS_KEY_PREFIX",
        default_value = "id-registry:v2"
    )]
    redis_key_prefix: String,
    #[arg(long, env = "ID_REGISTRY_CACHE_CAPACITY", default_value_t = 100_000)]
    cache_capacity: usize,
    #[arg(
        long,
        env = "ID_REGISTRY_REDIS_CONNECT_TIMEOUT_MS",
        default_value_t = 2_000
    )]
    redis_connect_timeout_ms: u64,
    #[arg(
        long,
        env = "ID_REGISTRY_REDIS_REQUEST_TIMEOUT_MS",
        default_value_t = 500
    )]
    redis_request_timeout_ms: u64,
    /// Snowflake worker id (0..=1023) embedded in allocated ids. Sequence
    /// counters live in Redis, so replicas may share a worker id; distinct
    /// ids only make allocations attributable to a replica.
    #[arg(long, env = "ID_REGISTRY_WORKER_ID", default_value_t = 0)]
    worker_id: u64,
    /// Allow first-seen ObjectIDs to receive newly allocated Snowflakes.
    /// Keep disabled when the registry must preserve IDs from main, Thunder,
    /// or published Phoenix indexes; import trusted mappings instead.
    #[arg(long, env = "ID_REGISTRY_ALLOW_ALLOCATION", default_value_t = false)]
    allow_allocation: bool,
    /// Accept `trusted_snowflake_id` in resolve requests. Only migration
    /// tooling should talk to a replica with this enabled; online replicas
    /// reject such requests with 403.
    #[arg(
        long,
        env = "ID_REGISTRY_ALLOW_TRUSTED_IMPORT",
        default_value_t = false
    )]
    allow_trusted_import: bool,
    /// Maximum number of ids in one batch request; larger batches get 413.
    #[arg(long, env = "ID_REGISTRY_MAX_BATCH_SIZE", default_value_t = 10_000)]
    max_batch_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    logging::init_from_env()?;
    anyhow::ensure!(
        args.cache_capacity > 0,
        "ID registry cache capacity must be positive"
    );
    anyhow::ensure!(
        args.max_batch_size > 0,
        "ID registry max batch size must be positive"
    );
    log::info!(
        "id-service starting: listen={} worker_id={} allow_allocation={} allow_trusted_import={} \
         max_batch_size={} key_prefix={} cache_capacity={} redis_connect_timeout_ms={} \
         redis_request_timeout_ms={} mapping_version={MAPPING_VERSION} redis={}",
        args.listen,
        args.worker_id,
        args.allow_allocation,
        args.allow_trusted_import,
        args.max_batch_size,
        args.redis_key_prefix,
        args.cache_capacity,
        args.redis_connect_timeout_ms,
        args.redis_request_timeout_ms,
        redis_endpoint_summary(&args),
    );

    let registry = RedisIdRegistry::connect(RedisIdRegistryConfig {
        single_url: args.redis_url,
        cluster_urls: args.redis_cluster_urls,
        key_prefix: args.redis_key_prefix,
        cache_capacity: args.cache_capacity,
        worker_id: args.worker_id,
        allow_allocation: args.allow_allocation,
        allow_trusted_import: args.allow_trusted_import,
        connect_timeout: Duration::from_millis(args.redis_connect_timeout_ms),
        request_timeout: Duration::from_millis(args.redis_request_timeout_ms),
    })
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let app = http::router(Arc::new(registry), args.max_batch_size);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    log::info!("id-service ready on {}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    log::info!("id-service stopped");
    Ok(())
}

/// Redis endpoints for the startup log, with credentials redacted.
fn redis_endpoint_summary(args: &Args) -> String {
    match (&args.redis_url, &args.redis_cluster_urls) {
        (Some(url), _) => logging::redact_url(url),
        (None, Some(urls)) => format!(
            "cluster[{}]",
            urls.iter()
                .map(|url| logging::redact_url(url))
                .collect::<Vec<_>>()
                .join(",")
        ),
        (None, None) => "unset".to_string(),
    }
}

/// Resolves on SIGTERM (orchestrator stop) or Ctrl-C so in-flight requests
/// drain instead of being cut off.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            log::warn!("failed to listen for Ctrl-C: {error}");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    log::warn!("failed to listen for SIGTERM: {error}");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            _ = ctrl_c => log::info!("received Ctrl-C, shutting down"),
            _ = terminate => log::info!("received SIGTERM, shutting down"),
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
        log::info!("received Ctrl-C, shutting down");
    }
}
