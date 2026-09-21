use clap::Parser;
use id_service::{
    grpc::{GrpcIdRegistryService, IdentityRegistryServiceServer},
    http, logging, RedisIdRegistry, RedisIdRegistryConfig, MAPPING_VERSION,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

const MAX_GRPC_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(about = "ObjectId ↔ Snowflake identity service")]
struct Args {
    #[arg(
        long,
        env = "ID_REGISTRY_GRPC_LISTEN",
        default_value = "127.0.0.1:50072"
    )]
    grpc_listen: SocketAddr,
    #[arg(
        long,
        env = "ID_REGISTRY_HTTP_LISTEN",
        default_value = "127.0.0.1:50070"
    )]
    http_listen: SocketAddr,
    /// Legacy HTTP listener flag retained for clients upgrading from the
    /// original single-port service. It takes precedence over `--http-listen`.
    #[arg(long = "listen", env = "ID_REGISTRY_LISTEN", hide = true)]
    legacy_listen: Option<SocketAddr>,
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
    let http_listen = args.legacy_listen.unwrap_or(args.http_listen);
    logging::init_from_env()?;
    if args.legacy_listen.is_some() {
        log::warn!(
            "--listen/ID_REGISTRY_LISTEN is deprecated; use --http-listen/ID_REGISTRY_HTTP_LISTEN"
        );
    }
    anyhow::ensure!(
        args.cache_capacity > 0,
        "ID registry cache capacity must be positive"
    );
    anyhow::ensure!(
        args.max_batch_size > 0,
        "ID registry max batch size must be positive"
    );
    anyhow::ensure!(
        args.grpc_listen != http_listen,
        "ID registry gRPC and HTTP listeners must use different addresses"
    );
    log::info!(
        "id-service starting: grpc_listen={} http_listen={} worker_id={} allow_allocation={} allow_trusted_import={} \
         max_batch_size={} key_prefix={} cache_capacity={} redis_connect_timeout_ms={} \
         redis_request_timeout_ms={} mapping_version={MAPPING_VERSION} redis={}",
        args.grpc_listen,
        http_listen,
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

    let registry = Arc::new(registry);
    let http_listener = tokio::net::TcpListener::bind(http_listen).await?;
    let http_addr = http_listener.local_addr()?;
    let grpc_addr = args.grpc_listen;
    let grpc_service = GrpcIdRegistryService::new(Arc::clone(&registry), args.max_batch_size);
    let http_app = http::router(registry, args.max_batch_size);
    log::info!("id-service gRPC ready on {grpc_addr}; HTTP compatibility ready on {http_addr}");

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let signal_tx = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal_tx.send(());
    });
    let mut grpc_shutdown = shutdown_tx.subscribe();
    let mut http_shutdown = shutdown_tx.subscribe();
    let grpc = async move {
        tonic::transport::Server::builder()
            .add_service(
                IdentityRegistryServiceServer::new(grpc_service)
                    .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                    .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
            )
            .serve_with_shutdown(grpc_addr, async move {
                let _ = grpc_shutdown.recv().await;
            })
            .await
            .map_err(|error| anyhow::anyhow!("gRPC server failed: {error}"))
    };
    let http = async move {
        axum::serve(http_listener, http_app)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown.recv().await;
            })
            .await
            .map_err(|error| anyhow::anyhow!("HTTP compatibility server failed: {error}"))
    };
    let servers = tokio::try_join!(grpc, http);
    signal_task.abort();
    servers?;
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
