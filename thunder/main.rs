use anyhow::Result;
use axum::Router;
use clap::Parser;
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use thunder::{
    args, kafka_utils, posts::post_store::PostStore, strato_client::StratoClient,
    thunder_service::ThunderServiceImpl,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = args::Args::parse();

    // Initialize PostStore
    let post_store = Arc::new(PostStore::new(
        args.post_retention_seconds,
        args.request_timeout_ms,
    ));
    info!(
        "Initialized PostStore for in-memory post storage (retention: {} seconds / {:.1} days, request_timeout: {}ms)",
        args.post_retention_seconds,
        args.post_retention_seconds as f64 / 86400.0,
        args.request_timeout_ms
    );

    // Initialize StratoClient for fetching following lists
    let strato_client = Arc::new(StratoClient::new());
    info!("Initialized StratoClient");

    // Create ThunderService with the PostStore, StratoClient, and concurrency limit
    let thunder_service = ThunderServiceImpl::new(
        Arc::clone(&post_store),
        Arc::clone(&strato_client),
        args.max_concurrent_requests,
    );
    info!(
        "Initialized with max_concurrent_requests={}",
        args.max_concurrent_requests
    );

    // Build gRPC routes
    let grpc_routes = Routes::new(thunder_service.server());

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Build combined HTTP + gRPC server
    let http_router = Router::new();
    let combined = http_router.into_make_service();

    // Start gRPC server
    let grpc_addr: SocketAddr = ([0, 0, 0, 0], args.grpc_port).into();
    let _grpc_handle = tokio::spawn(async move {
        info!("gRPC server listening on {}", grpc_addr);
        tonic::transport::Server::builder()
            .add_routes(grpc_routes)
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

    // Start HTTP server (health check / metrics)
    let http_addr: SocketAddr = ([0, 0, 0, 0], args.http_port).into();
    let _http_handle = tokio::spawn(async move {
        info!("HTTP server listening on {}", http_addr);
        let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
        axum::serve(listener, combined).await.unwrap();
    });

    if args.demo_seed_posts > 0 {
        // 演示模式：不消费 Kafka，直接生成模拟帖子灌入内存
        let posts = thunder::demo_seed::generate_demo_posts(args.demo_seed_posts);
        let count = posts.len();
        post_store.insert_posts(posts);
        post_store.finalize_init().await?;
        info!(
            "Demo mode: seeded {} posts from authors {:?} (Kafka disabled)",
            count,
            x_algorithm_proto::demo::DEMO_AUTHOR_IDS
        );

        Arc::clone(&post_store).start_stats_logger();
        Arc::clone(&post_store).start_auto_trim(2);
    } else {
        // Create channel for post events
        let (tx, mut rx) = tokio::sync::mpsc::channel::<i64>(args.kafka_num_threads);
        kafka_utils::start_kafka(&args, post_store.clone(), "", tx).await?;

        if args.is_serving {
            // Wait for Kafka catchup signal
            let start = Instant::now();
            for _ in 0..args.kafka_num_threads {
                rx.recv().await;
            }
            info!("Kafka init took {:?}", start.elapsed());

            post_store.finalize_init().await?;

            // Start stats logger
            Arc::clone(&post_store).start_stats_logger();
            info!("Started PostStore stats logger",);

            // Start auto-trim task to remove posts older than retention period
            Arc::clone(&post_store).start_auto_trim(2); // Run every 2 minutes
            info!(
                "Started PostStore auto-trim task (interval: 2 minutes, retention: {:.1} days)",
                args.post_retention_seconds as f64 / 86400.0
            );
        }
    }

    info!("Server ready");

    // Wait for termination signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received");
    cancel_token.cancel();
    info!("Server terminated");

    Ok(())
}
