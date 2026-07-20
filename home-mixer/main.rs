use clap::Parser;
use log::info;
use std::net::SocketAddr;

use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder;

use x_algorithm_proto::home_mixer as pb;

use home_mixer::params;
use home_mixer::HomeMixerServer;

#[derive(Parser, Debug)]
#[command(about = "HomeMixer gRPC Server")]
struct Args {
    #[arg(long, default_value = "50051")]
    grpc_port: u16,
    #[arg(long, default_value = "9090")]
    metrics_port: u16,
    #[arg(long, default_value = "5")]
    reload_interval_minutes: u64,
    #[arg(long, default_value = "100")]
    chunk_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!(
        "Starting server with gRPC port: {}, metrics port: {}, reload interval: {} minutes, chunk size: {}",
        args.grpc_port, args.metrics_port, args.reload_interval_minutes, args.chunk_size,
    );

    // Create the service implementation
    let service = HomeMixerServer::new().await;
    // Build gRPC reflection service
    let reflection_service = Builder::configure()
        .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let mut grpc_routes = RoutesBuilder::default();

    grpc_routes.add_service(
        pb::scored_posts_service_server::ScoredPostsServiceServer::new(service)
            .max_decoding_message_size(params::MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(params::MAX_GRPC_MESSAGE_SIZE),
    );

    grpc_routes.add_service(reflection_service);

    // Start gRPC server
    let grpc_addr: SocketAddr = ([0, 0, 0, 0], args.grpc_port).into();
    let _grpc_handle = tokio::spawn(async move {
        info!("gRPC server listening on {}", grpc_addr);
        tonic::transport::Server::builder()
            .add_routes(grpc_routes.routes())
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

    // Start HTTP server (health/metrics)
    let http_router = axum::Router::new();
    let http_addr: SocketAddr = ([0, 0, 0, 0], args.metrics_port).into();
    let _http_handle = tokio::spawn(async move {
        info!("HTTP server listening on {}", http_addr);
        let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
        axum::serve(listener, http_router).await.unwrap();
    });

    info!("Server ready");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, server shutting down");

    Ok(())
}
