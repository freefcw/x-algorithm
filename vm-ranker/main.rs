// vm-ranker 进程入口。
//
// 与上游 47c1bcd 的差异（U1）：上游使用私有 `xai_http_server`（统一
// HTTP+gRPC 壳、readiness 管理）与 `xai_profiling`；本地以 tonic Server +
// axum /metrics 端点替代，服务装配、DPP 配置与健康上报流程保持上游顺序。
// 上游 O2 embedding preload 在本地内存实现下立即完成。

use anyhow::{Context, Result};
use axum::routing::get;
use axum::Router;
use clap::Parser;
use log::info;

use xai_vm_ranker::{
    args::Args, dpp::DppConfig, embedding_store, ranker_service::VMRankerServiceImpl,
    scoring::DppContext,
};

async fn metrics_handler() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return String::new();
    }
    String::from_utf8(buffer).unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if args.enable_profiling {
        info!("profiling server unavailable in the local build (xai_profiling is not published)");
    }

    let (dpp, preload_future) = if args.dpp_enabled {
        let (store, preload_future) = embedding_store::init_store(args.embedding_dim)
            .context("DPP requested but embedding store init failed")?;
        let config = DppConfig {
            top_k: args.dpp_top_k,
            theta: args.dpp_theta,
            max_selected_rank: args.dpp_max_selected_rank,
            debug_viewer_id: args.dpp_debug_viewer_id,
        };
        info!(
            "DPP enabled: top_k={}, theta={}, max_selected_rank={}, embedding_dim={}, debug_viewer_id={}",
            config.top_k,
            config.theta,
            config.max_selected_rank,
            args.embedding_dim,
            config.debug_viewer_id,
        );
        (Some(DppContext { store, config }), Some(preload_future))
    } else {
        info!("DPP rescoring disabled");
        (None, None)
    };

    let ranker_service = VMRankerServiceImpl::new(args.max_concurrent_requests, dpp);
    info!(
        "Initialized VMRankerService with max_concurrent_requests={}",
        args.max_concurrent_requests
    );

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;

    if let Some(preload) = preload_future {
        preload.await;
    }

    let http_addr = format!("0.0.0.0:{}", args.http_port);
    let metrics_app = Router::new().route("/metrics", get(metrics_handler));
    let http_listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .with_context(|| format!("Failed to bind HTTP port {}", args.http_port))?;
    tokio::spawn(async move {
        let _ = axum::serve(http_listener, metrics_app).await;
    });

    info!("HTTP server on port: {}", args.http_port);
    info!("gRPC server on port: {}", args.grpc_port);
    info!(
        "Metrics server on: http://0.0.0.0:{}/metrics",
        args.http_port
    );

    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    info!("HTTP/gRPC server is ready");

    let grpc_addr = format!("0.0.0.0:{}", args.grpc_port)
        .parse()
        .context("invalid gRPC bind address")?;
    tonic::transport::Server::builder()
        .add_service(ranker_service.server())
        .add_service(health_service)
        .serve(grpc_addr)
        .await
        .context("gRPC server terminated with error")?;

    info!("Server terminated");

    Ok(())
}
