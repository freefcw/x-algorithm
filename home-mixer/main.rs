//! Home Mixer process entry point.
//!
//! Startup order is chosen so every externally visible signal is truthful:
//!
//! 1. configuration is parsed and validated before any socket opens;
//! 2. the admin HTTP port binds first, so `/healthz` answers and `/readyz`
//!    reports `starting` while the pipeline assembles (Redis handshakes,
//!    adapter construction);
//! 3. the gRPC listener binds only after assembly succeeded — a bound gRPC
//!    port therefore means the service can take traffic;
//! 4. `/readyz` flips to `ready` and `Server ready` is logged.
//!
//! On SIGTERM or Ctrl-C the process marks itself draining (`/readyz` → 503),
//! optionally waits for load balancers to notice, stops accepting new
//! connections, and lets in-flight requests finish up to `--drain-timeout-secs`.
//! Any listener failure — including a port already in use — ends the process
//! with a non-zero exit instead of leaving it alive but not serving.

use anyhow::Context;
use clap::Parser;
use log::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::service::RoutesBuilder;
use tonic::transport::server::TcpIncoming;
use tonic_reflection::server::Builder;

use home_mixer::admin_server::{self, AdminState, Readiness};
use home_mixer::metrics::Metrics;
use home_mixer::{params, shutdown, HomeMixerConfig, HomeMixerServer};
use x_algorithm_proto::home_mixer as pb;

#[derive(Parser, Debug)]
#[command(about = "HomeMixer gRPC Server")]
struct Args {
    /// gRPC port for ScoredPostsService and ForYouFeedService.
    #[arg(long, default_value = "50051")]
    grpc_port: u16,
    /// HTTP port for /healthz, /readyz and /metrics.
    #[arg(long, default_value = "9090")]
    metrics_port: u16,
    /// Seconds to keep serving after a termination signal before the
    /// listeners close, so load balancers can observe /readyz turning 503
    /// first. Leave at 0 when the platform provides an equivalent pre-stop
    /// hook.
    #[arg(long, default_value_t = 0)]
    shutdown_delay_secs: u64,
    /// Longest wait for in-flight requests once the listeners have closed.
    /// Keep it below the platform's termination grace period.
    #[arg(long, default_value_t = params::SHUTDOWN_DRAIN_TIMEOUT_SECS)]
    drain_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    home_mixer::logging::init_from_env()?;
    let args = Args::parse();

    info!(
        "Starting server with gRPC port: {}, metrics port: {}, shutdown delay: {} s, drain timeout: {} s",
        args.grpc_port, args.metrics_port, args.shutdown_delay_secs, args.drain_timeout_secs,
    );

    // Configuration problems fail before any socket opens.
    let config = HomeMixerConfig::from_env()?;
    let drain_timeout = Duration::from_secs(args.drain_timeout_secs);
    if drain_timeout < config.request_timeout {
        warn!(
            "--drain-timeout-secs ({:?}) is shorter than the request budget ({:?}); the slowest in-flight request may be cut off at shutdown",
            drain_timeout, config.request_timeout
        );
    }

    let readiness = Readiness::new();
    let metrics = Arc::new(Metrics::new());
    let (stop_tx, stop_rx) = watch::channel(false);

    // Termination handling is installed before the slow startup steps so a
    // signal during assembly aborts startup instead of being ignored.
    {
        let readiness = readiness.clone();
        let shutdown_delay = Duration::from_secs(args.shutdown_delay_secs);
        tokio::spawn(async move {
            shutdown::signal().await;
            info!("shutdown signal received; /readyz now reports draining");
            readiness.set_draining();
            if !shutdown_delay.is_zero() {
                tokio::time::sleep(shutdown_delay).await;
            }
            let _ = stop_tx.send(true);
        });
    }

    // Admin HTTP first: probes must get answers while the pipeline assembles.
    let http_addr: SocketAddr = ([0, 0, 0, 0], args.metrics_port).into();
    let http_listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("failed to bind admin HTTP port {http_addr}"))?;
    info!("HTTP server listening on {http_addr} (/healthz, /readyz, /metrics)");
    let admin = tokio::spawn(admin_server::serve(
        http_listener,
        AdminState::new(readiness.clone(), Arc::clone(&metrics)),
        stopped(stop_rx.clone()),
    ));

    let service = tokio::select! {
        built = HomeMixerServer::build_with_metrics(config, Arc::clone(&metrics)) => Arc::new(built?),
        _ = stopped(stop_rx.clone()) => {
            info!("shutdown requested during startup; exiting before serving");
            return Ok(());
        }
    };

    let reflection_service = Builder::configure()
        .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let mut grpc_routes = RoutesBuilder::default();
    service.register(&mut grpc_routes);
    grpc_routes.add_service(reflection_service);

    // Bind explicitly so an occupied port is an error here, not a panic inside
    // a background task that the process would survive.
    let grpc_addr: SocketAddr = ([0, 0, 0, 0], args.grpc_port).into();
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind gRPC port {grpc_addr}"))?;
    let incoming = TcpIncoming::from_listener(grpc_listener, true, None)
        .map_err(|error| anyhow::anyhow!("failed to accept on gRPC port {grpc_addr}: {error}"))?;
    let grpc = tonic::transport::Server::builder()
        .add_routes(grpc_routes.routes())
        .serve_with_incoming_shutdown(incoming, stopped(stop_rx.clone()));

    readiness.set_ready();
    info!("gRPC server listening on {grpc_addr}");
    info!("Server ready");

    // Both listeners run until the stop signal drains them. A transport error
    // in either ends the process with that error.
    let servers = async {
        tokio::try_join!(async { grpc.await.context("gRPC server failed") }, async {
            admin
                .await
                .context("admin HTTP server task failed")?
                .context("admin HTTP server failed")
        },)
        .map(|_| ())
    };
    tokio::select! {
        result = servers => result?,
        _ = drain_deadline(stop_rx, drain_timeout) => {
            warn!("in-flight requests did not finish within {drain_timeout:?}; exiting anyway");
        }
    }
    info!("Server stopped");
    Ok(())
}

/// Resolves once the stop signal has been sent. A dropped sender also
/// resolves, so a listener never waits on a signal task that is gone.
async fn stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.wait_for(|stop| *stop).await;
}

/// Resolves `drain_timeout` after the stop signal.
async fn drain_deadline(stop: watch::Receiver<bool>, drain_timeout: Duration) {
    stopped(stop).await;
    tokio::time::sleep(drain_timeout).await;
}
