//! Operational HTTP endpoints served next to the gRPC port.
//!
//! - `GET /healthz` — liveness: 200 as soon as the process runs its event loop.
//! - `GET /readyz` — readiness: 200 only while recommendation traffic is
//!   accepted; 503 while the pipeline is still being assembled and again once
//!   shutdown has begun, so load balancers stop routing before the listener
//!   closes.
//! - `GET /metrics` — Prometheus text exposition of [`Metrics`].
//!
//! The listener is bound before the pipeline is built so orchestrator probes
//! get answers during a slow start instead of connection refusals.

use crate::metrics::Metrics;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessState {
    Starting,
    Ready,
    Draining,
}

impl ReadinessState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Draining,
            _ => Self::Starting,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
        }
    }
}

/// Process-wide readiness flag, flipped by the startup and shutdown sequence
/// and read by `/readyz`. Draining is terminal: a process never becomes ready
/// again after it has started to shut down.
#[derive(Clone, Default)]
pub struct Readiness(Arc<AtomicU8>);

impl Readiness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> ReadinessState {
        ReadinessState::from_u8(self.0.load(Ordering::Acquire))
    }

    pub fn set_ready(&self) {
        // Do not undo a drain that raced with a late startup step.
        let _ = self.0.compare_exchange(
            ReadinessState::Starting as u8,
            ReadinessState::Ready as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn set_draining(&self) {
        self.0
            .store(ReadinessState::Draining as u8, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct AdminState {
    readiness: Readiness,
    metrics: Arc<Metrics>,
}

impl AdminState {
    pub fn new(readiness: Readiness, metrics: Arc<Metrics>) -> Self {
        Self { readiness, metrics }
    }
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve the admin routes until `shutdown` resolves, then finish in-flight
/// probe requests and return.
pub async fn serve(
    listener: TcpListener,
    state: AdminState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz(State(state): State<AdminState>) -> Response {
    let readiness = state.readiness.state();
    let status = if readiness == ReadinessState::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, format!("{}\n", readiness.as_str())).into_response()
}

async fn metrics(State(state): State<AdminState>) -> Response {
    state
        .metrics
        .set_ready(state.readiness.state() == ReadinessState::Ready);
    match state.metrics.encode() {
        Ok(body) => (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(Metrics::content_type()),
            )],
            body,
        )
            .into_response(),
        Err(error) => {
            log::error!("failed to encode metrics: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics encoding failed\n",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn state() -> (Readiness, Arc<Metrics>, AdminState) {
        let readiness = Readiness::new();
        let metrics = Arc::new(Metrics::new());
        let state = AdminState::new(readiness.clone(), Arc::clone(&metrics));
        (readiness, metrics, state)
    }

    async fn get_path(state: &AdminState, path: &str) -> (StatusCode, String) {
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("body");
        (
            status,
            String::from_utf8(bytes.to_vec()).expect("utf-8 body"),
        )
    }

    #[test]
    fn readiness_moves_forward_only() {
        let readiness = Readiness::new();
        assert_eq!(readiness.state(), ReadinessState::Starting);
        readiness.set_ready();
        assert_eq!(readiness.state(), ReadinessState::Ready);
        readiness.set_draining();
        assert_eq!(readiness.state(), ReadinessState::Draining);
        readiness.set_ready();
        assert_eq!(
            readiness.state(),
            ReadinessState::Draining,
            "a late startup step must not reopen a draining process"
        );
    }

    #[tokio::test]
    async fn liveness_is_always_ok_and_readiness_tracks_the_lifecycle() {
        let (readiness, _, state) = state();

        assert_eq!(get_path(&state, "/healthz").await.0, StatusCode::OK);
        assert_eq!(
            get_path(&state, "/readyz").await,
            (StatusCode::SERVICE_UNAVAILABLE, "starting\n".to_string())
        );

        readiness.set_ready();
        assert_eq!(
            get_path(&state, "/readyz").await,
            (StatusCode::OK, "ready\n".to_string())
        );

        readiness.set_draining();
        assert_eq!(
            get_path(&state, "/readyz").await,
            (StatusCode::SERVICE_UNAVAILABLE, "draining\n".to_string())
        );
        assert_eq!(
            get_path(&state, "/healthz").await.0,
            StatusCode::OK,
            "draining is not a liveness failure"
        );
        assert_eq!(get_path(&state, "/nope").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn metrics_exposition_reflects_readiness_at_scrape_time() {
        let (readiness, _, state) = state();
        let (status, body) = get_path(&state, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("home_mixer_ready 0"), "{body}");
        assert!(body.contains("home_mixer_build_info"), "{body}");

        readiness.set_ready();
        let (_, body) = get_path(&state, "/metrics").await;
        assert!(body.contains("home_mixer_ready 1"), "{body}");
    }
}
