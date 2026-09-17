//! 运维 HTTP 端点（与 gRPC 端口分开的 `--http-port`）。
//!
//! - `GET /healthz` —— 存活探针：事件循环跑起来就返回 200。
//! - `GET /readyz` —— 就绪探针：只有初始化完成（demo 灌库完成或 Kafka
//!   追平并 `finalize_init`）后才 200；关机开始后回到 503，负载均衡器
//!   可以先于进程退出停止导流。
//! - `GET /metrics` —— `metrics.rs` 注册进默认 registry 的 Prometheus
//!   文本导出；抓取时顺带刷新 `thunder_ready`。
//!
//! 监听 socket 在初始化之前就 bind，编排器探针在慢启动期间拿到的是
//! 503 `starting` 而不是 connection refused。

use crate::metrics::THUNDER_READY;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder, TextEncoder};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

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

/// 进程级就绪标志：启动序列置 ready，关机序列置 draining，只能前进。
/// 与 home-mixer 的 `Readiness` 同语义；thunder 的指标在全局默认
/// registry，不需要额外句柄。
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
        // 不回滚与 draining 竞争的迟到启动步骤。
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
}

impl AdminState {
    pub fn new(readiness: Readiness) -> Self {
        Self { readiness }
    }
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
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
    // 抓取时刷新生命周期 gauge，避免另起后台任务。
    THUNDER_READY.set(i64::from(state.readiness.state() == ReadinessState::Ready));

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    match encoder.encode(&prometheus::gather(), &mut buffer) {
        Ok(()) => (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(prometheus::TEXT_FORMAT),
            )],
            buffer,
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
            "迟到的启动步骤不能把 draining 进程重新置为 ready"
        );
    }

    #[tokio::test]
    async fn liveness_is_always_ok_and_readiness_tracks_the_lifecycle() {
        let readiness = Readiness::new();
        let state = AdminState::new(readiness.clone());

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
            "draining 不是存活失败"
        );
        assert_eq!(get_path(&state, "/nope").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn metrics_exposition_carries_registered_families_and_readiness() {
        let readiness = Readiness::new();
        let state = AdminState::new(readiness.clone());

        // lazy_static 家族首次解引用才注册进默认 registry；先触碰一个
        // 计数器，验证它在导出里出现。
        crate::metrics::KAFKA_POLL_ERRORS.inc();
        let (status, body) = get_path(&state, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("thunder_ready 0"), "{body}");
        assert!(body.contains("thunder_kafka_poll_errors_total 1"), "{body}");

        readiness.set_ready();
        let (_, body) = get_path(&state, "/metrics").await;
        assert!(body.contains("thunder_ready 1"), "{body}");
    }
}
