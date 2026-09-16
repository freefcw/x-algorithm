//! Prometheus registry for the Home Mixer process.
//!
//! One [`Metrics`] instance is created per process and shared by the gRPC
//! entry points, which record per-RPC outcomes, and the admin HTTP server,
//! which exposes the registry on `/metrics`. The registry is explicit rather
//! than the crate-wide default so tests can build isolated instances without
//! tripping over duplicate registration.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::time::Instant;
use tonic::Code;

/// Request latency buckets in seconds. The upper edge matches the default
/// request budget (`params::REQUEST_TIMEOUT_MS`) so a deadline-exceeded
/// request still lands in a finite bucket.
const RPC_DURATION_BUCKETS: &[f64] = &[0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

pub struct Metrics {
    registry: Registry,
    ready: IntGauge,
    rpc: RpcMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build a registry with every metric this process emits. Registration
    /// can only fail on a duplicate name, which is a programming error in this
    /// module, so it panics instead of returning a `Result`.
    pub fn new() -> Self {
        let registry = Registry::new();
        let build_info = IntGaugeVec::new(
            Opts::new(
                "home_mixer_build_info",
                "Constant 1, labelled with the running crate version",
            ),
            &["version"],
        )
        .expect("valid build_info opts");
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        let ready = IntGauge::new(
            "home_mixer_ready",
            "1 while the process accepts recommendation traffic, 0 while starting or draining",
        )
        .expect("valid ready opts");
        let rpc = RpcMetrics::new();

        registry
            .register(Box::new(build_info))
            .expect("register build_info");
        registry
            .register(Box::new(ready.clone()))
            .expect("register ready");
        rpc.register(&registry);

        Self {
            registry,
            ready,
            rpc,
        }
    }

    pub fn rpc(&self) -> &RpcMetrics {
        &self.rpc
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.set(i64::from(ready));
    }

    /// Prometheus text exposition of the whole registry.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&self.registry.gather(), &mut buffer)?;
        String::from_utf8(buffer).map_err(|error| prometheus::Error::Msg(error.to_string()))
    }

    /// `Content-Type` for [`Self::encode`].
    pub fn content_type() -> &'static str {
        prometheus::TEXT_FORMAT
    }
}

/// Per-RPC accounting: terminal status counts, latency and in-flight gauge,
/// all labelled by the gRPC method name.
pub struct RpcMetrics {
    requests: IntCounterVec,
    duration: HistogramVec,
    in_flight: IntGaugeVec,
}

impl RpcMetrics {
    fn new() -> Self {
        Self {
            requests: IntCounterVec::new(
                Opts::new(
                    "home_mixer_rpc_requests_total",
                    "Completed RPCs by method and terminal gRPC status code",
                ),
                &["rpc", "code"],
            )
            .expect("valid requests opts"),
            duration: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_rpc_duration_seconds",
                    "Wall-clock time from RPC entry to the terminal status",
                )
                .buckets(RPC_DURATION_BUCKETS.to_vec()),
                &["rpc"],
            )
            .expect("valid duration opts"),
            in_flight: IntGaugeVec::new(
                Opts::new(
                    "home_mixer_rpc_in_flight",
                    "RPCs currently executing, by method",
                ),
                &["rpc"],
            )
            .expect("valid in_flight opts"),
        }
    }

    fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.requests.clone()))
            .expect("register rpc requests");
        registry
            .register(Box::new(self.duration.clone()))
            .expect("register rpc duration");
        registry
            .register(Box::new(self.in_flight.clone()))
            .expect("register rpc in_flight");
    }

    /// Begin accounting for one RPC. The returned observation must be
    /// finished with the terminal status; see [`RpcObservation`].
    pub fn start(&self, rpc: &'static str) -> RpcObservation<'_> {
        self.in_flight.with_label_values(&[rpc]).inc();
        RpcObservation {
            metrics: self,
            rpc,
            started: Instant::now(),
            finished: false,
        }
    }

    fn record(&self, rpc: &'static str, code: Code, started: Instant) {
        self.requests
            .with_label_values(&[rpc, code_label(code)])
            .inc();
        self.duration
            .with_label_values(&[rpc])
            .observe(started.elapsed().as_secs_f64());
    }

    #[cfg(test)]
    fn completed(&self, rpc: &str, code: Code) -> u64 {
        self.requests
            .with_label_values(&[rpc, code_label(code)])
            .get()
    }

    #[cfg(test)]
    fn in_flight(&self, rpc: &str) -> i64 {
        self.in_flight.with_label_values(&[rpc]).get()
    }
}

/// Accounting for one RPC in progress.
///
/// [`finish`](Self::finish) records the terminal status. Dropping the
/// observation unfinished means the handler future was cancelled before it
/// produced a status (client disconnect, or the transport-level
/// `grpc-timeout` firing first); that is recorded as `CANCELLED`, so the
/// in-flight gauge never leaks and every request ends in exactly one bucket.
pub struct RpcObservation<'a> {
    metrics: &'a RpcMetrics,
    rpc: &'static str,
    started: Instant,
    finished: bool,
}

impl RpcObservation<'_> {
    pub fn finish(mut self, code: Code) {
        self.metrics.record(self.rpc, code, self.started);
        self.finished = true;
    }
}

impl Drop for RpcObservation<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.record(self.rpc, Code::Cancelled, self.started);
        }
        self.metrics.in_flight.with_label_values(&[self.rpc]).dec();
    }
}

/// Canonical gRPC status names, as used by other language runtimes' metrics.
pub fn code_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_observations_record_the_terminal_code_and_release_in_flight() {
        let metrics = Metrics::new();
        let observation = metrics.rpc().start("GetScoredPosts");
        assert_eq!(metrics.rpc().in_flight("GetScoredPosts"), 1);
        observation.finish(Code::Unavailable);
        assert_eq!(metrics.rpc().in_flight("GetScoredPosts"), 0);
        assert_eq!(
            metrics.rpc().completed("GetScoredPosts", Code::Unavailable),
            1
        );
        assert_eq!(metrics.rpc().completed("GetScoredPosts", Code::Ok), 0);
    }

    #[test]
    fn a_dropped_observation_counts_as_cancelled() {
        let metrics = Metrics::new();
        {
            let _observation = metrics.rpc().start("GetForYouFeed");
            assert_eq!(metrics.rpc().in_flight("GetForYouFeed"), 1);
        }
        assert_eq!(metrics.rpc().in_flight("GetForYouFeed"), 0);
        assert_eq!(metrics.rpc().completed("GetForYouFeed", Code::Cancelled), 1);
    }

    #[test]
    fn exposition_carries_build_info_readiness_and_rpc_series() {
        let metrics = Metrics::new();
        metrics.set_ready(true);
        metrics.rpc().start("GetScoredPosts").finish(Code::Ok);

        let text = metrics.encode().expect("text exposition");
        assert!(text.contains(&format!(
            "home_mixer_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        )));
        assert!(text.contains("home_mixer_ready 1"));
        assert!(
            text.contains("home_mixer_rpc_requests_total{code=\"OK\",rpc=\"GetScoredPosts\"} 1")
        );
        assert!(text.contains(
            "home_mixer_rpc_duration_seconds_bucket{rpc=\"GetScoredPosts\",le=\"10\"} 1"
        ));
        assert!(text.contains("home_mixer_rpc_in_flight{rpc=\"GetScoredPosts\"} 0"));
        assert_eq!(Metrics::content_type(), "text/plain; version=0.0.4");
    }

    #[test]
    fn every_code_has_a_canonical_label() {
        let labels: std::collections::HashSet<_> = (0..=16)
            .map(|value| code_label(Code::from_i32(value)))
            .collect();
        assert_eq!(labels.len(), 17, "labels must be distinct");
        assert!(labels
            .iter()
            .all(|label| label.chars().all(|c| c.is_ascii_uppercase() || c == '_')));
    }
}
