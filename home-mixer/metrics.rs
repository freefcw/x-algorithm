//! Prometheus registry for the Home Mixer process.
//!
//! One [`Metrics`] instance is created per process and shared by the gRPC
//! entry points, which record per-RPC outcomes, the candidate pipelines, which
//! report each request's stage summary through [`PipelineObserver`], and the
//! admin HTTP server, which exposes the registry on `/metrics`. The registry is
//! explicit rather than the crate-wide default so tests can build isolated
//! instances without tripping over duplicate registration.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::time::Instant;
use tonic::Code;
use xai_candidate_pipeline::observer::{
    PipelineObserver, PipelineReport, SideEffectReport, StageReport,
};

/// Request latency buckets in seconds. The upper edge matches the default
/// request budget (`params::REQUEST_TIMEOUT_MS`) so a deadline-exceeded
/// request still lands in a finite bucket.
const RPC_DURATION_BUCKETS: &[f64] = &[0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Stage latency buckets: the single external calls a stage wraps run from a
/// few milliseconds (Redis) to the 5 s Phoenix prediction budget.
const STAGE_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Final list sizes; `params::RESULT_SIZE` is 35 and the For You blender may
/// add module slots on top.
const RESULT_SIZE_BUCKETS: &[f64] = &[0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 50.0];

/// Candidate counts flowing out of a stage, from a handful after post-selection
/// filtering up to the combined recall of every source.
const STAGE_CANDIDATE_BUCKETS: &[f64] = &[
    0.0, 10.0, 25.0, 50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0,
];

pub struct Metrics {
    registry: Registry,
    ready: IntGauge,
    rpc: RpcMetrics,
    pipeline: PipelineMetrics,
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
        let pipeline = PipelineMetrics::new();

        registry
            .register(Box::new(build_info))
            .expect("register build_info");
        registry
            .register(Box::new(ready.clone()))
            .expect("register ready");
        rpc.register(&registry);
        pipeline.register(&registry);

        Self {
            registry,
            ready,
            rpc,
            pipeline,
        }
    }

    pub fn rpc(&self) -> &RpcMetrics {
        &self.rpc
    }

    pub fn pipeline(&self) -> &PipelineMetrics {
        &self.pipeline
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

/// Per-pipeline accounting fed by the candidate pipeline's request summary:
/// whole-request latency and result size, per-stage latency and candidate
/// counts, what each source fetched and each filter removed, which components
/// failed, and how side effects settled. Labels are the pipeline and component
/// type names, a fixed set decided at assembly time.
pub struct PipelineMetrics {
    duration: HistogramVec,
    result_size: HistogramVec,
    underfilled: IntCounterVec,
    stage_duration: HistogramVec,
    stage_candidates: HistogramVec,
    source_candidates: IntCounterVec,
    filter_removed: IntCounterVec,
    component_failures: IntCounterVec,
    component_failed_candidates: IntCounterVec,
    side_effect_runs: IntCounterVec,
    side_effect_duration: HistogramVec,
}

impl PipelineMetrics {
    fn new() -> Self {
        Self {
            duration: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_pipeline_duration_seconds",
                    "Candidate pipeline wall-clock time from query hydration to the final list",
                )
                .buckets(RPC_DURATION_BUCKETS.to_vec()),
                &["pipeline"],
            )
            .expect("valid pipeline duration opts"),
            result_size: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_pipeline_result_size",
                    "Candidates in the final list of a pipeline run",
                )
                .buckets(RESULT_SIZE_BUCKETS.to_vec()),
                &["pipeline"],
            )
            .expect("valid result size opts"),
            underfilled: IntCounterVec::new(
                Opts::new(
                    "home_mixer_pipeline_underfilled_total",
                    "Pipeline runs whose final list was shorter than the target result size",
                ),
                &["pipeline"],
            )
            .expect("valid underfilled opts"),
            stage_duration: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_stage_duration_seconds",
                    "Wall-clock time of one pipeline stage (all enabled components)",
                )
                .buckets(STAGE_DURATION_BUCKETS.to_vec()),
                &["pipeline", "stage"],
            )
            .expect("valid stage duration opts"),
            stage_candidates: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_stage_candidates",
                    "Candidates leaving a stage (kept candidates for filter stages)",
                )
                .buckets(STAGE_CANDIDATE_BUCKETS.to_vec()),
                &["pipeline", "stage"],
            )
            .expect("valid stage candidates opts"),
            source_candidates: IntCounterVec::new(
                Opts::new(
                    "home_mixer_source_candidates_total",
                    "Candidates returned by each source",
                ),
                &["pipeline", "source"],
            )
            .expect("valid source candidates opts"),
            filter_removed: IntCounterVec::new(
                Opts::new(
                    "home_mixer_filter_removed_total",
                    "Candidates removed by each filter (pre- and post-selection)",
                ),
                &["pipeline", "filter"],
            )
            .expect("valid filter removed opts"),
            component_failures: IntCounterVec::new(
                Opts::new(
                    "home_mixer_component_failures_total",
                    "Components that failed for a whole request and were isolated by the pipeline",
                ),
                &["pipeline", "stage", "component"],
            )
            .expect("valid component failures opts"),
            component_failed_candidates: IntCounterVec::new(
                Opts::new(
                    "home_mixer_component_failed_candidates_total",
                    "Candidates for which a hydrator or scorer reported an error",
                ),
                &["pipeline", "stage", "component"],
            )
            .expect("valid failed candidates opts"),
            side_effect_runs: IntCounterVec::new(
                Opts::new(
                    "home_mixer_side_effect_runs_total",
                    "Side effect runs by outcome (they run after the response is returned)",
                ),
                &["pipeline", "component", "result"],
            )
            .expect("valid side effect runs opts"),
            side_effect_duration: HistogramVec::new(
                HistogramOpts::new(
                    "home_mixer_side_effect_duration_seconds",
                    "Wall-clock time of one side effect run",
                )
                .buckets(RPC_DURATION_BUCKETS.to_vec()),
                &["pipeline", "component"],
            )
            .expect("valid side effect duration opts"),
        }
    }

    fn register(&self, registry: &Registry) {
        for collector in [
            Box::new(self.duration.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(self.result_size.clone()),
            Box::new(self.underfilled.clone()),
            Box::new(self.stage_duration.clone()),
            Box::new(self.stage_candidates.clone()),
            Box::new(self.source_candidates.clone()),
            Box::new(self.filter_removed.clone()),
            Box::new(self.component_failures.clone()),
            Box::new(self.component_failed_candidates.clone()),
            Box::new(self.side_effect_runs.clone()),
            Box::new(self.side_effect_duration.clone()),
        ] {
            registry
                .register(collector)
                .expect("register pipeline metric");
        }
    }

    fn record_request(&self, report: &PipelineReport<'_>) {
        let pipeline = report.pipeline;
        self.duration
            .with_label_values(&[pipeline])
            .observe(report.latency.as_secs_f64());
        self.result_size
            .with_label_values(&[pipeline])
            .observe(report.result_size as f64);
        if report.result_size < report.target_result_size {
            self.underfilled.with_label_values(&[pipeline]).inc();
        }
        for stage in report.stages {
            self.record_stage(pipeline, stage);
        }
    }

    fn record_stage(&self, pipeline: &str, stage: &StageReport) {
        let stage_label = stage.stage.label();
        if let Some(latency) = stage.latency {
            self.stage_duration
                .with_label_values(&[pipeline, stage_label])
                .observe(latency.as_secs_f64());
        }
        // Filter stages report their output as `kept`; every other stage as `size`.
        if let Some(size) = stage.kept.or(stage.size) {
            self.stage_candidates
                .with_label_values(&[pipeline, stage_label])
                .observe(size as f64);
        }
        for (source, count) in &stage.fetched_per_source {
            self.source_candidates
                .with_label_values(&[pipeline, source])
                .inc_by(*count as u64);
        }
        for (filter, count) in &stage.removed_per_filter {
            self.filter_removed
                .with_label_values(&[pipeline, filter])
                .inc_by(*count as u64);
        }
        for component in &stage.failed_components {
            self.component_failures
                .with_label_values(&[pipeline, stage_label, component])
                .inc();
        }
        for (component, count) in &stage.failed_candidates_per_component {
            self.component_failed_candidates
                .with_label_values(&[pipeline, stage_label, component])
                .inc_by(*count as u64);
        }
    }

    fn record_side_effect(&self, report: &SideEffectReport<'_>) {
        let result = if report.succeeded { "ok" } else { "error" };
        self.side_effect_runs
            .with_label_values(&[report.pipeline, report.component, result])
            .inc();
        self.side_effect_duration
            .with_label_values(&[report.pipeline, report.component])
            .observe(report.latency.as_secs_f64());
    }
}

impl PipelineObserver for Metrics {
    fn observe_request(&self, report: &PipelineReport<'_>) {
        self.pipeline.record_request(report);
    }

    fn observe_side_effect(&self, report: &SideEffectReport<'_>) {
        self.pipeline.record_side_effect(report);
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
    fn pipeline_reports_become_stage_source_filter_and_failure_series() {
        use std::time::Duration;
        use xai_candidate_pipeline::candidate_pipeline::PipelineStage;

        let metrics = Metrics::new();
        let stages = vec![
            StageReport {
                stage: PipelineStage::Source,
                total: 3,
                enabled: 2,
                latency: Some(Duration::from_millis(120)),
                size: Some(400),
                kept: None,
                removed: None,
                removed_per_filter: Vec::new(),
                fetched_per_source: vec![
                    ("ThunderSource".to_string(), 300),
                    ("PhoenixSource".to_string(), 100),
                ],
                failed_components: vec!["FallbackSource".to_string()],
                failed_candidates_per_component: Vec::new(),
            },
            StageReport {
                stage: PipelineStage::Filter,
                total: 5,
                enabled: 5,
                latency: Some(Duration::from_millis(2)),
                size: None,
                kept: Some(350),
                removed: Some(50),
                removed_per_filter: vec![("AgeFilter".to_string(), 50)],
                fetched_per_source: Vec::new(),
                failed_components: Vec::new(),
                failed_candidates_per_component: Vec::new(),
            },
            StageReport {
                stage: PipelineStage::Hydrator,
                total: 6,
                enabled: 6,
                latency: Some(Duration::from_millis(80)),
                size: Some(400),
                kept: None,
                removed: None,
                removed_per_filter: Vec::new(),
                fetched_per_source: Vec::new(),
                failed_components: Vec::new(),
                failed_candidates_per_component: vec![("CoreDataCandidateHydrator".to_string(), 7)],
            },
        ];
        metrics.observe_request(&PipelineReport {
            pipeline: "PhoenixCandidatePipeline",
            request_id: "req-1",
            latency: Duration::from_millis(900),
            result_size: 30,
            target_result_size: 35,
            stages: &stages,
        });
        metrics.observe_side_effect(&SideEffectReport {
            pipeline: "PhoenixCandidatePipeline",
            request_id: "req-1",
            component: "ServedCandidatesKafkaSideEffect",
            latency: Duration::from_millis(15),
            succeeded: false,
        });

        let text = metrics.encode().expect("text exposition");
        for expected in [
            "home_mixer_pipeline_duration_seconds_bucket{pipeline=\"PhoenixCandidatePipeline\",le=\"1\"} 1",
            "home_mixer_pipeline_result_size_bucket{pipeline=\"PhoenixCandidatePipeline\",le=\"30\"} 1",
            "home_mixer_pipeline_underfilled_total{pipeline=\"PhoenixCandidatePipeline\"} 1",
            "home_mixer_stage_duration_seconds_count{pipeline=\"PhoenixCandidatePipeline\",stage=\"sources\"} 1",
            "home_mixer_stage_candidates_bucket{pipeline=\"PhoenixCandidatePipeline\",stage=\"filters\",le=\"400\"} 1",
            "home_mixer_source_candidates_total{pipeline=\"PhoenixCandidatePipeline\",source=\"ThunderSource\"} 300",
            "home_mixer_source_candidates_total{pipeline=\"PhoenixCandidatePipeline\",source=\"PhoenixSource\"} 100",
            "home_mixer_filter_removed_total{filter=\"AgeFilter\",pipeline=\"PhoenixCandidatePipeline\"} 50",
            "home_mixer_component_failures_total{component=\"FallbackSource\",pipeline=\"PhoenixCandidatePipeline\",stage=\"sources\"} 1",
            "home_mixer_component_failed_candidates_total{component=\"CoreDataCandidateHydrator\",pipeline=\"PhoenixCandidatePipeline\",stage=\"hydrators\"} 7",
            "home_mixer_side_effect_runs_total{component=\"ServedCandidatesKafkaSideEffect\",pipeline=\"PhoenixCandidatePipeline\",result=\"error\"} 1",
            "home_mixer_side_effect_duration_seconds_count{component=\"ServedCandidatesKafkaSideEffect\",pipeline=\"PhoenixCandidatePipeline\"} 1",
        ] {
            assert!(text.contains(expected), "missing {expected}\n{text}");
        }

        // A full list is not underfilled.
        metrics.observe_request(&PipelineReport {
            pipeline: "PhoenixCandidatePipeline",
            request_id: "req-2",
            latency: Duration::from_millis(100),
            result_size: 35,
            target_result_size: 35,
            stages: &[],
        });
        let text = metrics.encode().expect("text exposition");
        assert!(text.contains(
            "home_mixer_pipeline_underfilled_total{pipeline=\"PhoenixCandidatePipeline\"} 1"
        ));
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
