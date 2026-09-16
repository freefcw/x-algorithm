//! Execution observer: the metrics hook of a pipeline run.
//!
//! Every request already records one [`pipeline_summary`](crate::pipeline_summary)
//! per stage — latency, candidate counts, what each filter removed, what each
//! source fetched, which components failed — and turns it into one log line.
//! An observer receives the same data as a structured [`PipelineReport`] once
//! the request has finished, plus one [`SideEffectReport`] per side effect
//! when the asynchronous side-effect task completes.
//!
//! The crate deliberately knows nothing about metric backends: the application
//! implements [`PipelineObserver`] against its own registry and installs it
//! through [`CandidatePipeline::observer`](crate::candidate_pipeline::CandidatePipeline::observer).
//! Observers must be cheap and must not fail; they run on the request path
//! right before the response is returned.

use crate::candidate_pipeline::PipelineStage;
use std::time::Duration;

/// Everything the per-request summary recorded about one pipeline stage.
///
/// Fields that a stage does not produce stay `None` / empty: only `Source`
/// fills `fetched_per_source`, only filter stages fill `kept` / `removed` /
/// `removed_per_filter`, and `size` is the stage's output candidate count where
/// it has one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageReport {
    pub stage: PipelineStage,
    /// Components assembled for the stage.
    pub total: usize,
    /// Components whose `enable()` accepted this query.
    pub enabled: usize,
    pub latency: Option<Duration>,
    pub size: Option<usize>,
    pub kept: Option<usize>,
    pub removed: Option<usize>,
    pub removed_per_filter: Vec<(String, usize)>,
    pub fetched_per_source: Vec<(String, usize)>,
    /// Components that returned an error for the whole request (a query
    /// hydrator, source or filter). The pipeline logged and isolated them.
    pub failed_components: Vec<String>,
    /// Components that failed for some candidates but not the request
    /// (hydrators and scorers report per-candidate results).
    pub failed_candidates_per_component: Vec<(String, usize)>,
}

impl StageReport {
    pub(crate) fn new(stage: PipelineStage) -> Self {
        Self {
            stage,
            total: 0,
            enabled: 0,
            latency: None,
            size: None,
            kept: None,
            removed: None,
            removed_per_filter: Vec::new(),
            fetched_per_source: Vec::new(),
            failed_components: Vec::new(),
            failed_candidates_per_component: Vec::new(),
        }
    }
}

/// One finished request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineReport<'a> {
    /// `CandidatePipeline::name()` of the pipeline that ran.
    pub pipeline: &'a str,
    pub request_id: &'a str,
    /// Wall-clock time from the first query hydrator to the final list.
    pub latency: Duration,
    /// Candidates in the final response.
    pub result_size: usize,
    /// `CandidatePipeline::result_size()`; `result_size < target_result_size`
    /// is the `result_underfilled` condition.
    pub target_result_size: usize,
    /// Stages in execution order; stages that never ran are absent.
    pub stages: &'a [StageReport],
}

/// One side effect that finished after a response was returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SideEffectReport<'a> {
    pub pipeline: &'a str,
    pub request_id: &'a str,
    pub component: &'a str,
    pub latency: Duration,
    pub succeeded: bool,
}

pub trait PipelineObserver: Send + Sync {
    /// Called once per request, after the final candidate list is fixed and
    /// before the response is returned.
    fn observe_request(&self, report: &PipelineReport<'_>);

    /// Called once per enabled side effect when its asynchronous run settles,
    /// including runs cut off by the side-effect timeout.
    fn observe_side_effect(&self, report: &SideEffectReport<'_>);
}
