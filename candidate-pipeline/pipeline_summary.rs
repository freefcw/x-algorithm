//! Per-request pipeline stage summary, upstream `47c1bcd` shape.
//!
//! Upstream records stage statistics into a tokio task-local
//! `PipelineSummary` and emits one aggregated line per request instead of one
//! line per stage. The local build keeps the upstream type and function names
//! but replaces the tracing-span/stats-receiver backend with `log` (U1), and
//! hands the same summary to an optional [`PipelineObserver`] so metrics are
//! derived from one collection point instead of a second set of hooks.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::time::{Duration, Instant};

use log::info;

use crate::candidate_pipeline::PipelineStage;
use crate::observer::{PipelineObserver, PipelineReport, StageReport};

impl PipelineStage {
    /// Stable lower-case name used in the summary line and as a metric label.
    pub fn label(&self) -> &'static str {
        match self {
            PipelineStage::QueryHydrator => "query_hydrators",
            PipelineStage::DependentQueryHydrator => "dependent_query_hydrators",
            PipelineStage::Source => "sources",
            PipelineStage::Hydrator => "hydrators",
            PipelineStage::PostSelectionHydrator => "post_selection_hydrators",
            PipelineStage::Filter => "filters",
            PipelineStage::PostSelectionFilter => "post_selection_filters",
            PipelineStage::Scorer => "scorers",
            PipelineStage::Selector => "selector",
            PipelineStage::SideEffect => "side_effects",
        }
    }
}

tokio::task_local! {
    static ACTIVE: RefCell<PipelineSummary>;
}

/// Run `fut` with an active per-request summary collector.
pub async fn scope<F: Future>(fut: F) -> F::Output {
    ACTIVE
        .scope(RefCell::new(PipelineSummary::default()), fut)
        .await
}

/// Emit the aggregated one-line summary for the finished request and hand the
/// same data to `observer`.
pub fn emit(
    pipeline: &str,
    request_id: &str,
    start: Instant,
    result_size: usize,
    target_result_size: usize,
    observer: Option<&dyn PipelineObserver>,
) {
    let latency = start.elapsed();
    with_active(|summary| {
        info!(
            "request_id={} pipeline={} latency_ms={} result_size={} Summary:{}",
            request_id,
            pipeline,
            latency.as_millis() as u64,
            result_size,
            summary
        );
        if let Some(observer) = observer {
            observer.observe_request(&PipelineReport {
                pipeline,
                request_id,
                latency,
                result_size,
                target_result_size,
                stages: &summary.stages,
            });
        }
    });
}

pub(crate) fn record_source_fetched(name: &str, count: usize) {
    let recorded = with_active(|summary| {
        summary
            .stage_mut(PipelineStage::Source)
            .fetched_per_source
            .push((name.to_string(), count));
    });
    if !recorded {
        info!("Fetched {} candidates", count);
    }
}

/// A component returned an error for the whole request and was isolated.
pub(crate) fn record_component_failure(stage: PipelineStage, name: &str) {
    with_active(|summary| {
        summary
            .stage_mut(stage)
            .failed_components
            .push(name.to_string());
    });
}

/// A per-candidate component (hydrator, scorer) failed for `count` candidates.
pub(crate) fn record_failed_candidates(stage: PipelineStage, name: &str, count: usize) {
    if count == 0 {
        return;
    }
    with_active(|summary| {
        summary
            .stage_mut(stage)
            .failed_candidates_per_component
            .push((name.to_string(), count));
    });
}

pub struct StageStats {
    stage: PipelineStage,
    start: Instant,
}

impl StageStats {
    pub fn begin(stage: PipelineStage) -> Self {
        Self {
            stage,
            start: Instant::now(),
        }
    }

    pub fn record_components(&self, total: usize, enabled: usize) {
        with_active(|summary| {
            let stage = summary.stage_mut(self.stage);
            stage.total = total;
            stage.enabled = enabled;
        });
    }

    pub fn finish(self) {
        let latency = self.start.elapsed();
        let recorded = with_active(|summary| {
            summary.stage_mut(self.stage).latency = Some(latency);
        });
        if !recorded {
            info!("latency_ms={}", latency.as_millis());
        }
    }

    pub fn finish_with_size(self, size: usize) {
        let latency = self.start.elapsed();
        let recorded = with_active(|summary| {
            let stage = summary.stage_mut(self.stage);
            stage.latency = Some(latency);
            stage.size = Some(size);
        });
        if !recorded {
            info!("latency_ms={} size={}", latency.as_millis(), size);
        }
    }

    pub fn finish_filters(
        self,
        kept: usize,
        removed: usize,
        removed_per_filter: Vec<(String, usize)>,
    ) {
        if is_active() {
            let latency = self.start.elapsed();
            with_active(|summary| {
                let stage = summary.stage_mut(self.stage);
                stage.latency = Some(latency);
                stage.kept = Some(kept);
                stage.removed = Some(removed);
                stage.removed_per_filter = removed_per_filter;
            });
        } else {
            info!(
                "kept {}, removed {} removed_per_filter [{}]",
                kept,
                removed,
                Counts(&removed_per_filter),
            );
        }
    }
}

#[derive(Default)]
struct PipelineSummary {
    stages: Vec<StageReport>,
}

impl PipelineSummary {
    fn stage_mut(&mut self, stage: PipelineStage) -> &mut StageReport {
        if let Some(idx) = self.stages.iter().position(|s| s.stage == stage) {
            &mut self.stages[idx]
        } else {
            self.stages.push(StageReport::new(stage));
            self.stages.last_mut().unwrap()
        }
    }
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

impl fmt::Display for PipelineSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for stage in &self.stages {
            write!(
                f,
                " {}{{total={} enabled={}",
                stage.stage.label(),
                stage.total,
                stage.enabled
            )?;
            if let Some(latency) = stage.latency {
                write!(f, " latency_ms={}", millis(latency))?;
            }
            if !stage.fetched_per_source.is_empty() {
                write!(f, " fetched=[{}]", Counts(&stage.fetched_per_source))?;
            }
            if let Some(size) = stage.size {
                write!(f, " size={}", size)?;
            }
            if let (Some(kept), Some(removed)) = (stage.kept, stage.removed) {
                write!(f, " kept={} removed={}", kept, removed)?;
                if !stage.removed_per_filter.is_empty() {
                    write!(
                        f,
                        " removed_per_filter=[{}]",
                        Counts(&stage.removed_per_filter)
                    )?;
                }
            }
            if !stage.failed_components.is_empty() {
                write!(f, " failed=[{}]", stage.failed_components.join(","))?;
            }
            if !stage.failed_candidates_per_component.is_empty() {
                write!(
                    f,
                    " failed_candidates=[{}]",
                    Counts(&stage.failed_candidates_per_component)
                )?;
            }
            write!(f, "}}")?;
        }
        Ok(())
    }
}

struct Counts<'a>(&'a [(String, usize)]);

impl fmt::Display for Counts<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (name, count)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}={}", name, count)?;
        }
        Ok(())
    }
}

fn is_active() -> bool {
    ACTIVE.try_with(|_| ()).is_ok()
}

fn with_active(f: impl FnOnce(&mut PipelineSummary)) -> bool {
    ACTIVE
        .try_with(|summary| f(&mut summary.borrow_mut()))
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn render_active() -> String {
        ACTIVE
            .try_with(|summary| summary.borrow().to_string())
            .expect("summary should be active inside scope")
    }

    #[test]
    fn scope_collects_stage_stats_into_one_summary() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let rendered = runtime.block_on(scope(async {
            let stats = StageStats::begin(PipelineStage::Source);
            stats.record_components(3, 2);
            record_source_fetched("ThunderSource", 10);
            record_source_fetched("PhoenixSource", 40);
            record_component_failure(PipelineStage::Source, "FallbackSource");
            stats.finish_with_size(50);

            let filter_stats = StageStats::begin(PipelineStage::Filter);
            filter_stats.record_components(2, 2);
            filter_stats.finish_filters(45, 5, vec![("AgeFilter".to_string(), 5)]);

            let hydrator_stats = StageStats::begin(PipelineStage::Hydrator);
            record_failed_candidates(PipelineStage::Hydrator, "CoreDataCandidateHydrator", 3);
            record_failed_candidates(PipelineStage::Hydrator, "HasMediaHydrator", 0);
            hydrator_stats.finish_with_size(45);

            render_active()
        }));

        assert!(rendered.contains("sources{total=3 enabled=2"));
        assert!(rendered.contains("fetched=[ThunderSource=10,PhoenixSource=40]"));
        assert!(rendered.contains("size=50"));
        assert!(rendered.contains("failed=[FallbackSource]"));
        assert!(rendered.contains("filters{total=2 enabled=2"));
        assert!(rendered.contains("kept=45 removed=5"));
        assert!(rendered.contains("removed_per_filter=[AgeFilter=5]"));
        assert!(
            rendered.contains("failed_candidates=[CoreDataCandidateHydrator=3]"),
            "{rendered}"
        );
    }

    #[test]
    fn stage_stats_outside_scope_do_not_panic() {
        let stats = StageStats::begin(PipelineStage::Scorer);
        stats.record_components(1, 1);
        stats.finish_with_size(7);
        record_source_fetched("orphan", 1);
        record_component_failure(PipelineStage::Source, "orphan");
        record_failed_candidates(PipelineStage::Hydrator, "orphan", 1);
    }

    struct Captured {
        pipeline: String,
        request_id: String,
        result_size: usize,
        target_result_size: usize,
        stages: Vec<StageReport>,
    }

    #[derive(Default)]
    struct RecordingObserver(Mutex<Vec<Captured>>);

    impl PipelineObserver for RecordingObserver {
        fn observe_request(&self, report: &PipelineReport<'_>) {
            self.0.lock().unwrap().push(Captured {
                pipeline: report.pipeline.to_string(),
                request_id: report.request_id.to_string(),
                result_size: report.result_size,
                target_result_size: report.target_result_size,
                stages: report.stages.to_vec(),
            });
        }

        fn observe_side_effect(&self, _report: &crate::observer::SideEffectReport<'_>) {}
    }

    #[test]
    fn emit_hands_the_collected_summary_to_the_observer() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let observer = RecordingObserver::default();

        runtime.block_on(scope(async {
            let stats = StageStats::begin(PipelineStage::Source);
            stats.record_components(1, 1);
            record_source_fetched("TestSource", 3);
            stats.finish_with_size(3);
            emit(
                "TestPipeline",
                "req-1",
                Instant::now(),
                2,
                5,
                Some(&observer),
            );
        }));

        let captured = observer.0.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let report = &captured[0];
        assert_eq!(report.pipeline, "TestPipeline");
        assert_eq!(report.request_id, "req-1");
        assert_eq!((report.result_size, report.target_result_size), (2, 5));
        assert_eq!(report.stages.len(), 1);
        let source = &report.stages[0];
        assert_eq!(source.stage, PipelineStage::Source);
        assert_eq!((source.total, source.enabled), (1, 1));
        assert_eq!(source.size, Some(3));
        assert!(source.latency.is_some());
        assert_eq!(
            source.fetched_per_source,
            vec![("TestSource".to_string(), 3)]
        );
    }

    #[test]
    fn emit_outside_scope_neither_logs_a_summary_nor_calls_the_observer() {
        let observer = RecordingObserver::default();
        emit(
            "TestPipeline",
            "req-1",
            Instant::now(),
            0,
            5,
            Some(&observer),
        );
        assert!(observer.0.lock().unwrap().is_empty());
    }
}
