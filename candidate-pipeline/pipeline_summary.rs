//! Per-request pipeline stage summary, upstream `47c1bcd` shape.
//!
//! Upstream records stage statistics into a tokio task-local
//! `PipelineSummary` and emits one aggregated line per request instead of one
//! line per stage. The local build keeps the upstream type and function names
//! but replaces the tracing-span/stats-receiver backend with `log` (U1).

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::time::Instant;

use log::info;

use crate::candidate_pipeline::PipelineStage;

impl PipelineStage {
    fn summary_name(&self) -> &'static str {
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

/// Emit the aggregated one-line summary for the finished request.
pub fn emit(pipeline: &str, request_id: &str, start: Instant, result_size: usize) {
    with_active(|summary| {
        info!(
            "request_id={} pipeline={} latency_ms={} result_size={} Summary:{}",
            request_id,
            pipeline,
            start.elapsed().as_millis() as u64,
            result_size,
            summary
        );
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
        let latency_ms = self.latency_ms();
        let recorded = with_active(|summary| {
            summary.stage_mut(self.stage).latency_ms = Some(latency_ms);
        });
        if !recorded {
            info!("latency_ms={}", latency_ms);
        }
    }

    pub fn finish_with_size(self, size: usize) {
        let latency_ms = self.latency_ms();
        let recorded = with_active(|summary| {
            let stage = summary.stage_mut(self.stage);
            stage.latency_ms = Some(latency_ms);
            stage.size = Some(size);
        });
        if !recorded {
            info!("latency_ms={} size={}", latency_ms, size);
        }
    }

    pub fn finish_filters(
        self,
        kept: usize,
        removed: usize,
        removed_per_filter: Vec<(String, usize)>,
    ) {
        if is_active() {
            with_active(|summary| {
                let stage = summary.stage_mut(self.stage);
                stage.latency_ms = Some(self.latency_ms());
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

    fn latency_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[derive(Default)]
struct PipelineSummary {
    stages: Vec<StageSummary>,
}

#[derive(Default)]
struct StageSummary {
    name: &'static str,
    total: usize,
    enabled: usize,
    latency_ms: Option<u64>,
    size: Option<usize>,
    kept: Option<usize>,
    removed: Option<usize>,
    removed_per_filter: Vec<(String, usize)>,
    fetched_per_source: Vec<(String, usize)>,
}

impl PipelineSummary {
    fn stage_mut(&mut self, stage: PipelineStage) -> &mut StageSummary {
        let name = stage.summary_name();
        if let Some(idx) = self.stages.iter().position(|s| s.name == name) {
            &mut self.stages[idx]
        } else {
            self.stages.push(StageSummary {
                name,
                ..Default::default()
            });
            self.stages.last_mut().unwrap()
        }
    }
}

impl fmt::Display for PipelineSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for stage in &self.stages {
            write!(
                f,
                " {}{{total={} enabled={}",
                stage.name, stage.total, stage.enabled
            )?;
            if let Some(latency_ms) = stage.latency_ms {
                write!(f, " latency_ms={}", latency_ms)?;
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
            stats.finish_with_size(50);

            let filter_stats = StageStats::begin(PipelineStage::Filter);
            filter_stats.record_components(2, 2);
            filter_stats.finish_filters(45, 5, vec![("AgeFilter".to_string(), 5)]);

            render_active()
        }));

        assert!(rendered.contains("sources{total=3 enabled=2"));
        assert!(rendered.contains("fetched=[ThunderSource=10,PhoenixSource=40]"));
        assert!(rendered.contains("size=50"));
        assert!(rendered.contains("filters{total=2 enabled=2"));
        assert!(rendered.contains("kept=45 removed=5"));
        assert!(rendered.contains("removed_per_filter=[AgeFilter=5]"));
    }

    #[test]
    fn stage_stats_outside_scope_do_not_panic() {
        let stats = StageStats::begin(PipelineStage::Scorer);
        stats.record_components(1, 1);
        stats.finish_with_size(7);
        record_source_fetched("orphan", 1);
    }
}
