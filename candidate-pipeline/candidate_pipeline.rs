use crate::filter::Filter;
use crate::hydrator::Hydrator;
use crate::query_hydrator::QueryHydrator;
use crate::scorer::Scorer;
use crate::selector::{SelectResult, Selector};
use crate::side_effect::{SideEffect, SideEffectInput};
use crate::source::Source;
use futures::future::join_all;
use log::{error, info, warn};
use std::sync::Arc;
use std::time::Instant;
use tonic::async_trait;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PipelineStage {
    QueryHydrator,
    DependentQueryHydrator,
    Source,
    Hydrator,
    PostSelectionHydrator,
    Filter,
    PostSelectionFilter,
    Scorer,
    Selector,
    SideEffect,
}

pub struct PipelineComponents {
    pub stage: PipelineStage,
    pub components: Vec<String>,
}

pub struct PipelineResult<Q, C> {
    pub retrieved_candidates: Vec<C>,
    pub filtered_candidates: Vec<C>,
    pub selected_candidates: Vec<C>,
    pub query: Arc<Q>,
}

impl<Q: Default, C> PipelineResult<Q, C> {
    pub fn empty() -> Self {
        Self {
            retrieved_candidates: Vec::new(),
            filtered_candidates: Vec::new(),
            selected_candidates: Vec::new(),
            query: Arc::new(Q::default()),
        }
    }
}

/// Provides a stable request identifier for logging/tracing.
pub trait HasRequestId {
    fn request_id(&self) -> &str;
}

pub trait PipelineQuery: HasRequestId + Clone + Send + Sync + 'static {}

impl<T> PipelineQuery for T where T: HasRequestId + Clone + Send + Sync + 'static {}

pub trait PipelineCandidate: Clone + Send + Sync + 'static {}

impl<T> PipelineCandidate for T where T: Clone + Send + Sync + 'static {}

#[async_trait]
pub trait CandidatePipeline<Q, C>: Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    fn query_hydrators(&self) -> &[Box<dyn QueryHydrator<Q>>];
    fn dependent_query_hydrators(&self) -> &[Box<dyn QueryHydrator<Q>>] {
        &[]
    }
    fn sources(&self) -> &[Box<dyn Source<Q, C>>];
    fn hydrators(&self) -> &[Box<dyn Hydrator<Q, C>>];
    fn filters(&self) -> &[Box<dyn Filter<Q, C>>];
    fn scorers(&self) -> &[Box<dyn Scorer<Q, C>>];
    fn selector(&self) -> &dyn Selector<Q, C>;
    fn post_selection_hydrators(&self) -> &[Box<dyn Hydrator<Q, C>>];
    fn post_selection_filters(&self) -> &[Box<dyn Filter<Q, C>>];
    fn side_effects(&self) -> Arc<Vec<Box<dyn SideEffect<Q, C>>>>;
    fn result_size(&self) -> usize;

    fn finalize(&self, _query: &Q, _candidates: &mut Vec<C>) {}

    fn components(&self) -> Vec<PipelineComponents> {
        let side_effects = self.side_effects();
        vec![
            PipelineComponents {
                stage: PipelineStage::QueryHydrator,
                components: self
                    .query_hydrators()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::DependentQueryHydrator,
                components: self
                    .dependent_query_hydrators()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::Source,
                components: self
                    .sources()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::Hydrator,
                components: self
                    .hydrators()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::Filter,
                components: self
                    .filters()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::Scorer,
                components: self
                    .scorers()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::Selector,
                components: vec![self.selector().name().to_string()],
            },
            PipelineComponents {
                stage: PipelineStage::PostSelectionHydrator,
                components: self
                    .post_selection_hydrators()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::PostSelectionFilter,
                components: self
                    .post_selection_filters()
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
            PipelineComponents {
                stage: PipelineStage::SideEffect,
                components: side_effects
                    .iter()
                    .map(|component| component.name().to_string())
                    .collect(),
            },
        ]
    }

    async fn execute(&self, query: Q) -> PipelineResult<Q, C> {
        let hydrated_query = self.hydrate_query(query).await;
        let hydrated_query = self.hydrate_dependent_query(hydrated_query).await;

        let candidates = self.fetch_candidates(&hydrated_query).await;
        if candidates.is_empty() {
            let request_id = hydrated_query.request_id().to_string();
            info!(
                "request_id={} pipeline short-circuited: no candidates",
                request_id
            );
            let query = Arc::new(hydrated_query);
            self.run_side_effects(Arc::new(SideEffectInput {
                query: Arc::clone(&query),
                selected_candidates: Vec::new(),
                non_selected_candidates: Vec::new(),
            }));
            return PipelineResult {
                retrieved_candidates: Vec::new(),
                filtered_candidates: Vec::new(),
                selected_candidates: Vec::new(),
                query,
            };
        }

        let hydrated_candidates = self.hydrate(&hydrated_query, candidates).await;

        let (kept_candidates, mut filtered_candidates) =
            self.filter(&hydrated_query, hydrated_candidates.clone());

        let scored_candidates = self.score(&hydrated_query, kept_candidates).await;

        let SelectResult {
            selected: selected_candidates,
            non_selected: mut non_selected_candidates,
        } = self.select(&hydrated_query, scored_candidates);

        let post_selection_hydrated_candidates = self
            .hydrate_post_selection(&hydrated_query, selected_candidates)
            .await;

        let (mut final_candidates, post_selection_filtered_candidates) =
            self.filter_post_selection(&hydrated_query, post_selection_hydrated_candidates);
        filtered_candidates.extend(post_selection_filtered_candidates);

        let truncated_candidates =
            final_candidates.split_off(self.result_size().min(final_candidates.len()));
        non_selected_candidates.extend(truncated_candidates);
        self.finalize(&hydrated_query, &mut final_candidates);

        let arc_hydrated_query = Arc::new(hydrated_query);
        let input = Arc::new(SideEffectInput {
            query: arc_hydrated_query.clone(),
            selected_candidates: final_candidates.clone(),
            non_selected_candidates,
        });
        self.run_side_effects(input);

        PipelineResult {
            retrieved_candidates: hydrated_candidates,
            filtered_candidates,
            selected_candidates: final_candidates,
            query: arc_hydrated_query,
        }
    }

    /// Run all query hydrators in parallel and merge results into the query.
    async fn hydrate_query(&self, query: Q) -> Q {
        let request_id = query.request_id().to_string();
        let hydrators: Vec<_> = self
            .query_hydrators()
            .iter()
            .filter(|h| h.enable(&query))
            .collect();
        let query_ref = &query;
        let results = join_all(hydrators.iter().map(|hydrator| async move {
            let started = Instant::now();
            (hydrator, started, hydrator.hydrate(query_ref).await)
        }))
        .await;

        let mut hydrated_query = query;
        for (hydrator, started, result) in results {
            match result {
                Ok(hydrated) => {
                    hydrator.update(&mut hydrated_query, hydrated);
                    info!(
                        "request_id={} stage={:?} component={} elapsed_ms={}",
                        request_id,
                        PipelineStage::QueryHydrator,
                        hydrator.name(),
                        started.elapsed().as_millis()
                    );
                }
                Err(err) => {
                    error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        PipelineStage::QueryHydrator,
                        hydrator.name(),
                        err,
                        started.elapsed().as_millis()
                    );
                }
            }
        }
        hydrated_query
    }

    /// Run query hydrators that depend on the initial hydration result.
    async fn hydrate_dependent_query(&self, query: Q) -> Q {
        let request_id = query.request_id().to_string();
        let hydrators: Vec<_> = self
            .dependent_query_hydrators()
            .iter()
            .filter(|hydrator| hydrator.enable(&query))
            .collect();
        let query_ref = &query;
        let results = join_all(hydrators.iter().map(|hydrator| async move {
            let started = Instant::now();
            (hydrator, started, hydrator.hydrate(query_ref).await)
        }))
        .await;

        let mut hydrated_query = query;
        for (hydrator, started, result) in results {
            match result {
                Ok(hydrated) => {
                    hydrator.update(&mut hydrated_query, hydrated);
                    info!(
                        "request_id={} stage={:?} component={} elapsed_ms={}",
                        request_id,
                        PipelineStage::DependentQueryHydrator,
                        hydrator.name(),
                        started.elapsed().as_millis()
                    );
                }
                Err(error) => error!(
                    "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                    request_id,
                    PipelineStage::DependentQueryHydrator,
                    hydrator.name(),
                    error,
                    started.elapsed().as_millis()
                ),
            }
        }
        hydrated_query
    }

    /// Run all candidate sources in parallel and collect results.
    async fn fetch_candidates(&self, query: &Q) -> Vec<C> {
        let request_id = query.request_id().to_string();
        let sources: Vec<_> = self.sources().iter().filter(|s| s.enable(query)).collect();
        let results = join_all(sources.iter().map(|source| async move {
            let started = Instant::now();
            (source, started, source.get_candidates(query).await)
        }))
        .await;

        let mut collected = Vec::new();
        for (source, started, result) in results {
            match result {
                Ok(mut candidates) => {
                    info!(
                        "request_id={} stage={:?} component={} output={} elapsed_ms={}",
                        request_id,
                        PipelineStage::Source,
                        source.name(),
                        candidates.len(),
                        started.elapsed().as_millis()
                    );
                    collected.append(&mut candidates);
                }
                Err(err) => {
                    error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        PipelineStage::Source,
                        source.name(),
                        err,
                        started.elapsed().as_millis()
                    );
                }
            }
        }
        collected
    }

    /// Run all candidate hydrators in parallel and merge results into candidates.
    async fn hydrate(&self, query: &Q, candidates: Vec<C>) -> Vec<C> {
        self.run_hydrators(query, candidates, self.hydrators(), PipelineStage::Hydrator)
            .await
    }

    /// Run post-selection candidate hydrators in parallel and merge results into candidates.
    async fn hydrate_post_selection(&self, query: &Q, candidates: Vec<C>) -> Vec<C> {
        self.run_hydrators(
            query,
            candidates,
            self.post_selection_hydrators(),
            PipelineStage::PostSelectionHydrator,
        )
        .await
    }

    /// Shared helper to hydrate with a provided hydrator list.
    async fn run_hydrators(
        &self,
        query: &Q,
        mut candidates: Vec<C>,
        hydrators: &[Box<dyn Hydrator<Q, C>>],
        stage: PipelineStage,
    ) -> Vec<C> {
        let request_id = query.request_id().to_string();
        let hydrators: Vec<_> = hydrators.iter().filter(|h| h.enable(query)).collect();
        let expected_len = candidates.len();
        let candidates_ref = &candidates;
        let results = join_all(hydrators.iter().map(|hydrator| async move {
            let started = Instant::now();
            (
                hydrator,
                started,
                hydrator.hydrate(query, candidates_ref).await,
            )
        }))
        .await;
        for (hydrator, started, result) in results {
            match result {
                Ok(hydrated) => {
                    if hydrated.len() == expected_len {
                        hydrator.update_all(&mut candidates, hydrated);
                        info!(
                            "request_id={} stage={:?} component={} candidates={} elapsed_ms={}",
                            request_id,
                            stage,
                            hydrator.name(),
                            expected_len,
                            started.elapsed().as_millis()
                        );
                    } else {
                        warn!(
                            "request_id={} stage={:?} component={} skipped: length_mismatch expected={} got={} elapsed_ms={}",
                            request_id,
                            stage,
                            hydrator.name(),
                            expected_len,
                            hydrated.len(),
                            started.elapsed().as_millis()
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        stage,
                        hydrator.name(),
                        err,
                        started.elapsed().as_millis()
                    );
                }
            }
        }
        candidates
    }

    /// Run all filters sequentially. Each filter partitions candidates into kept and removed.
    fn filter(&self, query: &Q, candidates: Vec<C>) -> (Vec<C>, Vec<C>) {
        self.run_filters(query, candidates, self.filters(), PipelineStage::Filter)
    }

    /// Run post-scoring filters sequentially on already-scored candidates.
    fn filter_post_selection(&self, query: &Q, candidates: Vec<C>) -> (Vec<C>, Vec<C>) {
        self.run_filters(
            query,
            candidates,
            self.post_selection_filters(),
            PipelineStage::PostSelectionFilter,
        )
    }

    // Shared helper to run filters sequentially from a provided filter list.
    fn run_filters(
        &self,
        query: &Q,
        mut candidates: Vec<C>,
        filters: &[Box<dyn Filter<Q, C>>],
        stage: PipelineStage,
    ) -> (Vec<C>, Vec<C>) {
        let request_id = query.request_id().to_string();
        let mut all_removed = Vec::new();
        for filter in filters.iter().filter(|f| f.enable(query)) {
            let started = Instant::now();
            let input_count = candidates.len();
            let backup = candidates.clone();
            match filter.filter(query, candidates) {
                Ok(result) => {
                    let removed_count = result.removed.len();
                    candidates = result.kept;
                    all_removed.extend(result.removed);
                    info!(
                        "request_id={} stage={:?} component={} input={} kept={} removed={} elapsed_ms={}",
                        request_id,
                        stage,
                        filter.name(),
                        input_count,
                        candidates.len(),
                        removed_count,
                        started.elapsed().as_millis()
                    );
                }
                Err(err) => {
                    error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        stage,
                        filter.name(),
                        err,
                        started.elapsed().as_millis()
                    );
                    candidates = backup;
                }
            }
        }
        info!(
            "request_id={} stage={:?} kept {}, removed {}",
            request_id,
            stage,
            candidates.len(),
            all_removed.len()
        );
        (candidates, all_removed)
    }

    /// Run all scorers sequentially and apply their results to candidates.
    async fn score(&self, query: &Q, mut candidates: Vec<C>) -> Vec<C> {
        let request_id = query.request_id().to_string();
        let expected_len = candidates.len();
        for scorer in self.scorers().iter().filter(|s| s.enable(query)) {
            let started = Instant::now();
            match scorer.score(query, &candidates).await {
                Ok(scored) => {
                    if scored.len() == expected_len {
                        scorer.update_all(&mut candidates, scored);
                        info!(
                            "request_id={} stage={:?} component={} candidates={} elapsed_ms={}",
                            request_id,
                            PipelineStage::Scorer,
                            scorer.name(),
                            expected_len,
                            started.elapsed().as_millis()
                        );
                    } else {
                        warn!(
                            "request_id={} stage={:?} component={} skipped: length_mismatch expected={} got={} elapsed_ms={}",
                            request_id,
                            PipelineStage::Scorer,
                            scorer.name(),
                            expected_len,
                            scored.len(),
                            started.elapsed().as_millis()
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        PipelineStage::Scorer,
                        scorer.name(),
                        err,
                        started.elapsed().as_millis()
                    );
                }
            }
        }
        candidates
    }

    /// Select (sort/truncate) candidates using the configured selector
    fn select(&self, query: &Q, candidates: Vec<C>) -> SelectResult<C> {
        let started = Instant::now();
        let input_count = candidates.len();
        let result = if self.selector().enable(query) {
            self.selector().select(query, candidates)
        } else {
            SelectResult {
                selected: candidates,
                non_selected: Vec::new(),
            }
        };
        info!(
            "request_id={} stage={:?} component={} input={} selected={} non_selected={} elapsed_ms={}",
            query.request_id(),
            PipelineStage::Selector,
            self.selector().name(),
            input_count,
            result.selected.len(),
            result.non_selected.len(),
            started.elapsed().as_millis()
        );
        result
    }

    // Run all side effects in parallel
    fn run_side_effects(&self, input: Arc<SideEffectInput<Q, C>>) {
        let side_effects = self.side_effects();
        tokio::spawn(async move {
            let request_id = input.query.request_id().to_string();
            let futures = side_effects
                .iter()
                .filter(|side_effect| side_effect.enable(input.query.clone()))
                .map(|side_effect| {
                    let input = Arc::clone(&input);
                    async move {
                        let started = Instant::now();
                        (side_effect.name(), started, side_effect.run(input).await)
                    }
                });
            for (name, started, result) in join_all(futures).await {
                match result {
                    Ok(()) => info!(
                        "request_id={} stage={:?} component={} elapsed_ms={}",
                        request_id,
                        PipelineStage::SideEffect,
                        name,
                        started.elapsed().as_millis()
                    ),
                    Err(error) => error!(
                        "request_id={} stage={:?} component={} failed: {} elapsed_ms={}",
                        request_id,
                        PipelineStage::SideEffect,
                        name,
                        error,
                        started.elapsed().as_millis()
                    ),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_hydrator::QueryHydrator;
    use crate::selector::Selector;
    use crate::source::Source;

    #[derive(Clone, Default)]
    struct TestQuery {
        initial_context: bool,
        dependent_context: bool,
    }

    impl HasRequestId for TestQuery {
        fn request_id(&self) -> &str {
            "test-request"
        }
    }

    struct InitialHydrator;

    #[async_trait]
    impl QueryHydrator<TestQuery> for InitialHydrator {
        async fn hydrate(&self, _query: &TestQuery) -> Result<TestQuery, String> {
            Ok(TestQuery {
                initial_context: true,
                ..Default::default()
            })
        }

        fn update(&self, query: &mut TestQuery, hydrated: TestQuery) {
            query.initial_context = hydrated.initial_context;
        }
    }

    struct DependentHydrator;

    #[async_trait]
    impl QueryHydrator<TestQuery> for DependentHydrator {
        async fn hydrate(&self, query: &TestQuery) -> Result<TestQuery, String> {
            if !query.initial_context {
                return Err("initial context is missing".to_string());
            }
            Ok(TestQuery {
                dependent_context: true,
                ..Default::default()
            })
        }

        fn update(&self, query: &mut TestQuery, hydrated: TestQuery) {
            query.dependent_context = hydrated.dependent_context;
        }
    }

    struct TestSource;

    #[async_trait]
    impl Source<TestQuery, i32> for TestSource {
        async fn get_candidates(&self, _query: &TestQuery) -> Result<Vec<i32>, String> {
            Ok(vec![1, 3, 2])
        }
    }

    struct FailingSource;

    #[async_trait]
    impl Source<TestQuery, i32> for FailingSource {
        async fn get_candidates(&self, _query: &TestQuery) -> Result<Vec<i32>, String> {
            Err("source unavailable".to_string())
        }
    }

    struct FailingFilter;

    impl Filter<TestQuery, i32> for FailingFilter {
        fn filter(
            &self,
            _query: &TestQuery,
            _candidates: Vec<i32>,
        ) -> Result<crate::filter::FilterResult<i32>, String> {
            Err("filter unavailable".to_string())
        }
    }

    struct RemoveAllFilter;

    impl Filter<TestQuery, i32> for RemoveAllFilter {
        fn filter(
            &self,
            _query: &TestQuery,
            candidates: Vec<i32>,
        ) -> Result<crate::filter::FilterResult<i32>, String> {
            Ok(crate::filter::FilterResult {
                kept: Vec::new(),
                removed: candidates,
            })
        }
    }

    struct TestSelector;

    impl Selector<TestQuery, i32> for TestSelector {
        fn score(&self, candidate: &i32) -> f64 {
            *candidate as f64
        }

        fn size(&self) -> Option<usize> {
            Some(2)
        }
    }

    struct TestPipeline {
        initial: Vec<Box<dyn QueryHydrator<TestQuery>>>,
        dependent: Vec<Box<dyn QueryHydrator<TestQuery>>>,
        sources: Vec<Box<dyn Source<TestQuery, i32>>>,
        filters: Vec<Box<dyn Filter<TestQuery, i32>>>,
        side_effects: Arc<Vec<Box<dyn SideEffect<TestQuery, i32>>>>,
        selector: TestSelector,
    }

    impl TestPipeline {
        fn new() -> Self {
            Self {
                initial: vec![Box::new(InitialHydrator)],
                dependent: vec![Box::new(DependentHydrator)],
                sources: vec![Box::new(TestSource)],
                filters: Vec::new(),
                side_effects: Arc::new(Vec::new()),
                selector: TestSelector,
            }
        }
    }

    #[async_trait]
    impl CandidatePipeline<TestQuery, i32> for TestPipeline {
        fn query_hydrators(&self) -> &[Box<dyn QueryHydrator<TestQuery>>] {
            &self.initial
        }

        fn dependent_query_hydrators(&self) -> &[Box<dyn QueryHydrator<TestQuery>>] {
            &self.dependent
        }

        fn sources(&self) -> &[Box<dyn Source<TestQuery, i32>>] {
            &self.sources
        }

        fn hydrators(&self) -> &[Box<dyn Hydrator<TestQuery, i32>>] {
            &[]
        }

        fn filters(&self) -> &[Box<dyn Filter<TestQuery, i32>>] {
            &self.filters
        }

        fn scorers(&self) -> &[Box<dyn Scorer<TestQuery, i32>>] {
            &[]
        }

        fn selector(&self) -> &dyn Selector<TestQuery, i32> {
            &self.selector
        }

        fn post_selection_hydrators(&self) -> &[Box<dyn Hydrator<TestQuery, i32>>] {
            &[]
        }

        fn post_selection_filters(&self) -> &[Box<dyn Filter<TestQuery, i32>>] {
            &[]
        }

        fn side_effects(&self) -> Arc<Vec<Box<dyn SideEffect<TestQuery, i32>>>> {
            Arc::clone(&self.side_effects)
        }

        fn result_size(&self) -> usize {
            2
        }
    }

    #[test]
    fn dependent_hydrators_receive_initial_context() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let result = runtime.block_on(TestPipeline::new().execute(TestQuery::default()));

        assert!(result.query.initial_context);
        assert!(result.query.dependent_context);
        assert_eq!(result.selected_candidates, vec![3, 2]);
    }

    #[test]
    fn pipeline_reports_components_by_execution_stage() {
        let components = TestPipeline::new().components();

        let query_stage = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::QueryHydrator)
            .expect("query stage");
        let dependent_stage = components
            .iter()
            .find(|entry| entry.stage == PipelineStage::DependentQueryHydrator)
            .expect("dependent stage");
        assert_eq!(query_stage.components, vec!["InitialHydrator"]);
        assert_eq!(dependent_stage.components, vec!["DependentHydrator"]);
    }

    #[test]
    fn empty_result_uses_default_query_and_no_candidates() {
        let result = PipelineResult::<TestQuery, i32>::empty();

        assert!(!result.query.initial_context);
        assert!(result.retrieved_candidates.is_empty());
        assert!(result.filtered_candidates.is_empty());
        assert!(result.selected_candidates.is_empty());
    }

    #[test]
    fn source_failure_short_circuits_with_hydrated_query() {
        let mut pipeline = TestPipeline::new();
        pipeline.sources = vec![Box::new(FailingSource)];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(pipeline.execute(TestQuery::default()));

        assert!(result.query.initial_context);
        assert!(result.query.dependent_context);
        assert!(result.retrieved_candidates.is_empty());
        assert!(result.selected_candidates.is_empty());
    }

    #[test]
    fn filter_failure_restores_candidates_for_later_stages() {
        let mut pipeline = TestPipeline::new();
        pipeline.filters = vec![Box::new(FailingFilter)];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(pipeline.execute(TestQuery::default()));

        assert_eq!(result.retrieved_candidates, vec![1, 3, 2]);
        assert!(result.filtered_candidates.is_empty());
        assert_eq!(result.selected_candidates, vec![3, 2]);
    }

    #[test]
    fn filter_clear_records_every_removed_candidate() {
        let mut pipeline = TestPipeline::new();
        pipeline.filters = vec![Box::new(RemoveAllFilter)];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(pipeline.execute(TestQuery::default()));

        assert_eq!(result.retrieved_candidates, vec![1, 3, 2]);
        assert_eq!(result.filtered_candidates, vec![1, 3, 2]);
        assert!(result.selected_candidates.is_empty());
    }
}
