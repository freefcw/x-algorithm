use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;
use std::any::type_name_of_val;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;

// A side-effect is an action run that doesn't affect the pipeline result from being returned
#[derive(Clone)]
pub struct SideEffectInput<Q, C> {
    pub query: Arc<Q>,
    pub selected_candidates: Vec<C>,
    pub non_selected_candidates: Vec<C>,
}

#[async_trait]
pub trait SideEffect<Q, C>: Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Decide if this side-effect should be run
    fn enable(&self, _query: Arc<Q>) -> bool {
        true
    }

    /// Keep the wrapper separate from the implementation so instrumentation can
    /// be added without changing every side effect.
    async fn run(&self, input: Arc<SideEffectInput<Q, C>>) -> Result<(), String> {
        self.side_effect(input).await
    }

    async fn side_effect(&self, input: Arc<SideEffectInput<Q, C>>) -> Result<(), String>;

    /// Called once at process shutdown, after the pipeline stopped taking
    /// requests and waited for running side effects: flush or release the
    /// transport behind this side effect within `timeout`. Most side effects
    /// have nothing buffered and keep the default.
    async fn shutdown(&self, _timeout: Duration) {}

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}
