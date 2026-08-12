use std::any::{type_name_of_val, Any};
use tonic::async_trait;

use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;

#[async_trait]
pub trait Source<Q, C>: Any + Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Decide if this source should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Load candidates for a query.
    async fn source(&self, query: &Q) -> Result<Vec<C>, String>;

    /// Keep the wrapper separate from the implementation so instrumentation can
    /// be added without changing every source.
    async fn run(&self, query: &Q) -> Result<Vec<C>, String> {
        self.source(query).await
    }

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}
