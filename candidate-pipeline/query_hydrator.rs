use std::any::{type_name_of_val, Any};
use tonic::async_trait;

use crate::candidate_pipeline::PipelineQuery;
use crate::util;

#[async_trait]
pub trait QueryHydrator<Q>: Any + Send + Sync
where
    Q: PipelineQuery,
{
    /// Decide if this query hydrator should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Hydrate the query by performing async operations.
    /// Returns a new query with this hydrator's fields populated.
    async fn hydrate(&self, query: &Q) -> Result<Q, String>;

    /// Keep the wrapper separate from the implementation so instrumentation can
    /// be added without changing every query hydrator.
    async fn run(&self, query: &Q) -> Result<Q, String> {
        self.hydrate(query).await
    }

    /// Update the query with the hydrated fields.
    /// Only the fields this hydrator is responsible for should be copied.
    fn update(&self, query: &mut Q, hydrated: Q);

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}
