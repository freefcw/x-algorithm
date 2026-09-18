use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;
use std::any::{type_name_of_val, Any};

pub struct FilterResult<C> {
    pub kept: Vec<C>,
    pub removed: Vec<C>,
}

pub struct FilterFailure<C> {
    pub error: String,
    pub candidates: Vec<C>,
}

/// Filters run sequentially and partition candidates into kept and removed sets.
///
/// The `filter` method matches the upstream public contract. `try_run` is a
/// local extension used to preserve failure isolation for adapters whose remote
/// dependency can fail before producing a partition.
pub trait Filter<Q, C>: Any + Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Decide if this filter should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Filter candidates by evaluating each against some criteria.
    fn filter(&self, query: &Q, candidates: Vec<C>) -> FilterResult<C>;

    /// Standard upstream execution wrapper.
    fn run(&self, query: &Q, candidates: Vec<C>) -> FilterResult<C> {
        self.filter(query, candidates)
    }

    /// Local failure-isolation extension for filters backed by remote services.
    /// A failed filter returns its untouched input so callers do not need to
    /// clone every candidate before attempting the filter.
    fn try_run(&self, query: &Q, candidates: Vec<C>) -> Result<FilterResult<C>, FilterFailure<C>> {
        Ok(self.run(query, candidates))
    }

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}
