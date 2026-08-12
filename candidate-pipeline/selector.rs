use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;
use std::any::type_name_of_val;

#[derive(Clone, Debug, PartialEq)]
pub struct SelectResult<C> {
    pub selected: Vec<C>,
    pub non_selected: Vec<C>,
}

impl<C> SelectResult<C> {
    pub fn len(&self) -> usize {
        self.selected.len()
    }

    pub fn is_empty(&self) -> bool {
        self.selected.is_empty() && self.non_selected.is_empty()
    }
}

pub trait Selector<Q, C>: Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Default selection: sort and truncate based on provided configs
    fn select(&self, _query: &Q, candidates: Vec<C>) -> SelectResult<C> {
        let mut sorted = self.sort(candidates);
        let non_selected = if let Some(limit) = self.size() {
            sorted.split_off(limit.min(sorted.len()))
        } else {
            Vec::new()
        };
        SelectResult {
            selected: sorted,
            non_selected,
        }
    }

    /// Decide if this selector should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Keep the wrapper separate from the implementation so instrumentation can
    /// be added without changing every selector.
    fn run(&self, query: &Q, candidates: Vec<C>) -> SelectResult<C> {
        self.select(query, candidates)
    }

    /// Extract the score from a candidate to use for sorting.
    fn score(&self, candidate: &C) -> f64;

    /// Sort candidates by their scores in descending order.
    fn sort(&self, candidates: Vec<C>) -> Vec<C> {
        let mut sorted = candidates;
        sorted.sort_by(|a, b| {
            self.score(b)
                .partial_cmp(&self.score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted
    }

    /// Optionally provide a size to select. Defaults to no truncation if not overridden.
    fn size(&self) -> Option<usize> {
        None
    }

    fn name(&self) -> &'static str {
        util::short_type_name(type_name_of_val(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::HasRequestId;

    #[derive(Clone)]
    struct TestQuery;

    impl HasRequestId for TestQuery {
        fn request_id(&self) -> &str {
            "test-request"
        }
    }

    struct TopTwo;

    impl Selector<TestQuery, i32> for TopTwo {
        fn score(&self, candidate: &i32) -> f64 {
            *candidate as f64
        }

        fn size(&self) -> Option<usize> {
            Some(2)
        }
    }

    #[test]
    fn selection_preserves_candidates_below_the_limit() {
        let result = TopTwo.run(&TestQuery, vec![1, 4, 3, 2]);

        assert_eq!(result.selected, vec![4, 3]);
        assert_eq!(result.non_selected, vec![2, 1]);
    }
}
