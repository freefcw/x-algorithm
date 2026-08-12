use crate::candidate_pipeline::{PipelineCandidate, PipelineQuery};
use crate::util;
use log::warn;
use std::any::type_name_of_val;
use tonic::async_trait;

/// Scorers update candidate fields (like a score field) and run sequentially
#[async_trait]
pub trait Scorer<Q, C>: Send + Sync
where
    Q: PipelineQuery,
    C: PipelineCandidate,
{
    /// Decide if this scorer should run for the given query
    fn enable(&self, _query: &Q) -> bool {
        true
    }

    /// Validate the cardinality contract before the pipeline applies updates.
    async fn run(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>> {
        let scored = self.score(query, candidates).await;
        let expected_len = candidates.len();
        if scored.len() == expected_len {
            scored
        } else {
            let message = format!(
                "Scorer length_mismatch expected={} got={}",
                expected_len,
                scored.len()
            );
            warn!("{}", message);
            vec![Err(message); expected_len]
        }
    }

    /// Score candidates by performing async operations.
    /// Returns one result per input candidate in the same order.
    ///
    /// Dropping candidates in a scorer is not allowed - use a filter stage instead.
    async fn score(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;

    /// Update a single candidate with the scored fields.
    /// Only the fields this scorer is responsible for should be copied.
    fn update(&self, candidate: &mut C, scored: C);

    /// Update only candidates that scored successfully.
    fn update_all(&self, candidates: &mut [C], scored: Vec<Result<C, String>>) {
        for (candidate, scored) in candidates.iter_mut().zip(scored) {
            if let Ok(scored) = scored {
                self.update(candidate, scored);
            }
        }
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

    struct TestScorer {
        return_too_few: bool,
    }

    #[async_trait]
    impl Scorer<TestQuery, i32> for TestScorer {
        async fn score(&self, _query: &TestQuery, candidates: &[i32]) -> Vec<Result<i32, String>> {
            if self.return_too_few {
                return vec![Ok(10)];
            }
            candidates
                .iter()
                .map(|candidate| {
                    if *candidate == 2 {
                        Err("candidate unavailable".to_string())
                    } else {
                        Ok(candidate * 10)
                    }
                })
                .collect()
        }

        fn update(&self, candidate: &mut i32, scored: i32) {
            *candidate = scored;
        }
    }

    #[test]
    fn partial_failure_updates_only_successful_scores() {
        let scorer = TestScorer {
            return_too_few: false,
        };
        let mut candidates = vec![1, 2, 3];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let scored = runtime.block_on(scorer.run(&TestQuery, &candidates));

        scorer.update_all(&mut candidates, scored);

        assert_eq!(candidates, vec![10, 2, 30]);
    }

    #[test]
    fn length_mismatch_becomes_one_error_per_input_candidate() {
        let scorer = TestScorer {
            return_too_few: true,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let scored = runtime.block_on(scorer.run(&TestQuery, &[1, 2, 3]));

        assert_eq!(scored.len(), 3);
        assert!(scored.iter().all(Result::is_err));
    }
}
