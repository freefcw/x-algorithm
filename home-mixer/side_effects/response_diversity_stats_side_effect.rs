pub use crate::candidate_diversity_stats::{CandidateDiversityStats, CandidateDiversityStatsSink};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::util::composition::Composition;
use rand::random;
use std::cmp::Ordering;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::side_effect::{SideEffect, SideEffectInput};

const DEFAULT_SAMPLING_RATE: f64 = 0.05;
const TOP_POSITIONS: usize = 10;

pub trait SamplingDecision: Send + Sync {
    fn should_sample(&self) -> bool;
}

pub struct RandomSampling {
    rate: f64,
}

impl RandomSampling {
    pub fn new(rate: f64) -> Self {
        Self {
            rate: rate.clamp(0.0, 1.0),
        }
    }
}

impl Default for RandomSampling {
    fn default() -> Self {
        Self::new(DEFAULT_SAMPLING_RATE)
    }
}

impl SamplingDecision for RandomSampling {
    fn should_sample(&self) -> bool {
        random::<f64>() < self.rate
    }
}

pub struct ResponseDiversityStatsSideEffect {
    sink: Arc<dyn CandidateDiversityStatsSink>,
    sampler: Arc<dyn SamplingDecision>,
}

impl ResponseDiversityStatsSideEffect {
    pub fn new(sink: Arc<dyn CandidateDiversityStatsSink>) -> Self {
        Self::with_sampler(sink, Arc::new(RandomSampling::default()))
    }

    pub fn with_sampler(
        sink: Arc<dyn CandidateDiversityStatsSink>,
        sampler: Arc<dyn SamplingDecision>,
    ) -> Self {
        Self { sink, sampler }
    }
}

#[async_trait]
impl SideEffect<ScoredPostsQuery, PostCandidate> for ResponseDiversityStatsSideEffect {
    async fn side_effect(
        &self,
        input: Arc<SideEffectInput<ScoredPostsQuery, PostCandidate>>,
    ) -> Result<(), String> {
        if input.selected_candidates.is_empty() || !self.sampler.should_sample() {
            return Ok(());
        }

        let mut final_order: Vec<&PostCandidate> = input.selected_candidates.iter().collect();
        final_order.sort_by(|left, right| {
            let score = |candidate: &PostCandidate| {
                candidate
                    .score
                    .filter(|score| score.is_finite())
                    .unwrap_or(f64::NEG_INFINITY)
            };
            score(right)
                .partial_cmp(&score(left))
                .unwrap_or(Ordering::Equal)
        });
        let top10 = &final_order[..TOP_POSITIONS.min(final_order.len())];

        self.record_stage(&input.query.request_id, "final", &final_order)?;
        self.record_stage(&input.query.request_id, "top10", top10)
    }
}

impl ResponseDiversityStatsSideEffect {
    fn record_stage(
        &self,
        request_id: &str,
        stage: &'static str,
        candidates: &[&PostCandidate],
    ) -> Result<(), String> {
        let authors =
            Composition::from_keys(candidates.iter().map(|candidate| candidate.author_id));
        let sources = Composition::from_keys(
            candidates
                .iter()
                .map(|candidate| candidate.served_type.map(|served_type| served_type as i32)),
        );
        let in_network = candidates
            .iter()
            .filter(|candidate| candidate.in_network == Some(true))
            .count();

        self.sink.record(CandidateDiversityStats {
            request_id: request_id.to_string(),
            stage,
            size: candidates.len(),
            authors,
            sources,
            in_network_share: in_network as f64 / candidates.len() as f64,
        })
    }
}
