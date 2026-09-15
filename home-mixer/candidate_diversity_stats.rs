//! Sampled composition of the inner Phoenix pipeline's selected candidates.
//! These records precede the outer ForYou blender and are not final-feed stats.

use crate::util::composition::Composition;
use log::info;

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateDiversityStats {
    pub request_id: String,
    pub stage: &'static str,
    pub size: usize,
    pub authors: Composition,
    pub sources: Composition,
    pub in_network_share: f64,
}

pub trait CandidateDiversityStatsSink: Send + Sync {
    /// Record locally without blocking I/O; external adapters should enqueue.
    fn record(&self, stats: CandidateDiversityStats) -> Result<(), String>;
}

pub struct LoggingCandidateDiversityStats;

impl CandidateDiversityStatsSink for LoggingCandidateDiversityStats {
    fn record(&self, stats: CandidateDiversityStats) -> Result<(), String> {
        info!(
            "ResponseDiversity request_id={} stage={} size={} unique_authors={} unique_author_ratio={} max_author_share={} author_hhi={} author_entropy_norm={} unique_sources={} source_entropy_norm={} in_network_share={}",
            stats.request_id,
            stats.stage,
            stats.size,
            stats.authors.unique,
            stats.authors.unique_ratio(),
            stats.authors.max_share(),
            stats.authors.hhi,
            stats.authors.entropy_norm,
            stats.sources.unique,
            stats.sources.entropy_norm,
            stats.in_network_share,
        );
        Ok(())
    }
}
