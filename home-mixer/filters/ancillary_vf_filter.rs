use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct AncillaryVFFilter;

#[async_trait]
impl Filter<ScoredPostsQuery, PostCandidate> for AncillaryVFFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> Result<FilterResult<PostCandidate>, String> {
        let (removed, kept) = candidates
            .into_iter()
            .partition(|candidate| candidate.drop_ancillary_posts == Some(true));
        Ok(FilterResult { kept, removed })
    }
}
