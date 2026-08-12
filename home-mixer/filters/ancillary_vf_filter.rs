use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct AncillaryVFFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for AncillaryVFFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let (removed, kept) = candidates
            .into_iter()
            .partition(|candidate| candidate.drop_ancillary_posts == Some(true));
        FilterResult { kept, removed }
    }
}
