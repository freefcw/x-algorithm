use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::util::bloom_filter::BloomFilter;
use crate::util::candidates_util::get_related_post_ids;
use std::collections::HashSet;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// Filter out previously seen posts using a Bloom Filter and
/// the seen IDs sent in the request directly from the client
pub struct PreviouslySeenPostsFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for PreviouslySeenPostsFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let bloom_filters = query
            .bloom_filter_entries
            .iter()
            .map(BloomFilter::from_entry)
            .collect::<Vec<_>>();

        let seen_ids = query.seen_ids.iter().copied().collect::<HashSet<_>>();
        let (removed, kept): (Vec<_>, Vec<_>) = candidates.into_iter().partition(|c| {
            get_related_post_ids(c).iter().any(|&post_id| {
                seen_ids.contains(&post_id)
                    || bloom_filters
                        .iter()
                        .any(|filter| filter.may_contain(&post_id.to_be_bytes()))
            })
        });

        FilterResult { kept, removed }
    }
}
