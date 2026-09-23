use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::collections::HashMap;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct DropDuplicatesFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for DropDuplicatesFilter {
    fn filter(
        &self,
        _query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let mut kept_index: HashMap<crate::models::PostId, usize> =
            HashMap::with_capacity(candidates.len());
        let mut kept: Vec<PostCandidate> = Vec::with_capacity(candidates.len());
        let mut removed = Vec::new();

        for mut candidate in candidates {
            if let Some(index) = kept_index.get(&candidate.tweet_id).copied() {
                kept[index]
                    .retrieval_sources
                    .append(&mut candidate.retrieval_sources);
                removed.push(candidate);
            } else {
                kept_index.insert(candidate.tweet_id, kept.len());
                kept.push(candidate);
            }
        }

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate::RetrievalSource;
    use x_algorithm_proto::home_mixer as pb;

    #[test]
    fn duplicate_candidates_merge_all_retrieval_sources_into_first_candidate() {
        let candidate = |tweet_id, served_type| PostCandidate {
            tweet_id,
            author_id: crate::models::uid(100),
            served_type: Some(served_type),
            retrieval_sources: vec![RetrievalSource::from_served_type(served_type)],
            ..Default::default()
        };
        let first = candidate(10, pb::ServedType::ForYouPhoenixRetrieval);
        let duplicate = candidate(10, pb::ServedType::ForYouPhoenixRetrievalMoe);
        let other = candidate(11, pb::ServedType::ForYouInNetwork);

        let result = DropDuplicatesFilter.filter(
            &ScoredPostsQuery::test_default(),
            vec![first, duplicate, other],
        );

        assert_eq!(result.kept.len(), 2);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(
            result.kept[0]
                .retrieval_sources
                .iter()
                .map(|source| source.served_type)
                .collect::<Vec<_>>(),
            vec![
                pb::ServedType::ForYouPhoenixRetrieval,
                pb::ServedType::ForYouPhoenixRetrievalMoe,
            ]
        );
        assert!(result.removed[0].retrieval_sources.is_empty());
    }
}
