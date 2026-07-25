use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use tonic::async_trait;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

// Remove candidates that are blocked or muted by the viewer
pub struct AuthorSocialgraphFilter;

#[async_trait]
impl Filter<ScoredPostsQuery, PostCandidate> for AuthorSocialgraphFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> Result<FilterResult<PostCandidate>, String> {
        let viewer_blocked_user_ids = query.user_features.blocked_user_ids.clone();
        let blocked_by_user_ids = query.user_features.blocked_by_user_ids.clone();
        let viewer_muted_user_ids = query.user_features.muted_user_ids.clone();

        if viewer_blocked_user_ids.is_empty()
            && blocked_by_user_ids.is_empty()
            && viewer_muted_user_ids.is_empty()
        {
            return Ok(FilterResult {
                kept: candidates,
                removed: Vec::new(),
            });
        }

        let mut kept: Vec<PostCandidate> = Vec::new();
        let mut removed: Vec<PostCandidate> = Vec::new();

        for candidate in candidates {
            let author_id = candidate.author_id as i64;
            let muted = viewer_muted_user_ids.contains(&author_id);
            let blocked = viewer_blocked_user_ids.contains(&author_id)
                || blocked_by_user_ids.contains(&author_id);
            if muted || blocked {
                removed.push(candidate);
            } else {
                kept.push(candidate);
            }
        }

        Ok(FilterResult { kept, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::query_features::UserFeatures;

    #[test]
    fn test_socialgraph_filter() {
        let filter = AuthorSocialgraphFilter;
        let mut query = ScoredPostsQuery::default();
        query.user_features = UserFeatures {
            blocked_user_ids: vec![200],
            blocked_by_user_ids: vec![400],
            muted_user_ids: vec![300],
            ..Default::default()
        };

        let candidates = vec![
            PostCandidate {
                author_id: 100,
                ..Default::default()
            }, // clear
            PostCandidate {
                author_id: 200,
                ..Default::default()
            }, // blocked
            PostCandidate {
                author_id: 300,
                ..Default::default()
            }, // muted
            PostCandidate {
                author_id: 400,
                ..Default::default()
            }, // author blocked viewer
        ];

        let result = filter.filter(&query, candidates).unwrap();
        assert_eq!(result.kept.len(), 1);
        assert_eq!(result.kept[0].author_id, 100);
        assert_eq!(result.removed.len(), 3);
    }
}
