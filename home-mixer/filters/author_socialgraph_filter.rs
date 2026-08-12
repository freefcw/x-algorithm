use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

// Remove candidates that are blocked or muted by the viewer
pub struct AuthorSocialgraphFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for AuthorSocialgraphFilter {
    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let viewer_blocked_user_ids = query.user_features.blocked_user_ids.clone();
        let blocked_by_user_ids = query.user_features.blocked_by_user_ids.clone();
        let viewer_muted_user_ids = query.user_features.muted_user_ids.clone();

        if viewer_blocked_user_ids.is_empty()
            && blocked_by_user_ids.is_empty()
            && viewer_muted_user_ids.is_empty()
        {
            return FilterResult {
                kept: candidates,
                removed: Vec::new(),
            };
        }

        let mut kept: Vec<PostCandidate> = Vec::new();
        let mut removed: Vec<PostCandidate> = Vec::new();

        for candidate in candidates {
            let Ok(author_id) = i64::try_from(candidate.author_id) else {
                kept.push(candidate);
                continue;
            };
            let muted = viewer_muted_user_ids.contains(&author_id);
            let blocked = viewer_blocked_user_ids.contains(&author_id)
                || blocked_by_user_ids.contains(&author_id);
            if muted || blocked {
                removed.push(candidate);
            } else {
                kept.push(candidate);
            }
        }

        FilterResult { kept, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_features::UserFeatures;

    #[test]
    fn test_socialgraph_filter() {
        let filter = AuthorSocialgraphFilter;
        let query = ScoredPostsQuery {
            user_features: UserFeatures {
                blocked_user_ids: vec![-1, 200],
                blocked_by_user_ids: vec![400],
                muted_user_ids: vec![300],
                ..Default::default()
            },
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
            PostCandidate {
                author_id: u64::MAX,
                ..Default::default()
            }, // not representable by the signed relationship store, keep neutral
        ];

        let result = filter.filter(&query, candidates);
        assert_eq!(result.kept.len(), 2);
        assert_eq!(result.kept[0].author_id, 100);
        assert_eq!(result.kept[1].author_id, u64::MAX);
        assert_eq!(result.removed.len(), 3);
    }
}
