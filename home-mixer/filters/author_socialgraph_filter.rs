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

        let viewer_blocks = |user_id: Option<crate::models::UserId>| {
            user_id
                .filter(|id| !id.is_nil())
                .is_some_and(|id| viewer_blocked_user_ids.contains(&id))
        };

        let mut kept: Vec<PostCandidate> = Vec::new();
        let mut removed: Vec<PostCandidate> = Vec::new();

        for candidate in candidates {
            // 与上游一致的候选级信号；未装配 BlockedByHydrator 时保持中立。
            let author_blocks_viewer = candidate.author_blocks_viewer.unwrap_or(false);
            let quoted_author_blocks_viewer =
                candidate.quoted_author_blocks_viewer.unwrap_or(false);
            let viewer_blocks_quoted_author = viewer_blocks(candidate.quoted_user_id);
            let viewer_blocks_retweeted_user = viewer_blocks(candidate.retweeted_user_id);

            // 签名关系存储无法表示的作者 ID 保持中立。
            let author_id = candidate.author_id;
            let (muted, blocked) = if author_id.is_nil() {
                (false, false)
            } else {
                (
                    viewer_muted_user_ids.contains(&author_id),
                    viewer_blocked_user_ids.contains(&author_id)
                        || blocked_by_user_ids.contains(&author_id),
                )
            };

            if muted
                || blocked
                || author_blocks_viewer
                || quoted_author_blocks_viewer
                || viewer_blocks_quoted_author
                || viewer_blocks_retweeted_user
            {
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
                blocked_user_ids: vec![200.into()],
                blocked_by_user_ids: vec![400.into()],
                muted_user_ids: vec![300.into()],
                ..Default::default()
            },
            ..Default::default()
        };

        let candidates = vec![
            PostCandidate {
                author_id: 100.into(),
                ..Default::default()
            }, // clear
            PostCandidate {
                author_id: 200.into(),
                ..Default::default()
            }, // blocked
            PostCandidate {
                author_id: 300.into(),
                ..Default::default()
            }, // muted
            PostCandidate {
                author_id: 400.into(),
                ..Default::default()
            }, // author blocked viewer
            PostCandidate {
                author_id: crate::models::uid(u64::MAX),
                ..Default::default()
            }, // not representable by the signed relationship store, keep neutral
        ];

        let result = filter.filter(&query, candidates);
        assert_eq!(result.kept.len(), 2);
        assert_eq!(result.kept[0].author_id, crate::models::uid(100));
        assert_eq!(result.kept[1].author_id, crate::models::uid(u64::MAX));
        assert_eq!(result.removed.len(), 3);
    }

    #[test]
    fn candidate_level_signals_follow_upstream_semantics() {
        let filter = AuthorSocialgraphFilter;
        let query = ScoredPostsQuery {
            user_features: UserFeatures {
                blocked_user_ids: vec![900.into()],
                ..Default::default()
            },
            ..Default::default()
        };

        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                author_id: 100.into(),
                author_blocks_viewer: Some(true),
                ..Default::default()
            }, // author blocks viewer (hydrated signal)
            PostCandidate {
                tweet_id: 2.into(),
                author_id: 101.into(),
                quoted_author_blocks_viewer: Some(true),
                ..Default::default()
            }, // quoted author blocks viewer
            PostCandidate {
                tweet_id: 3.into(),
                author_id: 102.into(),
                quoted_user_id: Some(900.into()),
                ..Default::default()
            }, // viewer blocks quoted author
            PostCandidate {
                tweet_id: 4.into(),
                author_id: 103.into(),
                retweeted_user_id: Some(900.into()),
                ..Default::default()
            }, // viewer blocks retweeted user
            PostCandidate {
                tweet_id: 5.into(),
                author_id: 104.into(),
                author_blocks_viewer: Some(false),
                ..Default::default()
            }, // explicit negative stays
            PostCandidate {
                tweet_id: 6.into(),
                author_id: 105.into(),
                ..Default::default()
            }, // unhydrated stays neutral
        ];

        let result = filter.filter(&query, candidates);
        let kept_ids: Vec<_> = result.kept.iter().map(|c| c.tweet_id).collect();
        assert_eq!(kept_ids, vec![crate::models::pid(5), crate::models::pid(6)]);
        assert_eq!(result.removed.len(), 4);
    }
}
