//! 上游同构的反向屏蔽 Hydrator（CH-09）。
//!
//! 默认不装配：需要真实 `SocialGraphClientOps` Adapter（认证、超时、错误
//! 语义验收）后由装配显式注入。未装配时 `author_blocks_viewer` 保持
//! `None`，`AuthorSocialgraphFilter` 对该字段中立。

use crate::clients::socialgraph_client::SocialGraphClientOps;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct BlockedByHydrator {
    pub socialgraph_client: Arc<dyn SocialGraphClientOps>,
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for BlockedByHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let author_ids: Vec<u64> = candidates.iter().map(|x| x.author_id).collect();

        let blocked_by_user_ids = match self
            .socialgraph_client
            .check_blocked_by(query.user_id, &author_ids)
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                return candidates.iter().map(|_| Err(error.clone())).collect();
            }
        };
        candidates
            .iter()
            .map(|candidate| {
                let author_blocks_viewer = blocked_by_user_ids.contains(&candidate.author_id);
                Ok(PostCandidate {
                    author_blocks_viewer: Some(author_blocks_viewer),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.author_blocks_viewer = hydrated.author_blocks_viewer;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    struct FakeSocialGraph {
        blocked_by: HashSet<u64>,
        fail: bool,
    }

    #[async_trait]
    impl SocialGraphClientOps for FakeSocialGraph {
        async fn check_blocked_by(
            &self,
            _viewer_id: u64,
            author_ids: &[u64],
        ) -> Result<HashSet<u64>, String> {
            if self.fail {
                return Err("socialgraph unavailable".to_string());
            }
            Ok(author_ids
                .iter()
                .filter(|id| self.blocked_by.contains(id))
                .copied()
                .collect())
        }
    }

    fn candidates() -> Vec<PostCandidate> {
        vec![
            PostCandidate {
                tweet_id: 1,
                author_id: 100,
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                author_id: 200,
                ..Default::default()
            },
        ]
    }

    #[tokio::test]
    async fn marks_only_blocking_authors() {
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(FakeSocialGraph {
                blocked_by: HashSet::from([200]),
                fail: false,
            }),
        };
        let query = ScoredPostsQuery {
            user_id: 42,
            ..Default::default()
        };

        let mut candidates = candidates();
        let hydrated = hydrator.hydrate(&query, &candidates).await;
        assert_eq!(hydrated.len(), 2);
        hydrator.update_all(&mut candidates, hydrated);

        assert_eq!(candidates[0].author_blocks_viewer, Some(false));
        assert_eq!(candidates[1].author_blocks_viewer, Some(true));
    }

    #[tokio::test]
    async fn client_failure_preserves_cardinality_and_leaves_candidates_neutral() {
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(FakeSocialGraph {
                blocked_by: HashSet::new(),
                fail: true,
            }),
        };
        let query = ScoredPostsQuery::default();

        let mut candidates = candidates();
        let hydrated = hydrator.hydrate(&query, &candidates).await;
        assert_eq!(hydrated.len(), 2);
        assert!(hydrated.iter().all(Result::is_err));
        hydrator.update_all(&mut candidates, hydrated);

        assert_eq!(candidates[0].author_blocks_viewer, None);
        assert_eq!(candidates[1].author_blocks_viewer, None);
    }

    #[tokio::test]
    async fn disabled_for_cached_posts_requests() {
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(FakeSocialGraph {
                blocked_by: HashSet::new(),
                fail: false,
            }),
        };
        let query = ScoredPostsQuery {
            has_cached_posts: true,
            ..Default::default()
        };
        assert!(!hydrator.enable(&query));
    }
}
