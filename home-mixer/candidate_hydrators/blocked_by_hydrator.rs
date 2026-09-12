//! 上游同构的反向屏蔽 Hydrator（CH-09）。
//!
//! 默认不装配：需要真实 `SocialGraphClientOps` Adapter（认证、超时、错误
//! 语义验收）后由装配显式注入。未装配时关系字段保持 `None`，
//! `AuthorSocialgraphFilter` 对这些字段中立。

use crate::clients::socialgraph_client::SocialGraphClientOps;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct BlockedByHydrator {
    pub socialgraph_client: Arc<dyn SocialGraphClientOps>,
}

fn relationship_user_ids(candidates: &[PostCandidate]) -> Vec<crate::models::UserId> {
    let mut seen = HashSet::new();
    let mut user_ids = Vec::new();

    for user_id in candidates.iter().flat_map(|candidate| {
        std::iter::once(candidate.author_id)
            .chain(candidate.retweeted_user_id)
            .chain(candidate.quoted_user_id)
    }) {
        if seen.insert(user_id) {
            user_ids.push(user_id);
        }
    }

    user_ids
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
        let user_ids = relationship_user_ids(candidates);

        let blocked_by_user_ids = match self
            .socialgraph_client
            .check_blocked_by(query.user_id, &user_ids)
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
                let author_blocks_viewer = std::iter::once(candidate.author_id)
                    .chain(candidate.retweeted_user_id)
                    .any(|user_id| blocked_by_user_ids.contains(&user_id));
                let quoted_author_blocks_viewer = candidate
                    .quoted_user_id
                    .map(|user_id| blocked_by_user_ids.contains(&user_id));
                Ok(PostCandidate {
                    author_blocks_viewer: Some(author_blocks_viewer),
                    quoted_author_blocks_viewer,
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.author_blocks_viewer = hydrated.author_blocks_viewer;
        if hydrated.quoted_author_blocks_viewer.is_some() {
            candidate.quoted_author_blocks_viewer = hydrated.quoted_author_blocks_viewer;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    struct FakeSocialGraph {
        blocked_by: HashSet<crate::models::UserId>,
        fail: bool,
        requested_ids: Arc<Mutex<Vec<crate::models::UserId>>>,
    }

    impl FakeSocialGraph {
        fn new(
            blocked_by: HashSet<crate::models::UserId>,
            fail: bool,
        ) -> (Self, Arc<Mutex<Vec<crate::models::UserId>>>) {
            let requested_ids = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    blocked_by,
                    fail,
                    requested_ids: Arc::clone(&requested_ids),
                },
                requested_ids,
            )
        }
    }

    #[async_trait]
    impl SocialGraphClientOps for FakeSocialGraph {
        async fn check_blocked_by(
            &self,
            _viewer_id: crate::models::UserId,
            user_ids: &[crate::models::UserId],
        ) -> Result<HashSet<crate::models::UserId>, String> {
            self.requested_ids
                .lock()
                .unwrap()
                .extend_from_slice(user_ids);
            if self.fail {
                return Err("socialgraph unavailable".to_string());
            }
            Ok(user_ids
                .iter()
                .filter(|id| self.blocked_by.contains(id))
                .copied()
                .collect())
        }
    }

    fn candidates() -> Vec<PostCandidate> {
        vec![
            PostCandidate {
                tweet_id: 1.into(),
                author_id: 100.into(),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                author_id: 200.into(),
                ..Default::default()
            },
        ]
    }

    #[tokio::test]
    async fn marks_only_blocking_authors() {
        let (fake_social_graph, _) =
            FakeSocialGraph::new(HashSet::from([crate::models::uid(200)]), false);
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(fake_social_graph),
        };
        let query = ScoredPostsQuery {
            user_id: 42.into(),
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
        let (fake_social_graph, _) = FakeSocialGraph::new(HashSet::new(), true);
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(fake_social_graph),
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
        let (fake_social_graph, _) = FakeSocialGraph::new(HashSet::new(), false);
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(fake_social_graph),
        };
        let query = ScoredPostsQuery {
            has_cached_posts: true,
            ..Default::default()
        };
        assert!(!hydrator.enable(&query));
    }

    #[tokio::test]
    async fn marks_retweeted_and_quoted_authors_and_deduplicates_lookup_ids() {
        let (fake_social_graph, requested_ids) = FakeSocialGraph::new(
            HashSet::from([crate::models::uid(300), crate::models::uid(400)]),
            false,
        );
        let hydrator = BlockedByHydrator {
            socialgraph_client: Arc::new(fake_social_graph),
        };
        let query = ScoredPostsQuery {
            user_id: 42.into(),
            ..Default::default()
        };
        let candidates = vec![
            PostCandidate {
                author_id: 100.into(),
                retweeted_user_id: Some(300.into()),
                quoted_user_id: Some(400.into()),
                ..Default::default()
            },
            PostCandidate {
                author_id: 100.into(),
                quoted_user_id: Some(400.into()),
                ..Default::default()
            },
        ];

        let hydrated = hydrator.hydrate(&query, &candidates).await;
        let mut candidates = candidates;
        hydrator.update_all(&mut candidates, hydrated);

        assert_eq!(candidates[0].author_blocks_viewer, Some(true));
        assert_eq!(candidates[0].quoted_author_blocks_viewer, Some(true));
        assert_eq!(candidates[1].author_blocks_viewer, Some(false));
        assert_eq!(candidates[1].quoted_author_blocks_viewer, Some(true));
        assert_eq!(
            *requested_ids.lock().unwrap(),
            vec![
                crate::models::uid(100),
                crate::models::uid(300),
                crate::models::uid(400)
            ]
        );
    }
}
