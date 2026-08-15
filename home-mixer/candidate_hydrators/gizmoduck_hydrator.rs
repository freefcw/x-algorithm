use crate::clients::gizmoduck_client::GizmoduckClient;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct GizmoduckCandidateHydrator {
    pub gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>,
    request_timeout: Duration,
}

impl GizmoduckCandidateHydrator {
    pub async fn new(gizmoduck_client: Arc<dyn GizmoduckClient + Send + Sync>) -> Self {
        Self {
            gizmoduck_client,
            request_timeout: Duration::from_millis(params::GIZMODOUCK_REQUEST_TIMEOUT_MS),
        }
    }

    #[cfg(test)]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for GizmoduckCandidateHydrator {
    async fn hydrate(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let client = &self.gizmoduck_client;

        let mut seen_user_ids = HashSet::new();
        let user_ids_to_fetch: Vec<u64> = candidates
            .iter()
            .flat_map(|candidate| {
                let author_id = (candidate.author_profile_looked_up_for_user_id
                    != Some(candidate.author_id))
                .then_some(candidate.author_id);
                let retweeted_user_id = candidate.retweeted_user_id.filter(|user_id| {
                    candidate.retweeted_profile_looked_up_for_user_id != Some(*user_id)
                });
                author_id.into_iter().chain(retweeted_user_id)
            })
            .filter(|user_id| seen_user_ids.insert(*user_id))
            .collect();

        let users = if user_ids_to_fetch.is_empty() {
            Default::default()
        } else {
            match tokio::time::timeout(self.request_timeout, client.get_users(user_ids_to_fetch))
                .await
            {
                Ok(Ok(users)) => users,
                Ok(Err(error)) => return vec![Err(error.to_string()); candidates.len()],
                Err(_) => {
                    return vec![
                        Err(format!(
                            "Gizmoduck profile request timed out after {}ms",
                            self.request_timeout.as_millis()
                        ));
                        candidates.len()
                    ];
                }
            }
        };

        let mut hydrated_candidates = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            let author_id_to_update = (candidate.author_profile_looked_up_for_user_id
                != Some(candidate.author_id))
            .then_some(candidate.author_id);
            let user = author_id_to_update
                .and_then(|author_id| users.get(&author_id))
                .and_then(|user| user.as_ref());
            let user_counts = user.and_then(|user| user.user.as_ref().map(|u| &u.counts));
            let user_profile = user.and_then(|user| user.user.as_ref().map(|u| &u.profile));

            let author_followers_count =
                user_counts.and_then(|counts| i32::try_from(counts.followers_count).ok());
            let author_screen_name = user_profile.map(|profile| profile.screen_name.clone());

            let retweeted_user_id_to_update = candidate.retweeted_user_id.filter(|user_id| {
                candidate.retweeted_profile_looked_up_for_user_id != Some(*user_id)
            });
            let retweet_user = retweeted_user_id_to_update
                .and_then(|retweeted_user_id| users.get(&retweeted_user_id))
                .and_then(|user| user.as_ref());
            let retweet_profile =
                retweet_user.and_then(|user| user.user.as_ref().map(|u| &u.profile));
            let retweeted_screen_name = retweet_profile.map(|profile| profile.screen_name.clone());

            hydrated_candidates.push(Ok(PostCandidate {
                author_followers_count,
                author_screen_name,
                retweeted_screen_name,
                author_profile_looked_up_for_user_id: author_id_to_update,
                retweeted_profile_looked_up_for_user_id: retweeted_user_id_to_update,
                ..Default::default()
            }));
        }

        hydrated_candidates
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        if let Some(user_id) = hydrated.author_profile_looked_up_for_user_id {
            if candidate.author_id == user_id {
                candidate.author_followers_count = hydrated.author_followers_count;
                candidate.author_screen_name = hydrated.author_screen_name;
                candidate.author_profile_looked_up_for_user_id = Some(user_id);
            }
        }
        if let Some(user_id) = hydrated.retweeted_profile_looked_up_for_user_id {
            if candidate.retweeted_user_id == Some(user_id) {
                candidate.retweeted_screen_name = hydrated.retweeted_screen_name;
                candidate.retweeted_profile_looked_up_for_user_id = Some(user_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate_features::GizmoduckUserResult;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingGizmoduckClient {
        requests: Mutex<Vec<Vec<u64>>>,
    }

    #[tonic::async_trait]
    impl GizmoduckClient for RecordingGizmoduckClient {
        async fn get_users(
            &self,
            user_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
            self.requests
                .lock()
                .expect("request lock")
                .push(user_ids.clone());
            Ok(user_ids
                .into_iter()
                .map(|user_id| (user_id, None))
                .collect())
        }
    }

    struct SlowGizmoduckClient;

    #[tonic::async_trait]
    impl GizmoduckClient for SlowGizmoduckClient {
        async fn get_users(
            &self,
            _user_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(HashMap::new())
        }
    }

    #[derive(Default)]
    struct FailOnceGizmoduckClient {
        calls: AtomicUsize,
    }

    #[tonic::async_trait]
    impl GizmoduckClient for FailOnceGizmoduckClient {
        async fn get_users(
            &self,
            user_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(anyhow::anyhow!("temporary profile failure"));
            }
            Ok(user_ids
                .into_iter()
                .map(|user_id| (user_id, None))
                .collect())
        }
    }

    async fn hydrate_and_update(
        hydrator: &GizmoduckCandidateHydrator,
        candidates: &mut [PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let hydrated = hydrator
            .hydrate(&ScoredPostsQuery::default(), candidates)
            .await;
        hydrator.update_all(candidates, hydrated.clone());
        hydrated
    }

    #[tokio::test]
    async fn slow_profile_lookup_is_bounded() {
        let hydrator = GizmoduckCandidateHydrator::new(Arc::new(SlowGizmoduckClient))
            .await
            .with_request_timeout(Duration::from_millis(1));
        let candidates = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        let result = hydrator
            .hydrate(&ScoredPostsQuery::default(), &candidates)
            .await;

        assert!(result[0]
            .as_ref()
            .expect_err("slow profile lookup must time out")
            .contains("timed out"));
    }

    #[tokio::test]
    async fn fetches_each_author_and_retweet_author_once_per_batch() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = vec![
            PostCandidate {
                author_id: 1,
                retweeted_user_id: Some(2),
                ..Default::default()
            },
            PostCandidate {
                author_id: 2,
                retweeted_user_id: Some(3),
                ..Default::default()
            },
            PostCandidate {
                author_id: 1,
                ..Default::default()
            },
        ];

        let hydrated = hydrate_and_update(&hydrator, &mut candidates).await;

        assert_eq!(hydrated.len(), candidates.len());
        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1, 2, 3]]
        );
        assert_eq!(candidates[0].author_profile_looked_up_for_user_id, Some(1));
        assert_eq!(
            candidates[0].retweeted_profile_looked_up_for_user_id,
            Some(2)
        );
    }

    #[tokio::test]
    async fn post_selection_fetches_only_newly_discovered_retweet_authors() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = vec![
            PostCandidate {
                author_id: 1,
                ..Default::default()
            },
            PostCandidate {
                author_id: 2,
                ..Default::default()
            },
        ];

        hydrate_and_update(&hydrator, &mut candidates).await;
        candidates[0].retweeted_user_id = Some(3);
        hydrate_and_update(&hydrator, &mut candidates).await;

        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1, 2], vec![3]]
        );
        assert_eq!(
            candidates[0].retweeted_profile_looked_up_for_user_id,
            Some(3)
        );
    }

    #[tokio::test]
    async fn missing_profiles_are_not_fetched_again_within_the_request() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        hydrate_and_update(&hydrator, &mut candidates).await;
        hydrate_and_update(&hydrator, &mut candidates).await;

        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1]]
        );
    }

    #[tokio::test]
    async fn concurrent_author_change_drops_the_stale_profile_result() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        let hydrated = hydrator
            .hydrate(&ScoredPostsQuery::default(), &candidates)
            .await;
        candidates[0].author_id = 2;
        hydrator.update_all(&mut candidates, hydrated);

        assert_eq!(candidates[0].author_profile_looked_up_for_user_id, None);
        hydrate_and_update(&hydrator, &mut candidates).await;
        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1], vec![2]]
        );
    }

    #[tokio::test]
    async fn changed_author_id_invalidates_the_request_local_lookup() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        hydrate_and_update(&hydrator, &mut candidates).await;
        candidates[0].author_id = 2;
        hydrate_and_update(&hydrator, &mut candidates).await;

        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1], vec![2]]
        );
        assert_eq!(candidates[0].author_profile_looked_up_for_user_id, Some(2));
    }

    #[tokio::test]
    async fn failed_profile_requests_can_be_retried() {
        let client = Arc::new(FailOnceGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut candidates = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        let failed = hydrate_and_update(&hydrator, &mut candidates).await;
        assert!(failed[0].is_err());
        assert_eq!(candidates[0].author_profile_looked_up_for_user_id, None);

        let retried = hydrate_and_update(&hydrator, &mut candidates).await;
        assert!(retried[0].is_ok());
        assert_eq!(candidates[0].author_profile_looked_up_for_user_id, Some(1));
        assert_eq!(client.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn lookup_state_does_not_cross_candidate_requests() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let mut first_request = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];
        let mut second_request = [PostCandidate {
            author_id: 1,
            ..Default::default()
        }];

        hydrate_and_update(&hydrator, &mut first_request).await;
        hydrate_and_update(&hydrator, &mut second_request).await;

        assert_eq!(
            *client.requests.lock().expect("request lock"),
            vec![vec![1], vec![1]]
        );
    }
}
