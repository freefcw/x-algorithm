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
        let user_ids_to_fetch = candidates
            .iter()
            .flat_map(|candidate| {
                std::iter::once(candidate.author_id).chain(candidate.retweeted_user_id)
            })
            .filter(|user_id| seen_user_ids.insert(*user_id))
            .collect();

        let users =
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
            };

        let mut hydrated_candidates = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            let user = users
                .get(&candidate.author_id)
                .and_then(|user| user.as_ref());
            let user_counts = user.and_then(|user| user.user.as_ref().map(|u| &u.counts));
            let user_profile = user.and_then(|user| user.user.as_ref().map(|u| &u.profile));

            let author_followers_count =
                user_counts.and_then(|counts| i32::try_from(counts.followers_count).ok());
            let author_screen_name: Option<String> = user_profile.map(|x| x.screen_name.clone());

            let retweet_user = candidate
                .retweeted_user_id
                .and_then(|retweeted_user_id| users.get(&retweeted_user_id))
                .and_then(|user| user.as_ref());
            let retweet_profile =
                retweet_user.and_then(|user| user.user.as_ref().map(|u| &u.profile));
            let retweeted_screen_name: Option<String> =
                retweet_profile.map(|x| x.screen_name.clone());

            let hydrated = PostCandidate {
                author_followers_count,
                author_screen_name,
                retweeted_screen_name,
                ..Default::default()
            };
            hydrated_candidates.push(Ok(hydrated));
        }

        hydrated_candidates
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.author_followers_count = hydrated.author_followers_count;
        candidate.author_screen_name = hydrated.author_screen_name;
        candidate.retweeted_screen_name = hydrated.retweeted_screen_name;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate_features::GizmoduckUserResult;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingGizmoduckClient {
        requested_user_ids: Mutex<Vec<u64>>,
    }

    #[tonic::async_trait]
    impl GizmoduckClient for RecordingGizmoduckClient {
        async fn get_users(
            &self,
            user_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<GizmoduckUserResult>>, anyhow::Error> {
            *self.requested_user_ids.lock().expect("request lock") = user_ids.clone();
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
    async fn fetches_each_author_and_retweet_author_once() {
        let client = Arc::new(RecordingGizmoduckClient::default());
        let hydrator = GizmoduckCandidateHydrator::new(client.clone()).await;
        let candidates = vec![
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

        let hydrated = hydrator
            .hydrate(&ScoredPostsQuery::default(), &candidates)
            .await;

        assert_eq!(hydrated.len(), candidates.len());
        assert_eq!(
            *client.requested_user_ids.lock().expect("request lock"),
            vec![1, 2, 3]
        );
    }
}
