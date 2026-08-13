use crate::candidate_hydrators::tes_hydration_provider::TesHydrationProvider;
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct CoreDataCandidateHydrator {
    provider: Arc<TesHydrationProvider>,
}

impl CoreDataCandidateHydrator {
    pub fn new(provider: Arc<TesHydrationProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for CoreDataCandidateHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        self.provider
            .core_candidates(query, candidates)
            .await
            .as_ref()
            .clone()
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.retweeted_user_id = hydrated.retweeted_user_id;
        candidate.retweeted_tweet_id = hydrated.retweeted_tweet_id;
        candidate.in_reply_to_tweet_id = hydrated.in_reply_to_tweet_id;
        candidate.tweet_text = hydrated.tweet_text;

        // CH-10 is owned here: the public TES adapter returns engagement counts
        // inside core data, so a separate counts hydrator would only re-issue the
        // same batch. Upstream splits them because it has a dedicated counts API.
        candidate.favorite_count = hydrated.favorite_count;
        candidate.reply_count = hydrated.reply_count;
        candidate.repost_count = hydrated.repost_count;
        candidate.quote_count = hydrated.quote_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::tweet_entity_service_client::TESClient;
    use crate::models::candidate_features::{MediaEntities, PureCoreData};
    use std::collections::HashMap;

    struct FakeTESClient;

    #[async_trait]
    impl TESClient for FakeTESClient {
        async fn get_tweet_core_datas(
            &self,
            tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<PureCoreData>>, anyhow::Error> {
            Ok(tweet_ids
                .into_iter()
                .map(|id| {
                    let core = if id == 100 {
                        PureCoreData {
                            author_id: 200,
                            text: "main text".to_string(),
                            quoted_tweet_id: Some(300),
                            quoted_user_id: Some(400),
                            language_code: Some("en".to_string()),
                            favorite_count: Some(12),
                            filtered_topic_ids: vec![10],
                            ..Default::default()
                        }
                    } else {
                        PureCoreData {
                            author_id: 400,
                            text: "quoted text".to_string(),
                            ..Default::default()
                        }
                    };
                    (id, Some(core))
                })
                .collect())
        }

        async fn get_tweet_media_entities(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<MediaEntities>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<u64>,
        ) -> Result<HashMap<u64, Option<u64>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    #[tokio::test]
    async fn owns_core_fields_while_shared_batch_exposes_split_fields() {
        let provider = Arc::new(TesHydrationProvider::new(Arc::new(FakeTESClient)));
        let hydrator = CoreDataCandidateHydrator::new(provider);
        let query = ScoredPostsQuery {
            request_id: "request-1".to_string(),
            ..Default::default()
        };
        let candidates = [PostCandidate {
            tweet_id: 100,
            ..Default::default()
        }];

        let hydrated = hydrator.hydrate(&query, &candidates).await;
        let hydrated = hydrated[0].as_ref().expect("core hydration");
        assert_eq!(hydrated.tweet_text, "main text");
        assert_eq!(hydrated.favorite_count, Some(12));
        assert_eq!(hydrated.quoted_tweet_text, "quoted text");
        assert_eq!(hydrated.language_code.as_deref(), Some("en"));
        assert_eq!(hydrated.filtered_topic_ids, vec![10]);

        let mut candidate = candidates[0].clone();
        hydrator.update(&mut candidate, hydrated.clone());
        assert_eq!(candidate.tweet_text, "main text");
        assert_eq!(candidate.favorite_count, Some(12));
        assert!(candidate.quoted_tweet_text.is_empty());
        assert!(candidate.language_code.is_none());
        assert!(candidate.filtered_topic_ids.is_empty());
    }
}
