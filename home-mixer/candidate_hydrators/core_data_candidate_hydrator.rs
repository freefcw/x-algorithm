use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::tweet_entity_service_client::TESClient;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tonic::async_trait;
use xai_candidate_pipeline::hydrator::Hydrator;

pub struct CoreDataCandidateHydrator {
    pub tes_client: Arc<dyn TESClient + Send + Sync>,
}

impl CoreDataCandidateHydrator {
    pub async fn new(tes_client: Arc<dyn TESClient + Send + Sync>) -> Self {
        Self { tes_client }
    }
}

#[async_trait]
impl Hydrator<ScoredPostsQuery, PostCandidate> for CoreDataCandidateHydrator {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.has_cached_posts
    }

    async fn hydrate(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Result<Vec<PostCandidate>, String> {
        let tweet_ids = candidates
            .iter()
            .map(|candidate| candidate.tweet_id)
            .collect::<Vec<_>>();
        let core_by_tweet = self
            .tes_client
            .get_tweet_core_datas(tweet_ids.clone())
            .await
            .map_err(|error| error.to_string())?;

        let quoted_ids = core_by_tweet
            .values()
            .filter_map(|core| core.as_ref()?.quoted_tweet_id)
            .filter_map(|id| i64::try_from(id).ok())
            .collect::<HashSet<_>>();
        let quoted_core_by_tweet = if quoted_ids.is_empty() {
            HashMap::new()
        } else {
            self.tes_client
                .get_tweet_core_datas(quoted_ids.into_iter().collect())
                .await
                .map_err(|error| error.to_string())?
        };

        Ok(tweet_ids
            .into_iter()
            .map(|tweet_id| {
                let core = core_by_tweet.get(&tweet_id).and_then(Option::as_ref);
                let quoted_text = core
                    .and_then(|value| value.quoted_tweet_id)
                    .and_then(|id| i64::try_from(id).ok())
                    .and_then(|id| quoted_core_by_tweet.get(&id))
                    .and_then(Option::as_ref)
                    .map(|quoted| quoted.text.clone())
                    .unwrap_or_default();

                PostCandidate {
                    author_id: core.map(|value| value.author_id).unwrap_or_default(),
                    tweet_text: core.map(|value| value.text.clone()).unwrap_or_default(),
                    quoted_tweet_text: quoted_text,
                    retweeted_user_id: core.and_then(|value| value.source_user_id),
                    retweeted_tweet_id: core.and_then(|value| value.source_tweet_id),
                    quoted_tweet_id: core.and_then(|value| value.quoted_tweet_id),
                    quoted_user_id: core.and_then(|value| value.quoted_user_id),
                    in_reply_to_tweet_id: core.and_then(|value| value.in_reply_to_tweet_id),
                    language_code: core.and_then(|value| value.language_code.clone()),
                    favorite_count: core.and_then(|value| value.favorite_count),
                    reply_count: core.and_then(|value| value.reply_count),
                    repost_count: core.and_then(|value| value.repost_count),
                    quote_count: core.and_then(|value| value.quote_count),
                    filtered_topic_ids: core
                        .map(|value| value.filtered_topic_ids.clone())
                        .unwrap_or_default(),
                    unfiltered_topic_ids: core
                        .map(|value| value.unfiltered_topic_ids.clone())
                        .unwrap_or_default(),
                    ..Default::default()
                }
            })
            .collect())
    }

    fn update(&self, candidate: &mut PostCandidate, hydrated: PostCandidate) {
        candidate.retweeted_user_id = hydrated.retweeted_user_id;
        candidate.retweeted_tweet_id = hydrated.retweeted_tweet_id;
        candidate.quoted_tweet_id = hydrated.quoted_tweet_id;
        candidate.quoted_user_id = hydrated.quoted_user_id;
        candidate.in_reply_to_tweet_id = hydrated.in_reply_to_tweet_id;
        candidate.tweet_text = hydrated.tweet_text;
        candidate.quoted_tweet_text = hydrated.quoted_tweet_text;
        candidate.language_code = hydrated.language_code;
        candidate.favorite_count = hydrated.favorite_count;
        candidate.reply_count = hydrated.reply_count;
        candidate.repost_count = hydrated.repost_count;
        candidate.quote_count = hydrated.quote_count;
        candidate.filtered_topic_ids = hydrated.filtered_topic_ids;
        candidate.unfiltered_topic_ids = hydrated.unfiltered_topic_ids;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate_pipeline::candidate_features::{MediaEntities, PureCoreData};

    struct FakeTESClient;

    #[async_trait]
    impl TESClient for FakeTESClient {
        async fn get_tweet_core_datas(
            &self,
            tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<PureCoreData>>, anyhow::Error> {
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
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<MediaEntities>>, anyhow::Error> {
            Ok(HashMap::new())
        }

        async fn get_subscription_author_ids(
            &self,
            _tweet_ids: Vec<i64>,
        ) -> Result<HashMap<i64, Option<u64>>, anyhow::Error> {
            Ok(HashMap::new())
        }
    }

    #[test]
    fn hydrates_quote_text_language_engagements_and_topics() {
        let hydrator = CoreDataCandidateHydrator {
            tes_client: Arc::new(FakeTESClient),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let hydrated = runtime
            .block_on(hydrator.hydrate(
                &ScoredPostsQuery::default(),
                &[PostCandidate {
                    tweet_id: 100,
                    ..Default::default()
                }],
            ))
            .expect("core hydration");

        assert_eq!(hydrated[0].quoted_tweet_id, Some(300));
        assert_eq!(hydrated[0].quoted_tweet_text, "quoted text");
        assert_eq!(hydrated[0].language_code.as_deref(), Some("en"));
        assert_eq!(hydrated[0].favorite_count, Some(12));
        assert_eq!(hydrated[0].filtered_topic_ids, vec![10]);
    }
}
