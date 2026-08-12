//! 上游同构的 TweetMixer 网外候选来源（SRC-05）。
//!
//! 默认不装配：需要真实 `TweetMixerClient` Adapter 后由装配显式注入。
//! 行为与上游一致：仅在允许网外且无请求缓存时启用；把 `seen_ids` 作为
//! 排除列表传给服务；用 Snowflake 时间戳过滤超龄帖子；返回最小候选并
//! 交由标准 Hydrator 补全。

use crate::clients::tweet_mixer_client::{TweetMixerClient, TweetMixerRequest};
use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::params::{MAX_POST_AGE, TWEET_MIXER_MAX_RESULTS};
use crate::util::snowflake::duration_since_creation_opt;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct TweetMixerSource {
    pub tweet_mixer_client: Arc<dyn TweetMixerClient>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for TweetMixerSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.in_network_only && !query.has_cached_posts
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let opt = |value: &str| {
            if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }
        };

        let request = TweetMixerRequest {
            user_id: query.user_id,
            client_app_id: query.client_app_id,
            user_agent: opt(&query.user_agent),
            country_code: opt(&query.country_code),
            language_code: opt(&query.language_code),
            excluded_tweet_ids: query.seen_ids.clone(),
            max_results: TWEET_MIXER_MAX_RESULTS,
        };

        let candidates = self
            .tweet_mixer_client
            .get_recommendations(request)
            .await
            .map_err(|error| format!("TweetMixerSource: {error}"))?;

        let result = candidates
            .into_iter()
            .filter_map(|candidate| {
                let within_age = duration_since_creation_opt(candidate.tweet_id)
                    .map(|age| age <= Duration::from_secs(MAX_POST_AGE))
                    .unwrap_or(false);
                if !within_age {
                    return None;
                }

                Some(PostCandidate {
                    tweet_id: candidate.tweet_id,
                    author_id: candidate.author_id.unwrap_or_default(),
                    in_reply_to_tweet_id: candidate.in_reply_to_tweet_id,
                    retweeted_tweet_id: None,
                    served_type: Some(pb::ServedType::ForYouTweetMixer),
                    ..Default::default()
                })
            })
            .collect();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::tweet_mixer_client::TweetMixerCandidate;
    use std::sync::Mutex;

    struct FakeTweetMixer {
        candidates: Vec<TweetMixerCandidate>,
        last_request: Mutex<Option<TweetMixerRequest>>,
    }

    #[async_trait]
    impl TweetMixerClient for FakeTweetMixer {
        async fn get_recommendations(
            &self,
            request: TweetMixerRequest,
        ) -> Result<Vec<TweetMixerCandidate>, String> {
            *self.last_request.lock().expect("request lock") = Some(request);
            Ok(self.candidates.clone())
        }
    }

    /// 用本地 Snowflake 纪元构造指定年龄的帖子 ID。
    fn snowflake_id_with_age(age: Duration) -> u64 {
        const TWITTER_EPOCH_MS: u64 = 1_288_834_974_657;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        let created_ms = now_ms - age.as_millis() as u64;
        (created_ms - TWITTER_EPOCH_MS) << 22
    }

    #[test]
    fn disabled_for_in_network_only_or_cached_requests() {
        let source = TweetMixerSource {
            tweet_mixer_client: Arc::new(FakeTweetMixer {
                candidates: Vec::new(),
                last_request: Mutex::new(None),
            }),
        };
        assert!(!source.enable(&ScoredPostsQuery {
            in_network_only: true,
            ..Default::default()
        }));
        assert!(!source.enable(&ScoredPostsQuery {
            has_cached_posts: true,
            ..Default::default()
        }));
        assert!(source.enable(&ScoredPostsQuery::default()));
    }

    #[tokio::test]
    async fn maps_request_fields_and_drops_stale_posts() {
        let fresh_id = snowflake_id_with_age(Duration::from_secs(60));
        let stale_id = snowflake_id_with_age(Duration::from_secs(MAX_POST_AGE + 3_600));
        let client = Arc::new(FakeTweetMixer {
            candidates: vec![
                TweetMixerCandidate {
                    tweet_id: fresh_id,
                    author_id: Some(7),
                    in_reply_to_tweet_id: None,
                },
                TweetMixerCandidate {
                    tweet_id: stale_id,
                    author_id: Some(8),
                    in_reply_to_tweet_id: None,
                },
            ],
            last_request: Mutex::new(None),
        });
        let source = TweetMixerSource {
            tweet_mixer_client: Arc::clone(&client) as Arc<dyn TweetMixerClient>,
        };

        let query = ScoredPostsQuery {
            user_id: 42,
            user_agent: "agent".to_string(),
            country_code: String::new(),
            seen_ids: vec![11, 12],
            ..Default::default()
        };
        let candidates = source.source(&query).await.expect("source");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].tweet_id, fresh_id);
        assert_eq!(candidates[0].author_id, 7);
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouTweetMixer)
        );

        let request = client
            .last_request
            .lock()
            .expect("request lock")
            .clone()
            .expect("request sent");
        assert_eq!(request.user_id, 42);
        assert_eq!(request.user_agent.as_deref(), Some("agent"));
        assert_eq!(request.country_code, None);
        assert_eq!(request.excluded_tweet_ids, vec![11, 12]);
        assert_eq!(request.max_results, TWEET_MIXER_MAX_RESULTS);
    }
}
