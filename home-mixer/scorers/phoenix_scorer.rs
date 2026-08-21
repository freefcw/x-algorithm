use crate::clients::phoenix_prediction_client::{predict_with_timeout, PhoenixPredictionClient};
use crate::models::candidate::{PhoenixScores, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use crate::params;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::async_trait;
use x_algorithm_proto::recsys::{ActionName, ContinuousActionName};
use xai_candidate_pipeline::scorer::Scorer;

pub struct PhoenixScorer {
    pub phoenix_client: Arc<dyn PhoenixPredictionClient + Send + Sync>,
}

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for PhoenixScorer {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let user_id = query.user_id;
        let prediction_request_id = query.prediction_id;
        let last_scored_at_ms = Self::current_timestamp_millis();

        if let Some(sequence) = query
            .scoring_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
        {
            let tweet_infos: Vec<x_algorithm_proto::recsys::TweetInfo> = candidates
                .iter()
                .map(|c| {
                    let tweet_id = c.retweeted_tweet_id.unwrap_or(c.tweet_id);
                    let author_id = c.retweeted_user_id.unwrap_or(c.author_id);
                    x_algorithm_proto::recsys::TweetInfo {
                        tweet_id,
                        author_id,
                        // 上游 b089ce6 合同：仅原创帖且作者命中 Phoenix NSFW 口径时置位；
                        // 转帖不置位（原作者安全语义经由原帖 ID 传递）。
                        safety_label_mask: if c.retweeted_user_id.is_none()
                            && c.nsfw_author_phoenix.unwrap_or(false)
                        {
                            x_algorithm_proto::SAFETY_BIT_AUTHOR_NSFW
                        } else {
                            0
                        },
                        ..Default::default()
                    }
                })
                .collect();

            match predict_with_timeout(
                self.phoenix_client.as_ref(),
                user_id,
                sequence.clone(),
                tweet_infos,
                Duration::from_millis(params::PHOENIX_PREDICTION_TIMEOUT_MS),
            )
            .await
            {
                Ok(response) => {
                    let predictions_map = self.build_predictions_map(&response);

                    return candidates
                        .iter()
                        .map(|candidate| {
                            let lookup_tweet_id =
                                candidate.retweeted_tweet_id.unwrap_or(candidate.tweet_id);
                            let phoenix_scores = predictions_map
                                .get(&lookup_tweet_id)
                                .map(|predictions| self.extract_phoenix_scores(predictions))
                                .unwrap_or_default();

                            Ok(PostCandidate {
                                phoenix_scores,
                                prediction_request_id: Some(prediction_request_id),
                                last_scored_at_ms,
                                ..Default::default()
                            })
                        })
                        .collect();
                }
                Err(error) => {
                    return vec![
                        Err(format!("Phoenix prediction failed: {error}"));
                        candidates.len()
                    ];
                }
            }
        }

        vec![Ok(PostCandidate::default()); candidates.len()]
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.phoenix_scores = scored.phoenix_scores;
        candidate.prediction_request_id = scored.prediction_request_id;
        candidate.last_scored_at_ms = scored.last_scored_at_ms;
    }
}

impl PhoenixScorer {
    /// Builds Map[tweet_id -> ActionPredictions]
    fn build_predictions_map(
        &self,
        response: &x_algorithm_proto::recsys::PredictNextActionsResponse,
    ) -> HashMap<u64, ActionPredictions> {
        let mut predictions_map = HashMap::new();

        let Some(distribution_set) = response.distribution_sets.first() else {
            return predictions_map;
        };

        for distribution in &distribution_set.candidate_distributions {
            let Some(candidate) = &distribution.candidate else {
                continue;
            };
            let tweet_id = candidate.tweet_id;

            let action_probs: HashMap<usize, f64> = distribution
                .top_log_probs
                .iter()
                .enumerate()
                .map(|(idx, log_prob)| (idx, (*log_prob as f64).exp()))
                .collect();

            let continuous_values: HashMap<usize, f64> = distribution
                .continuous_actions_values
                .iter()
                .enumerate()
                .map(|(idx, value)| (idx, *value as f64))
                .collect();

            predictions_map.insert(
                tweet_id,
                ActionPredictions {
                    action_probs,
                    continuous_values,
                },
            );
        }

        predictions_map
    }

    fn extract_phoenix_scores(&self, p: &ActionPredictions) -> PhoenixScores {
        PhoenixScores {
            favorite_score: p.get(ActionName::ServerTweetFav),
            reply_score: p.get(ActionName::ServerTweetReply),
            retweet_score: p.get(ActionName::ServerTweetRetweet),
            photo_expand_score: p.get(ActionName::ClientTweetPhotoExpand),
            click_score: p.get(ActionName::ClientTweetClick),
            profile_click_score: p.get(ActionName::ClientTweetClickProfile),
            vqv_score: p.get(ActionName::ClientTweetVideoQualityView),
            share_score: p.get(ActionName::ClientTweetShare),
            share_via_dm_score: p.get(ActionName::ClientTweetClickSendViaDirectMessage),
            share_via_copy_link_score: p.get(ActionName::ClientTweetShareViaCopyLink),
            dwell_score: p.get(ActionName::ClientTweetRecapDwelled),
            quote_score: p.get(ActionName::ServerTweetQuote),
            quoted_click_score: p.get(ActionName::ClientQuotedTweetClick),
            quoted_vqv_score: p.get(ActionName::ClientQuotedTweetVideoQualityView),
            follow_author_score: p.get(ActionName::ClientTweetFollowAuthor),
            not_interested_score: p.get(ActionName::ClientTweetNotInterestedIn),
            block_author_score: p.get(ActionName::ClientTweetBlockAuthor),
            mute_author_score: p.get(ActionName::ClientTweetMuteAuthor),
            report_score: p.get(ActionName::ClientTweetReport),
            not_dwelled_score: p.get(ActionName::ClientTweetNotDwelled),
            // 上游 47c1bcd 新增头：本地发布模型与公开协议尚无对应槽位，
            // 保持 None（权重贡献为 0），待模型/协议提供后接线。
            video_open_score: None,
            open_link_score: None,
            post_unexplored_score: None,
            dwell_time: p.get_continuous(ContinuousActionName::DwellTime),
            click_dwell_time: None,
            active_secs_5m_residual_norm: None,
        }
    }

    fn current_timestamp_millis() -> Option<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
    }
}

struct ActionPredictions {
    /// Map of action index -> probability (exp of log prob)
    action_probs: HashMap<usize, f64>,
    /// Map of continuous action index -> value
    continuous_values: HashMap<usize, f64>,
}

impl ActionPredictions {
    fn get(&self, action: ActionName) -> Option<f64> {
        self.action_probs.get(&(action as usize)).copied()
    }

    fn get_continuous(&self, action: ContinuousActionName) -> Option<f64> {
        self.continuous_values.get(&(action as usize)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnusedPhoenixClient;

    #[async_trait]
    impl PhoenixPredictionClient for UnusedPhoenixClient {
        async fn predict(
            &self,
            _user_id: u64,
            _sequence: x_algorithm_proto::recsys::UserActionSequence,
            _candidates: Vec<x_algorithm_proto::recsys::TweetInfo>,
        ) -> Result<x_algorithm_proto::recsys::PredictNextActionsResponse, anyhow::Error> {
            Ok(Default::default())
        }
    }

    struct FailingPhoenixClient;

    #[async_trait]
    impl PhoenixPredictionClient for FailingPhoenixClient {
        async fn predict(
            &self,
            _user_id: u64,
            _sequence: x_algorithm_proto::recsys::UserActionSequence,
            _candidates: Vec<x_algorithm_proto::recsys::TweetInfo>,
        ) -> Result<x_algorithm_proto::recsys::PredictNextActionsResponse, anyhow::Error> {
            anyhow::bail!("prediction unavailable")
        }
    }

    #[derive(Default)]
    struct RecordingPhoenixClient {
        seen: std::sync::Mutex<Vec<x_algorithm_proto::recsys::TweetInfo>>,
    }

    #[async_trait]
    impl PhoenixPredictionClient for RecordingPhoenixClient {
        async fn predict(
            &self,
            _user_id: u64,
            _sequence: x_algorithm_proto::recsys::UserActionSequence,
            candidates: Vec<x_algorithm_proto::recsys::TweetInfo>,
        ) -> Result<x_algorithm_proto::recsys::PredictNextActionsResponse, anyhow::Error> {
            *seen_lock(&self.seen) = candidates;
            Ok(Default::default())
        }
    }

    fn seen_lock<'a>(
        seen: &'a std::sync::Mutex<Vec<x_algorithm_proto::recsys::TweetInfo>>,
    ) -> std::sync::MutexGuard<'a, Vec<x_algorithm_proto::recsys::TweetInfo>> {
        seen.lock().expect("recording client lock")
    }

    fn scorer() -> PhoenixScorer {
        PhoenixScorer {
            phoenix_client: Arc::new(UnusedPhoenixClient),
        }
    }

    #[tokio::test]
    async fn scorer_propagates_query_prediction_id() {
        let query = ScoredPostsQuery {
            prediction_id: 123,
            scoring_sequence: Some(Default::default()),
            ..Default::default()
        };
        let scored = scorer()
            .score(
                &query,
                &[PostCandidate {
                    tweet_id: 10,
                    ..Default::default()
                }],
            )
            .await;

        assert_eq!(
            scored[0]
                .as_ref()
                .expect("candidate score")
                .prediction_request_id,
            Some(123)
        );
    }

    #[tokio::test]
    async fn unavailable_prediction_does_not_produce_scored_candidate_metadata() {
        let query = ScoredPostsQuery {
            prediction_id: 123,
            scoring_sequence: Some(Default::default()),
            ..Default::default()
        };
        let scorer = PhoenixScorer {
            phoenix_client: Arc::new(FailingPhoenixClient),
        };

        let scored = scorer
            .score(
                &query,
                &[PostCandidate {
                    tweet_id: 10,
                    ..Default::default()
                }],
            )
            .await;

        assert!(scored[0].is_err());
    }

    #[tokio::test]
    async fn author_nsfw_bit_only_set_for_original_posts() {
        let client = Arc::new(RecordingPhoenixClient::default());
        let scorer = PhoenixScorer {
            phoenix_client: client.clone(),
        };
        let query = ScoredPostsQuery {
            prediction_id: 1,
            scoring_sequence: Some(Default::default()),
            ..Default::default()
        };
        let candidates = vec![
            // 原创帖 + 命中 NSFW：置位
            PostCandidate {
                tweet_id: 1,
                nsfw_author_phoenix: Some(true),
                ..Default::default()
            },
            // 转帖 + 命中 NSFW：不置位
            PostCandidate {
                tweet_id: 2,
                retweeted_user_id: Some(99),
                retweeted_tweet_id: Some(20),
                nsfw_author_phoenix: Some(true),
                ..Default::default()
            },
            // 原创帖 + 数据源缺失：不置位
            PostCandidate {
                tweet_id: 3,
                ..Default::default()
            },
            // 原创帖 + 明确未命中：不置位
            PostCandidate {
                tweet_id: 4,
                nsfw_author_phoenix: Some(false),
                ..Default::default()
            },
        ];

        let scored = scorer.score(&query, &candidates).await;
        assert!(scored.iter().all(|r| r.is_ok()));

        let seen = seen_lock(&client.seen);
        let masks: Vec<u64> = seen.iter().map(|c| c.safety_label_mask).collect();
        assert_eq!(
            masks,
            vec![x_algorithm_proto::SAFETY_BIT_AUTHOR_NSFW, 0, 0, 0]
        );
    }

    #[test]
    fn reserved_discrete_slots_map_to_their_named_scores() {
        let predictions = ActionPredictions {
            action_probs: HashMap::from([(19, 0.25), (20, 0.75)]),
            continuous_values: HashMap::from([(ContinuousActionName::DwellTime as usize, 3.5)]),
        };

        let scores = scorer().extract_phoenix_scores(&predictions);

        assert_eq!(scores.quoted_vqv_score, Some(0.25));
        assert_eq!(scores.not_dwelled_score, Some(0.75));
        assert_eq!(scores.dwell_time, Some(3.5));
        assert_eq!(scores.click_dwell_time, None);
    }

    #[test]
    fn released_profile_without_reserved_slots_keeps_reserved_scores_empty() {
        let predictions = ActionPredictions {
            action_probs: (0..=ActionName::ClientTweetReport as usize)
                .map(|index| (index, 0.5))
                .collect(),
            continuous_values: HashMap::new(),
        };

        let scores = scorer().extract_phoenix_scores(&predictions);

        assert_eq!(scores.report_score, Some(0.5));
        assert_eq!(scores.quoted_vqv_score, None);
        assert_eq!(scores.not_dwelled_score, None);
        assert_eq!(scores.click_dwell_time, None);
    }
}
