use crate::clients::phoenix_retrieval_client::{retrieve_with_timeout, PhoenixRetrievalClient};
use crate::models::candidate::{PostCandidate, RetrievalSource};
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params as p;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixSource {
    pub phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        !query.in_network_only
            && !query.has_cached_posts
            && !matches!(
                query.topic_recall_mode(),
                TopicRecallMode::Strict | TopicRecallMode::ColdStart
            )
    }

    async fn source(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let user_id = query.user_id;

        let sequence = query
            .retrieval_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
            .ok_or_else(|| "PhoenixSource: missing retrieval sequence".to_string())?;

        let response = retrieve_with_timeout(
            self.phoenix_retrieval_client.as_ref(),
            user_id,
            sequence.clone(),
            p::PHOENIX_MAX_RESULTS,
            Duration::from_millis(p::PHOENIX_RETRIEVAL_TIMEOUT_MS),
        )
        .await
        .map_err(|e| format!("PhoenixSource: {e}"))?;

        Ok(candidates_from_retrieval_response(
            response,
            pb::ServedType::ForYouPhoenixRetrieval,
            "PhoenixSource",
        ))
    }
}

pub(crate) fn candidates_from_retrieval_response(
    response: x_algorithm_proto::recsys::RetrieveResponse,
    served_type: pb::ServedType,
    source_name: &str,
) -> Vec<PostCandidate> {
    let mut invalid_ids = 0usize;
    let mut invalid_example: Option<u64> = None;
    let scored: Vec<_> = response
        .top_k_candidates
        .into_iter()
        .flat_map(|group| group.candidates)
        .filter_map(|scored| {
            let tweet = scored.candidate?;
            let parsed = parse_tweet_info(&tweet);
            if parsed.is_none() {
                invalid_ids += 1;
                invalid_example = Some(tweet.tweet_id);
            }
            parsed.map(|identity| {
                (
                    identity,
                    scored.score,
                    scored.dataset_type,
                    scored.source_idx,
                )
            })
        })
        .collect();

    if invalid_ids > 0 {
        log::warn!(
            "{source_name}: dropped {invalid_ids} retrieved candidate(s) with out-of-range Snowflake IDs (e.g. {:?})",
            invalid_example.unwrap_or_default()
        );
    }

    let mut score_order: Vec<usize> = (0..scored.len()).collect();
    score_order.sort_by(|left, right| scored[*right].1.total_cmp(&scored[*left].1));
    let mut positions = vec![0; scored.len()];
    let mut next_position = HashMap::new();
    for scored_index in score_order {
        let provenance_group = (scored[scored_index].2, scored[scored_index].3);
        let position = next_position.entry(provenance_group).or_insert(0);
        *position += 1;
        positions[scored_index] = *position;
    }

    scored
        .into_iter()
        .zip(positions)
        .map(
            |(
                ((tweet_id, author_id, in_reply_to_tweet_id), score, dataset_type, source_idx),
                position,
            )| PostCandidate {
                tweet_id,
                author_id,
                in_reply_to_tweet_id,
                served_type: Some(served_type),
                retrieval_sources: vec![RetrievalSource {
                    served_type,
                    dataset_type,
                    source_idx,
                    score: Some(score),
                    position: Some(position),
                }],
                ..Default::default()
            },
        )
        .collect()
}

/// Validate Phoenix `TweetInfo` numeric identity fields. `0`
/// `in_reply_to_tweet_id` means not a reply; `0` in the required fields or any
/// value above `i64::MAX` fails closed (the whole candidate is dropped).
pub(crate) fn parse_tweet_info(
    tweet_info: &x_algorithm_proto::recsys::TweetInfo,
) -> Option<(
    crate::models::PostId,
    crate::models::UserId,
    Option<crate::models::PostId>,
)> {
    fn valid(id: u64) -> Option<u64> {
        (id != 0 && id <= i64::MAX as u64).then_some(id)
    }
    let tweet_id = valid(tweet_info.tweet_id)?;
    let author_id = valid(tweet_info.author_id)?;
    let in_reply_to_tweet_id = match tweet_info.in_reply_to_tweet_id {
        0 => None,
        id => Some(valid(id)?),
    };
    Some((tweet_id, author_id, in_reply_to_tweet_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::recsys;

    struct UnusedRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for UnusedRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: crate::models::UserId,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            unreachable!("enablement tests do not retrieve candidates")
        }
    }

    fn source() -> PhoenixSource {
        PhoenixSource {
            phoenix_retrieval_client: Arc::new(UnusedRetrievalClient),
        }
    }

    struct MixedIdRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for MixedIdRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: crate::models::UserId,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            let tweet = |tweet_id: u64, author_id: u64, in_reply_to: u64| recsys::ScoredCandidate {
                candidate: Some(recsys::TweetInfo {
                    tweet_id,
                    author_id,
                    in_reply_to_tweet_id: in_reply_to,
                    ..Default::default()
                }),
                score: 0.5,
                dataset_type: Some(1),
                source_idx: Some(0),
            };
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![
                        tweet(0x64, 0xc8, 0),
                        tweet(0x65, 0xc8, 0x63),
                        tweet(0x66, 0xc8, 0),
                        tweet(0, 0xc8, 0),
                        tweet(0x68, 0, 0),
                        tweet(0x69, 0xc8, i64::MAX as u64 + 1),
                    ],
                }],
            })
        }
    }

    #[tokio::test]
    async fn out_of_range_ids_are_dropped_instead_of_becoming_nil() {
        use crate::models::{pid, uid};
        let source = PhoenixSource {
            phoenix_retrieval_client: Arc::new(MixedIdRetrievalClient),
        };
        let query = ScoredPostsQuery {
            retrieval_sequence: Some(recsys::UserActionSequence::default()),
            ..ScoredPostsQuery::test_default()
        };

        let candidates = source.source(&query).await.expect("retrieval succeeds");

        let ids: Vec<_> = candidates
            .iter()
            .map(|c| (c.tweet_id, c.author_id, c.in_reply_to_tweet_id))
            .collect();
        assert_eq!(
            ids,
            vec![
                (pid(0x64), uid(0xc8), None),
                (pid(0x65), uid(0xc8), Some(pid(0x63))),
                (pid(0x66), uid(0xc8), None),
            ]
        );
        assert!(candidates
            .iter()
            .all(|c| c.tweet_id != 0 && c.author_id != 0));
        assert!(candidates
            .iter()
            .all(|c| c.served_type == Some(pb::ServedType::ForYouPhoenixRetrieval)));
        assert!(candidates.iter().all(|candidate| {
            candidate.retrieval_sources[0].dataset_type == Some(1)
                && candidate.retrieval_sources[0].source_idx == Some(0)
        }));
    }

    #[test]
    fn provenance_keeps_service_metadata_and_score_positions_separate_from_ranking_score() {
        let scored = |tweet_id, score, dataset_type, source_idx| recsys::ScoredCandidate {
            candidate: Some(recsys::TweetInfo {
                tweet_id,
                author_id: 200,
                ..Default::default()
            }),
            score,
            dataset_type,
            source_idx,
        };
        let response = recsys::RetrieveResponse {
            top_k_candidates: vec![recsys::ScoredCandidates {
                candidates: vec![
                    scored(100, 0.25, Some(1), Some(0)),
                    scored(101, 0.75, Some(1), Some(0)),
                    scored(102, 0.50, Some(8), Some(3)),
                    scored(103, 0.10, Some(8), Some(3)),
                    scored(104, 0.20, None, None),
                ],
            }],
        };

        let candidates = candidates_from_retrieval_response(
            response,
            pb::ServedType::ForYouPhoenixRetrieval,
            "test",
        );

        assert_eq!(candidates[0].score, None);
        assert_eq!(candidates[0].retrieval_sources[0].position, Some(2));
        assert_eq!(candidates[0].retrieval_sources[0].source_idx, Some(0));
        assert_eq!(candidates[1].retrieval_sources[0].position, Some(1));
        assert_eq!(candidates[1].retrieval_sources[0].dataset_type, Some(1));
        assert_eq!(candidates[2].retrieval_sources[0].position, Some(1));
        assert_eq!(candidates[2].retrieval_sources[0].source_idx, Some(3));
        assert_eq!(candidates[3].retrieval_sources[0].position, Some(2));
        assert_eq!(candidates[4].retrieval_sources[0].position, Some(1));
        assert_eq!(candidates[4].retrieval_sources[0].source_idx, None);
    }

    #[test]
    fn supplemental_topics_keep_standard_retrieval_enabled() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            ..ScoredPostsQuery::test_default()
        };

        assert!(source().enable(&query));
    }

    #[test]
    fn new_user_topics_disable_standard_retrieval() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..ScoredPostsQuery::test_default()
        };

        assert!(!source().enable(&query));
    }

    #[test]
    fn requested_topic_page_disables_standard_retrieval() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10],
            ..ScoredPostsQuery::test_default()
        };

        assert!(!source().enable(&query));
    }
}
