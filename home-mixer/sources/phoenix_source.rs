use crate::clients::phoenix_retrieval_client::{retrieve_with_timeout, PhoenixRetrievalClient};
use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params as p;
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

        let mut unparsable_ids = 0usize;
        let mut unparsable_example: Option<String> = None;
        let candidates: Vec<PostCandidate> = response
            .top_k_candidates
            .into_iter()
            .flat_map(|scored_candidates| scored_candidates.candidates)
            .filter_map(|scored_candidate| scored_candidate.candidate)
            .filter_map(|tweet_info| {
                let parsed = parse_tweet_info(&tweet_info);
                if parsed.is_none() {
                    unparsable_ids += 1;
                    unparsable_example.get_or_insert_with(|| tweet_info.tweet_id.clone());
                }
                parsed
            })
            .map(
                |(tweet_id, author_id, in_reply_to_tweet_id)| PostCandidate {
                    tweet_id,
                    author_id,
                    in_reply_to_tweet_id,
                    served_type: Some(pb::ServedType::ForYouPhoenixRetrieval),
                    ..Default::default()
                },
            )
            .collect();
        if unparsable_ids > 0 {
            log::warn!(
                "PhoenixSource: dropped {} retrieved candidate(s) whose ids are not 24-hex ObjectIds (e.g. {:?})",
                unparsable_ids,
                unparsable_example.unwrap_or_default()
            );
        }

        Ok(candidates)
    }
}

/// Parse Phoenix `TweetInfo` identity fields into pipeline ObjectIds.
/// Empty `in_reply_to_tweet_id` means not a reply. `"0"` is not a sentinel
/// and fails closed (the whole candidate is dropped).
pub(crate) fn parse_tweet_info(
    tweet_info: &x_algorithm_proto::recsys::TweetInfo,
) -> Option<(
    crate::models::PostId,
    crate::models::UserId,
    Option<crate::models::PostId>,
)> {
    use crate::models::ids::ObjectId;
    let tweet_id = ObjectId::parse(&tweet_info.tweet_id)
        .ok()
        .filter(|id| !id.is_nil())?;
    let author_id = ObjectId::parse(&tweet_info.author_id)
        .ok()
        .filter(|id| !id.is_nil())?;
    let in_reply_to_tweet_id = match ObjectId::parse_optional(&tweet_info.in_reply_to_tweet_id) {
        Ok(None) => None,
        Ok(Some(id)) if id.is_nil() => None,
        Ok(Some(id)) => Some(id),
        Err(_) => return None,
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
            let tweet =
                |tweet_id: &str, author_id: &str, in_reply_to: &str| recsys::ScoredCandidate {
                    candidate: Some(recsys::TweetInfo {
                        tweet_id: tweet_id.to_string(),
                        author_id: author_id.to_string(),
                        in_reply_to_tweet_id: in_reply_to.to_string(),
                        ..Default::default()
                    }),
                    score: 0.5,
                };
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![
                        tweet("000000000000000000000064", "0000000000000000000000c8", ""),
                        tweet(
                            "000000000000000000000065",
                            "0000000000000000000000c8",
                            "000000000000000000000063",
                        ),
                        tweet("5f1a2b3c4d5e6f7a8b9c0d1e", "0000000000000000000000c8", ""),
                        tweet("103", "0000000000000000000000c8", ""),
                        tweet("000000000000000000000068", "not-a-id", ""),
                        tweet("000000000000000000000069", "0000000000000000000000c8", "0"),
                    ],
                }],
            })
        }
    }

    #[tokio::test]
    async fn non_object_ids_are_dropped_instead_of_becoming_nil() {
        use crate::models::{pid, uid};
        let source = PhoenixSource {
            phoenix_retrieval_client: Arc::new(MixedIdRetrievalClient),
        };
        let query = ScoredPostsQuery {
            retrieval_sequence: Some(recsys::UserActionSequence::default()),
            ..Default::default()
        };

        let candidates = source.source(&query).await.expect("retrieval succeeds");

        let ids: Vec<_> = candidates
            .iter()
            .map(|c| (c.tweet_id, c.author_id, c.in_reply_to_tweet_id))
            .collect();
        let hex = crate::models::ObjectId::parse("5f1a2b3c4d5e6f7a8b9c0d1e").unwrap();
        assert_eq!(
            ids,
            vec![
                (pid(0x64), uid(0xc8), None),
                (pid(0x65), uid(0xc8), Some(pid(0x63))),
                (hex, uid(0xc8), None),
            ]
        );
        assert!(candidates
            .iter()
            .all(|c| !c.tweet_id.is_nil() && !c.author_id.is_nil()));
        assert!(candidates
            .iter()
            .all(|c| c.served_type == Some(pb::ServedType::ForYouPhoenixRetrieval)));
    }

    #[test]
    fn supplemental_topics_keep_standard_retrieval_enabled() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            ..Default::default()
        };

        assert!(source().enable(&query));
    }

    #[test]
    fn new_user_topics_disable_standard_retrieval() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..Default::default()
        };

        assert!(!source().enable(&query));
    }

    #[test]
    fn requested_topic_page_disables_standard_retrieval() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10],
            ..Default::default()
        };

        assert!(!source().enable(&query));
    }
}
