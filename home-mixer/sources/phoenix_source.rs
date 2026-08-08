use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::{ScoredPostsQuery, TopicRecallMode};
use crate::clients::phoenix_retrieval_client::PhoenixRetrievalClient;
use crate::params as p;
use std::sync::Arc;
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

    async fn get_candidates(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let user_id = query.user_id as u64;

        let sequence = query
            .retrieval_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
            .ok_or_else(|| "PhoenixSource: missing retrieval sequence".to_string())?;

        let response = self
            .phoenix_retrieval_client
            .retrieve(user_id, sequence.clone(), p::PHOENIX_MAX_RESULTS)
            .await
            .map_err(|e| format!("PhoenixSource: {}", e))?;

        let candidates: Vec<PostCandidate> = response
            .top_k_candidates
            .into_iter()
            .flat_map(|scored_candidates| scored_candidates.candidates)
            .filter_map(|scored_candidate| scored_candidate.candidate)
            .map(|tweet_info| PostCandidate {
                tweet_id: tweet_info.tweet_id as i64,
                author_id: tweet_info.author_id,
                in_reply_to_tweet_id: Some(tweet_info.in_reply_to_tweet_id),
                served_type: Some(pb::ServedType::ForYouPhoenixRetrieval),
                ..Default::default()
            })
            .collect();

        Ok(candidates)
    }
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
            _user_id: u64,
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
