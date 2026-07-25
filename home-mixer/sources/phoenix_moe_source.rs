use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::clients::phoenix_retrieval_client::PhoenixRetrievalClient;
use crate::params;
use std::sync::Arc;
use tonic::async_trait;
use x_algorithm_proto::home_mixer as pb;
use xai_candidate_pipeline::source::Source;

pub struct PhoenixMoeSource {
    pub phoenix_retrieval_client: Arc<dyn PhoenixRetrievalClient + Send + Sync>,
}

#[async_trait]
impl Source<ScoredPostsQuery, PostCandidate> for PhoenixMoeSource {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.enable_phoenix_moe
            && query.topic_ids.is_empty()
            && query.new_user_topic_ids.is_empty()
            && !query.in_network_only
            && !query.has_cached_posts
    }

    async fn get_candidates(&self, query: &ScoredPostsQuery) -> Result<Vec<PostCandidate>, String> {
        let sequence = query
            .retrieval_sequence
            .as_ref()
            .or(query.user_action_sequence.as_ref())
            .ok_or_else(|| "PhoenixMoeSource: missing retrieval sequence".to_string())?;
        let response = self
            .phoenix_retrieval_client
            .retrieve(
                query.user_id as u64,
                sequence.clone(),
                params::PHOENIX_MAX_RESULTS,
            )
            .await
            .map_err(|error| format!("PhoenixMoeSource: {error}"))?;

        Ok(response
            .top_k_candidates
            .into_iter()
            .flat_map(|group| group.candidates)
            .filter_map(|candidate| candidate.candidate)
            .map(|tweet| PostCandidate {
                tweet_id: tweet.tweet_id as i64,
                author_id: tweet.author_id,
                in_reply_to_tweet_id: Some(tweet.in_reply_to_tweet_id),
                served_type: Some(pb::ServedType::ForYouPhoenixRetrievalMoe),
                ..Default::default()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use x_algorithm_proto::recsys;

    struct FakeRetrievalClient;

    #[async_trait]
    impl PhoenixRetrievalClient for FakeRetrievalClient {
        async fn retrieve(
            &self,
            _user_id: u64,
            _sequence: recsys::UserActionSequence,
            _max_results: u32,
        ) -> Result<recsys::RetrieveResponse, anyhow::Error> {
            Ok(recsys::RetrieveResponse {
                top_k_candidates: vec![recsys::ScoredCandidates {
                    candidates: vec![recsys::ScoredCandidate {
                        candidate: Some(recsys::TweetInfo {
                            tweet_id: 100,
                            author_id: 200,
                            ..Default::default()
                        }),
                        score: 0.9,
                    }],
                }],
            })
        }
    }

    #[test]
    fn enabled_moe_source_marks_its_candidates() {
        let source = PhoenixMoeSource {
            phoenix_retrieval_client: Arc::new(FakeRetrievalClient),
        };
        let query = ScoredPostsQuery {
            enable_phoenix_moe: true,
            retrieval_sequence: Some(recsys::UserActionSequence::default()),
            ..Default::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");

        let candidates = runtime
            .block_on(source.get_candidates(&query))
            .expect("MoE candidates");

        assert!(source.enable(&query));
        assert_eq!(candidates[0].tweet_id, 100);
        assert_eq!(
            candidates[0].served_type,
            Some(pb::ServedType::ForYouPhoenixRetrievalMoe)
        );
    }
}
