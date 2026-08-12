use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use crate::params as p;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

// Prioritize in-network candidates over out-of-network candidates
pub struct OONScorer;

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for OONScorer {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let oon_weight = if query.topic_recall_mode() == TopicRecallMode::Strict {
            p::TOPIC_OON_WEIGHT_FACTOR
        } else {
            p::OON_WEIGHT_FACTOR
        };
        let scored = candidates
            .iter()
            .map(|c| {
                let updated_score = c.score.map(|base_score| match c.in_network {
                    Some(false) => base_score * oon_weight,
                    _ => base_score,
                });

                Ok(PostCandidate {
                    score: updated_score,
                    ..Default::default()
                })
            })
            .collect();

        scored
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.score = scored.score;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn score(query: ScoredPostsQuery) -> f64 {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let scored = runtime.block_on(OONScorer.score(
            &query,
            &[PostCandidate {
                score: Some(2.0),
                in_network: Some(false),
                ..Default::default()
            }],
        ));
        scored[0].as_ref().expect("OON score").score.expect("score")
    }

    #[test]
    fn topic_feed_does_not_apply_generic_oon_penalty() {
        let topic_query = ScoredPostsQuery {
            topic_ids: vec![10],
            ..Default::default()
        };

        assert_eq!(score(ScoredPostsQuery::default()), 1.0);
        assert_eq!(score(topic_query), 2.0);
    }

    #[test]
    fn supplemental_topics_keep_generic_oon_penalty() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            ..Default::default()
        };

        assert_eq!(score(query), 1.0);
    }

    #[test]
    fn new_user_topics_keep_generic_oon_penalty_without_eligibility_data() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..Default::default()
        };

        assert_eq!(score(query), 1.0);
    }
}
