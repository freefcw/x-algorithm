use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
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
    ) -> Result<Vec<PostCandidate>, String> {
        let oon_weight = if query.topic_ids.is_empty() && query.new_user_topic_ids.is_empty() {
            p::OON_WEIGHT_FACTOR
        } else {
            p::TOPIC_OON_WEIGHT_FACTOR
        };
        let scored = candidates
            .iter()
            .map(|c| {
                let updated_score = c.score.map(|base_score| match c.in_network {
                    Some(false) => base_score * oon_weight,
                    _ => base_score,
                });

                PostCandidate {
                    score: updated_score,
                    ..Default::default()
                }
            })
            .collect();

        Ok(scored)
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
        let scored = runtime
            .block_on(OONScorer.score(
                &query,
                &[PostCandidate {
                    score: Some(2.0),
                    in_network: Some(false),
                    ..Default::default()
                }],
            ))
            .expect("OON score");
        scored[0].score.expect("score")
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
}
