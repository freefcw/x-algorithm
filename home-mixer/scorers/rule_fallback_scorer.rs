use crate::models::candidate::{PhoenixScores, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use crate::params::MAX_POST_AGE;
use std::collections::HashMap;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

const FALLBACK_REASON: &str = "phoenix_unavailable";

/// Whole-batch rule ranking used when Phoenix produced no usable scores.
///
/// Assembled after `RankingScorer`. It is a no-op only when every candidate in
/// the batch has usable Phoenix heads; otherwise it overwrites every candidate
/// with one consistent rule score and stamps `degraded_reason`.
pub struct RuleFallbackScorer;

impl RuleFallbackScorer {
    fn phoenix_scored(scores: &PhoenixScores) -> bool {
        scores.favorite_score.is_some()
            || scores.reply_score.is_some()
            || scores.click_score.is_some()
            || scores.dwell_score.is_some()
            || scores.follow_author_score.is_some()
            || scores.not_interested_score.is_some()
    }

    fn candidate_has_usable_phoenix(candidate: &PostCandidate) -> bool {
        candidate.degraded_reason.is_none() && Self::phoenix_scored(&candidate.phoenix_scores)
    }

    fn rule_score(candidate: &PostCandidate, now_ms: u64) -> f64 {
        let created = candidate.created_at_ms.unwrap_or(0);
        let age_ms = now_ms.saturating_sub(created);
        let age_days = age_ms as f64 / 86_400_000.0;
        let recency = 1.5 * (1.0 - age_days / 7.0).max(0.0);
        let in_network = if candidate.in_network.unwrap_or(false) {
            2.0
        } else {
            0.0
        };
        // Backend counters are untrusted: clamp negatives before ln_1p so the
        // fallback always emits a finite, non-negative score.
        let likes = candidate.favorite_count.unwrap_or(0).max(0) as f64;
        let replies = candidate.reply_count.unwrap_or(0).max(0) as f64;
        let engagement = 0.2 * (likes + 2.0 * replies).ln_1p() * 0.5f64.powf(age_days);
        let score = recency + in_network + engagement;
        if score.is_finite() {
            score.max(0.0)
        } else {
            0.0
        }
    }
}

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for RuleFallbackScorer {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        if !candidates.is_empty() && candidates.iter().all(Self::candidate_has_usable_phoenix) {
            return vec![Ok(PostCandidate::default()); candidates.len()];
        }

        let now_ms = u64::try_from(query.request_time_ms).unwrap_or(0);
        let max_age_ms = MAX_POST_AGE.saturating_mul(1000);
        let mut author_counts = HashMap::new();
        candidates
            .iter()
            .map(|candidate| {
                let created = candidate.created_at_ms.unwrap_or(0);
                let too_old =
                    created > 0 && now_ms.saturating_sub(created) > max_age_ms && max_age_ms > 0;
                let base_score = if too_old {
                    0.0
                } else {
                    Self::rule_score(candidate, now_ms)
                };
                let count = author_counts.entry(candidate.author_id).or_insert(0_u32);
                let diversity = 0.5_f64.powi((*count).min(8) as i32).max(0.25);
                *count += 1;
                let score = (base_score * diversity).max(0.0);
                Ok(PostCandidate {
                    phoenix_scores: PhoenixScores::default(),
                    score: Some(score),
                    degraded_reason: Some(FALLBACK_REASON.to_string()),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        if scored.degraded_reason.is_none() {
            return;
        }
        candidate.phoenix_scores = PhoenixScores::default();
        candidate.prediction_request_id = None;
        candidate.last_scored_at_ms = None;
        candidate.weighted_score = None;
        candidate.score = scored.score;
        candidate.degraded_reason = scored.degraded_reason;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::pid;

    #[tokio::test]
    async fn noops_when_any_phoenix_head_is_present() {
        let scorer = RuleFallbackScorer;
        let candidates = [PostCandidate {
            tweet_id: pid(1),
            phoenix_scores: PhoenixScores {
                favorite_score: Some(0.4),
                ..Default::default()
            },
            score: Some(9.0),
            ..Default::default()
        }];
        let scored = scorer
            .score(&ScoredPostsQuery::test_default(), &candidates)
            .await;
        let mut candidate = candidates[0].clone();
        scorer.update(&mut candidate, scored[0].as_ref().unwrap().clone());
        assert_eq!(candidate.score, Some(9.0));
        assert_eq!(candidate.degraded_reason, None);
    }

    #[tokio::test]
    async fn ranks_in_network_ahead_of_oon_when_phoenix_is_empty() {
        let scorer = RuleFallbackScorer;
        let now = 1_700_000_000_000_i64;
        let query = ScoredPostsQuery {
            request_time_ms: now,
            ..ScoredPostsQuery::test_default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: pid(1),
                in_network: Some(false),
                created_at_ms: Some(u64::try_from(now).unwrap() - 3_600_000),
                favorite_count: Some(1),
                ..Default::default()
            },
            PostCandidate {
                tweet_id: pid(2),
                in_network: Some(true),
                created_at_ms: Some(u64::try_from(now).unwrap() - 3_600_000),
                favorite_count: Some(1),
                ..Default::default()
            },
        ];
        let scored = scorer.score(&query, &candidates).await;
        let mut oon = candidates[0].clone();
        let mut inn = candidates[1].clone();
        scorer.update(&mut oon, scored[0].as_ref().unwrap().clone());
        scorer.update(&mut inn, scored[1].as_ref().unwrap().clone());
        assert_eq!(inn.degraded_reason.as_deref(), Some(FALLBACK_REASON));
        assert!(inn.score.unwrap() > oon.score.unwrap());
    }

    #[tokio::test]
    async fn mixed_or_invalid_phoenix_batch_uses_rules_and_clears_stale_heads() {
        let scorer = RuleFallbackScorer;
        let query = ScoredPostsQuery {
            request_time_ms: 1_700_000_000_000,
            ..ScoredPostsQuery::test_default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: pid(1),
                author_id: crate::models::uid(7),
                created_at_ms: Some(1_699_999_000_000),
                favorite_count: Some(-10),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.8),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: pid(2),
                author_id: crate::models::uid(7),
                created_at_ms: Some(1_699_999_000_000),
                favorite_count: Some(4),
                ..Default::default()
            },
        ];
        let scored = scorer.score(&query, &candidates).await;
        assert!(scored.iter().all(Result::is_ok));
        let mut first = candidates[0].clone();
        scorer.update(&mut first, scored[0].as_ref().unwrap().clone());
        assert!(first.phoenix_scores.favorite_score.is_none());
        assert!(first.score.unwrap().is_finite());
        assert!(first.score.unwrap() >= 0.0);
        assert!(scored[1].as_ref().unwrap().score.unwrap() < first.score.unwrap());
    }
}
