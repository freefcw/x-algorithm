use crate::models::candidate::PostCandidate;
use crate::models::query::ScoredPostsQuery;
use crate::scorers::author_diversity_scorer::AuthorDiversityScorer;
use crate::scorers::oon_scorer::OONScorer;
use crate::scorers::weighted_scorer::WeightedScorer;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

/// Upstream-shaped ranking boundary over the local portable ranking stages.
pub struct RankingScorer;

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for RankingScorer {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let mut working = candidates.to_vec();
        let mut errors: Vec<Option<String>> = vec![None; candidates.len()];

        let weighted = WeightedScorer;
        let results = weighted.score(query, &working).await;
        apply_stage(&weighted, &mut working, &mut errors, results);

        let author_diversity = AuthorDiversityScorer::default();
        let results = author_diversity.score(query, &working).await;
        apply_stage(&author_diversity, &mut working, &mut errors, results);

        let oon = OONScorer;
        let results = oon.score(query, &working).await;
        apply_stage(&oon, &mut working, &mut errors, results);

        working
            .into_iter()
            .zip(errors)
            .map(|(candidate, error)| match error {
                Some(error) => Err(error),
                None => Ok(PostCandidate {
                    weighted_score: candidate.weighted_score,
                    score: candidate.score,
                    ..Default::default()
                }),
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.weighted_score = scored.weighted_score;
        candidate.score = scored.score;
    }
}

fn apply_stage<S>(
    scorer: &S,
    candidates: &mut [PostCandidate],
    errors: &mut [Option<String>],
    results: Vec<Result<PostCandidate, String>>,
) where
    S: Scorer<ScoredPostsQuery, PostCandidate>,
{
    if results.len() != candidates.len() {
        let error = format!(
            "RankingScorer length_mismatch expected={} got={}",
            candidates.len(),
            results.len()
        );
        errors
            .iter_mut()
            .for_each(|slot| *slot = Some(error.clone()));
        return;
    }

    for ((candidate, error), result) in candidates.iter_mut().zip(errors.iter_mut()).zip(results) {
        if error.is_some() {
            continue;
        }
        match result {
            Ok(scored) => scorer.update(candidate, scored),
            Err(stage_error) => *error = Some(stage_error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::candidate::PhoenixScores;

    #[tokio::test]
    async fn combines_local_ranking_stages_behind_one_upstream_boundary() {
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                author_id: 10,
                in_network: Some(true),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.8),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                author_id: 10,
                in_network: Some(false),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.7),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let ranked = RankingScorer
            .score(&ScoredPostsQuery::default(), &candidates)
            .await;

        let first = ranked[0].as_ref().expect("first ranking result");
        let second = ranked[1].as_ref().expect("second ranking result");
        assert!(first.weighted_score.is_some());
        assert!(first.score > second.score);
    }
}
