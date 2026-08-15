use crate::models::candidate::PostCandidate;
use crate::params;
use crate::util::snowflake::duration_since_creation_opt;
use rand::Rng;
use rand_distr::{Beta, Distribution};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct ColdStartConfig {
    pub enabled: bool,
    pub thompson_sampling: bool,
    pub impression_threshold: u64,
    pub slot_min: usize,
    pub slot_max: usize,
    pub follower_cap: i64,
    pub max_post_age: Duration,
    pub max_position_ratio: f64,
    pub beta_alpha0: f64,
    pub beta_beta0: f64,
    pub thompson_top_k: usize,
    pub impression_scale: f64,
}

impl Default for ColdStartConfig {
    fn default() -> Self {
        Self {
            enabled: params::ENABLE_VIEWER_COLD_START,
            thompson_sampling: params::ENABLE_COLD_START_THOMPSON_SAMPLING,
            impression_threshold: params::COLD_START_IMPRESSION_THRESHOLD,
            slot_min: params::COLD_START_SLOT_MIN,
            slot_max: params::COLD_START_SLOT_MAX,
            follower_cap: params::COLD_START_FOLLOWER_CAP,
            max_post_age: Duration::from_secs(params::COLD_START_MAX_POST_AGE_SECS),
            max_position_ratio: params::LOW_IMPRESSIONS_MAX_POSITION_RATIO,
            beta_alpha0: params::COLD_START_BETA_ALPHA0,
            beta_beta0: params::COLD_START_BETA_BETA0,
            thompson_top_k: params::COLD_START_TS_TOP_K,
            impression_scale: params::COLD_START_IMPRESSION_SCALE,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AuthorColdStart {
    config: ColdStartConfig,
}

impl AuthorColdStart {
    pub fn new(config: ColdStartConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn apply(&self, candidates: &[PostCandidate], scores: &[f64]) -> Vec<f64> {
        self.apply_with_rng(candidates, scores, &mut rand::rng())
    }

    fn apply_with_rng<R: Rng + ?Sized>(
        &self,
        candidates: &[PostCandidate],
        scores: &[f64],
        rng: &mut R,
    ) -> Vec<f64> {
        if !self.config.enabled || candidates.len() != scores.len() {
            return scores.to_vec();
        }

        let Some(target) = target_score(scores, &self.config, rng) else {
            return scores.to_vec();
        };
        let (positions, nonzero) = positions_among_nonzero(scores);
        let max_position = if self.config.max_position_ratio.is_finite() {
            (self.config.max_position_ratio.clamp(0.0, 1.0) * nonzero as f64) as usize
        } else {
            0
        };

        let eligible: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(|(index, candidate)| {
                is_eligible(candidate, &self.config) && positions[*index] < max_position
            })
            .map(|(index, _)| index)
            .collect();

        let selected = if self.config.thompson_sampling {
            pick_thompson(&eligible, candidates, scores, &self.config, rng)
        } else {
            pick_by_score(&eligible, scores)
        };

        let Some(selected) = selected else {
            return scores.to_vec();
        };
        let mut effective = scores.to_vec();
        effective[selected] = effective[selected].max(target);
        effective
    }
}

fn positions_among_nonzero(scores: &[f64]) -> (Vec<usize>, usize) {
    let mut order: Vec<usize> = scores
        .iter()
        .enumerate()
        .filter(|(_, score)| **score != 0.0)
        .map(|(index, _)| index)
        .collect();
    order.sort_by(|&left, &right| {
        scores[right]
            .total_cmp(&scores[left])
            .then(left.cmp(&right))
    });
    let nonzero = order.len();
    let mut positions = vec![usize::MAX; scores.len()];
    for (position, index) in order.into_iter().enumerate() {
        positions[index] = position;
    }
    (positions, nonzero)
}

fn target_score<R: Rng + ?Sized>(
    scores: &[f64],
    config: &ColdStartConfig,
    rng: &mut R,
) -> Option<f64> {
    let mut ranked = scores.to_vec();
    ranked.sort_by(|left, right| right.total_cmp(left));
    let upper = config.slot_max.min(ranked.len());
    let lower = config.slot_min.min(upper);
    (lower < upper).then(|| ranked[rng.random_range(lower..upper)])
}

fn is_eligible(candidate: &PostCandidate, config: &ColdStartConfig) -> bool {
    candidate.in_reply_to_tweet_id.is_none()
        && candidate.retweeted_tweet_id.is_none()
        && candidate
            .author_followers_count
            .is_some_and(|followers| i64::from(followers) <= config.follower_cap)
        && candidate
            .view_count
            .is_some_and(|views| views < config.impression_threshold)
        && duration_since_creation_opt(candidate.tweet_id)
            .is_some_and(|age| age <= config.max_post_age)
}

fn pick_by_score(eligible: &[usize], scores: &[f64]) -> Option<usize> {
    eligible.iter().copied().max_by(|&left, &right| {
        scores[left]
            .total_cmp(&scores[right])
            .then(left.cmp(&right))
    })
}

fn sample_reward<R: Rng + ?Sized>(
    candidate: &PostCandidate,
    config: &ColdStartConfig,
    rng: &mut R,
) -> f64 {
    let impressions = config.impression_scale * candidate.view_count.unwrap_or(0) as f64;
    let favorites = (candidate.favorite_count.unwrap_or(0).max(0) as f64).min(impressions);
    let alpha = config.beta_alpha0 + favorites;
    let beta = config.beta_beta0 + (impressions - favorites).max(0.0);
    Beta::new(alpha, beta)
        .map(|distribution| distribution.sample(rng))
        .unwrap_or(0.5)
}

fn pick_thompson<R: Rng + ?Sized>(
    eligible: &[usize],
    candidates: &[PostCandidate],
    scores: &[f64],
    config: &ColdStartConfig,
    rng: &mut R,
) -> Option<usize> {
    if eligible.is_empty() {
        return None;
    }
    if config.thompson_top_k == 0
        || config.beta_alpha0 <= 0.0
        || config.beta_beta0 <= 0.0
        || config.impression_scale <= 0.0
    {
        return pick_by_score(eligible, scores);
    }

    let mut sampled: Vec<(usize, f64)> = eligible
        .iter()
        .map(|&index| (index, sample_reward(&candidates[index], config, rng)))
        .collect();
    sampled.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    let top_k = config.thompson_top_k.min(sampled.len());
    let sampled_indices: Vec<usize> = sampled[..top_k].iter().map(|(index, _)| *index).collect();
    pick_by_score(&sampled_indices, scores)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use std::time::{SystemTime, UNIX_EPOCH};

    const TWITTER_EPOCH_MS: u64 = 1_288_834_974_657;

    fn minutes(value: u64) -> Duration {
        Duration::from_secs(value * 60)
    }

    fn tweet_id_with_age(age: Duration) -> u64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time after Unix epoch")
            .as_millis() as u64;
        ((now_ms - TWITTER_EPOCH_MS) << 22) - ((age.as_millis() as u64) << 22)
    }

    fn candidate(author_id: u64, age: Duration, views: Option<u64>) -> PostCandidate {
        PostCandidate {
            tweet_id: tweet_id_with_age(age),
            author_id,
            author_followers_count: Some(100),
            favorite_count: Some(0),
            view_count: views,
            ..Default::default()
        }
    }

    fn enabled_config() -> ColdStartConfig {
        ColdStartConfig {
            enabled: true,
            slot_min: 0,
            slot_max: 1,
            max_position_ratio: 1.0,
            ..Default::default()
        }
    }

    #[test]
    fn default_config_is_disabled() {
        let candidates = vec![candidate(1, minutes(10), Some(1))];
        assert_eq!(
            AuthorColdStart::default().apply(&candidates, &[10.0]),
            vec![10.0]
        );
    }

    #[test]
    fn boosts_one_eligible_low_impression_candidate_to_target_slot() {
        let candidates = vec![
            candidate(1, minutes(10), Some(2_000)),
            candidate(2, minutes(10), Some(10)),
        ];
        let cold_start = AuthorColdStart::new(enabled_config());

        assert_eq!(
            cold_start.apply(&candidates, &[100.0, 10.0]),
            vec![100.0, 100.0]
        );
    }

    #[test]
    fn missing_or_threshold_view_count_is_ineligible() {
        let candidates = vec![
            candidate(1, minutes(10), Some(2_000)),
            candidate(2, minutes(10), None),
            candidate(3, minutes(10), Some(1_000)),
        ];
        let cold_start = AuthorColdStart::new(enabled_config());

        assert_eq!(
            cold_start.apply(&candidates, &[100.0, 20.0, 10.0]),
            vec![100.0, 20.0, 10.0]
        );
    }

    #[test]
    fn replies_retweets_old_posts_and_large_authors_are_ineligible() {
        let mut reply = candidate(2, minutes(10), Some(10));
        reply.in_reply_to_tweet_id = Some(1);
        let mut retweet = candidate(3, minutes(10), Some(10));
        retweet.retweeted_tweet_id = Some(1);
        let mut old = candidate(4, Duration::from_secs(90_000), Some(10));
        old.author_followers_count = Some(100);
        let mut large_author = candidate(5, minutes(10), Some(10));
        large_author.author_followers_count = Some(1_001);
        let candidates = vec![
            candidate(1, minutes(10), Some(2_000)),
            reply,
            retweet,
            old,
            large_author,
        ];
        let cold_start = AuthorColdStart::new(enabled_config());

        assert_eq!(
            cold_start.apply(&candidates, &[100.0, 40.0, 30.0, 20.0, 10.0]),
            vec![100.0, 40.0, 30.0, 20.0, 10.0]
        );
    }

    #[test]
    fn position_ratio_limits_the_eligible_search_space() {
        let mut config = enabled_config();
        config.max_position_ratio = 0.5;
        let candidates = vec![
            candidate(1, minutes(10), Some(2_000)),
            candidate(2, minutes(10), Some(2_000)),
            candidate(3, minutes(10), Some(10)),
            candidate(4, minutes(10), Some(2_000)),
        ];
        let cold_start = AuthorColdStart::new(config);

        assert_eq!(
            cold_start.apply(&candidates, &[100.0, 40.0, 30.0, 20.0]),
            vec![100.0, 40.0, 30.0, 20.0]
        );
    }

    #[test]
    fn top_k_zero_falls_back_to_highest_ranking_score() {
        let mut config = enabled_config();
        config.thompson_sampling = true;
        config.thompson_top_k = 0;
        let cold_start = AuthorColdStart::new(config);
        let candidates = vec![
            candidate(1, minutes(10), Some(2_000)),
            candidate(2, minutes(10), Some(10)),
            candidate(3, minutes(10), Some(10)),
        ];

        assert_eq!(
            cold_start.apply(&candidates, &[100.0, 20.0, 10.0]),
            vec![100.0, 100.0, 10.0]
        );
    }

    #[test]
    fn thompson_top_one_prefers_uncertain_candidate() {
        let mut config = enabled_config();
        config.thompson_sampling = true;
        config.thompson_top_k = 1;
        let mut peaked = candidate(1, minutes(10), Some(10_000));
        peaked.favorite_count = Some(0);
        let uncertain = candidate(2, minutes(10), Some(0));
        let candidates = vec![peaked, uncertain];
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);

        assert_eq!(
            pick_thompson(&[0, 1], &candidates, &[100.0, 10.0], &config, &mut rng),
            Some(1)
        );
    }
}
