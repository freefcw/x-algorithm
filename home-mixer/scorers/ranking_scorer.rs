// 本地精排：把 Phoenix 各行为预测组合成最终分数。
//
// 结构对齐上游 `47c1bcd` home-mixer/scorers/ranking_scorer.rs：
// `ScoringWeights` + `compute_weighted_parts` + `offset_score` + 作者多样性 +
// 网外（OON）降权在一个 Scorer 内按序完成，旧 weighted/author_diversity/oon
// 三个独立 scorer 文件已随上游删除。
//
// 与上游的有记录差异：
// - 上游权重来自请求级 feature switches；本地读 `params::param` 常量（U1）。
// - 上游 `ValueModelMode` 支持 dwell_regret 两种模式、`EnableMpnScoring`
//   乘法归一路径、`AuthorColdStart` 冷启动加成与 `SlateContext` 持久化；
//   它们依赖本地尚无的数据源（曝光计数、请求级实验参数），本轮保持
//   weighted 默认路径，未实现项登记在 20260813 迁移文档（U3）。
// - 新增预测头（video_open/open_link/post_unexplored/active_secs_5m）字段
//   已进入 `PhoenixScores`；本地发布模型不输出时为 None，权重贡献为 0。

use crate::models::candidate::{PhoenixScores, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use crate::params::config::NEGATIVE_SCORES_OFFSET;
use crate::util::candidates_util;
use std::cmp::Ordering;
use std::collections::HashMap;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

pub(crate) struct ScoringWeights {
    favorite: f64,
    reply: f64,
    retweet: f64,
    photo_expand: f64,
    video_open: f64,
    click: f64,
    open_link: f64,
    profile_click: f64,
    vqv: f64,
    share: f64,
    share_via_dm: f64,
    share_via_copy_link: f64,
    dwell: f64,
    quote: f64,
    quoted_click: f64,
    quoted_vqv: f64,
    cont_dwell_time: f64,
    cont_click_dwell_time: f64,
    enable_click_dwell_low_fav_rate_penalty: bool,
    click_dwell_low_fav_rate_penalty_baseline: f64,
    click_dwell_low_fav_rate_penalty_alpha: f64,
    click_dwell_low_fav_rate_penalty_floor: f64,
    click_dwell_low_fav_rate_penalty_cap: f64,
    cont_active_secs_5m_residual_norm: f64,
    follow_author: f64,
    post_unexplored: f64,
    enable_multiplicative_post_unexplored: bool,
    multiplicative_post_unexplored_alpha: f64,
    post_unexplored_in_network_only: bool,
    not_interested: f64,
    block_author: f64,
    mute_author: f64,
    report: f64,
    not_dwelled: f64,
    negative_sum: f64,
    total_sum: f64,
    min_video_duration_ms: i32,
    enable_quoted_vqv_duration_check: bool,
    bidirectional_follow_reply_weight_boost: f64,
    bidirectional_follow_dwell_weight_boost: f64,
}

impl ScoringWeights {
    /// 上游 `from_params(&query.params)`；本地无请求级 FS，从参数常量构建。
    pub(crate) fn from_defaults() -> Self {
        let favorite = p::FAVORITE_WEIGHT;
        let reply = p::REPLY_WEIGHT;
        let retweet = p::RETWEET_WEIGHT;
        let photo_expand = p::PHOTO_EXPAND_WEIGHT;
        let video_open = p::VIDEO_OPEN_WEIGHT;
        let click = p::CLICK_WEIGHT;
        let open_link = p::OPEN_LINK_WEIGHT;
        let profile_click = p::PROFILE_CLICK_WEIGHT;
        let vqv = p::VQV_WEIGHT;
        let share = p::SHARE_WEIGHT;
        let share_via_dm = p::SHARE_VIA_DM_WEIGHT;
        let share_via_copy_link = p::SHARE_VIA_COPY_LINK_WEIGHT;
        let dwell = p::DWELL_WEIGHT;
        let quote = p::QUOTE_WEIGHT;
        let quoted_click = p::QUOTED_CLICK_WEIGHT;
        let quoted_vqv = p::QUOTED_VQV_WEIGHT;
        let follow_author = p::FOLLOW_AUTHOR_WEIGHT;
        let post_unexplored = p::POST_UNEXPLORED_WEIGHT;
        let enable_multiplicative_post_unexplored = p::ENABLE_MULTIPLICATIVE_POST_UNEXPLORED;
        let not_interested = p::NOT_INTERESTED_WEIGHT;
        let block_author = p::BLOCK_AUTHOR_WEIGHT;
        let mute_author = p::MUTE_AUTHOR_WEIGHT;
        let report = p::REPORT_WEIGHT;
        let not_dwelled = p::NOT_DWELLED_WEIGHT;

        let positive_sum = favorite
            + reply
            + retweet
            + photo_expand
            + video_open
            + click
            + open_link
            + profile_click
            + vqv
            + share
            + share_via_dm
            + share_via_copy_link
            + dwell
            + quote
            + quoted_click
            + quoted_vqv
            + follow_author
            + if enable_multiplicative_post_unexplored {
                0.0
            } else {
                post_unexplored
            };
        let negative_sum = -(not_interested + block_author + mute_author + report + not_dwelled);
        let total_sum = positive_sum + negative_sum;

        Self {
            favorite,
            reply,
            retweet,
            photo_expand,
            video_open,
            click,
            open_link,
            profile_click,
            vqv,
            share,
            share_via_dm,
            share_via_copy_link,
            dwell,
            quote,
            quoted_click,
            quoted_vqv,
            cont_dwell_time: p::CONT_DWELL_TIME_WEIGHT,
            cont_click_dwell_time: p::CONT_CLICK_DWELL_TIME_WEIGHT,
            enable_click_dwell_low_fav_rate_penalty: p::ENABLE_CLICK_DWELL_LOW_FAV_RATE_PENALTY,
            click_dwell_low_fav_rate_penalty_baseline: p::CLICK_DWELL_LOW_FAV_RATE_PENALTY_BASELINE,
            click_dwell_low_fav_rate_penalty_alpha: p::CLICK_DWELL_LOW_FAV_RATE_PENALTY_ALPHA,
            click_dwell_low_fav_rate_penalty_floor: p::CLICK_DWELL_LOW_FAV_RATE_PENALTY_FLOOR,
            click_dwell_low_fav_rate_penalty_cap: p::CLICK_DWELL_LOW_FAV_RATE_PENALTY_CAP,
            cont_active_secs_5m_residual_norm: p::CONT_ACTIVE_SECS_5M_RESIDUAL_NORM_WEIGHT,
            follow_author,
            post_unexplored,
            enable_multiplicative_post_unexplored,
            multiplicative_post_unexplored_alpha: p::MULTIPLICATIVE_POST_UNEXPLORED_ALPHA,
            post_unexplored_in_network_only: p::POST_UNEXPLORED_WEIGHT_IN_NETWORK_ONLY,
            not_interested,
            block_author,
            mute_author,
            report,
            not_dwelled,
            negative_sum,
            total_sum,
            min_video_duration_ms: p::MIN_VIDEO_DURATION_MS,
            enable_quoted_vqv_duration_check: p::ENABLE_QUOTED_VQV_DURATION_CHECK,
            bidirectional_follow_reply_weight_boost: p::BIDIRECTIONAL_FOLLOW_REPLY_WEIGHT_BOOST,
            bidirectional_follow_dwell_weight_boost: p::BIDIRECTIONAL_FOLLOW_DWELL_WEIGHT_BOOST,
        }
    }
}

impl ScoringWeights {
    fn post_unexplored_active_for(&self, candidate: &PostCandidate) -> bool {
        !self.post_unexplored_in_network_only || candidate.in_network == Some(true)
    }

    fn bidirectional_boost_eligible(candidate: &PostCandidate) -> bool {
        candidate.in_reply_to_tweet_id.is_none()
            && candidate.retweeted_tweet_id.is_none()
            && candidate.is_mutual_follow_author == Some(true)
    }

    fn reply_weight_for(&self, candidate: &PostCandidate) -> f64 {
        if self.bidirectional_follow_reply_weight_boost != 0.0
            && Self::bidirectional_boost_eligible(candidate)
        {
            return self.reply + self.bidirectional_follow_reply_weight_boost;
        }
        self.reply
    }

    fn low_fav_penalized_click_dwell(&self, scores: &PhoenixScores) -> Option<f64> {
        if !self.enable_click_dwell_low_fav_rate_penalty {
            return scores.click_dwell_time;
        }
        match (scores.click_dwell_time, scores.favorite_score) {
            (Some(cd), Some(fav)) => {
                let baseline = self
                    .click_dwell_low_fav_rate_penalty_baseline
                    .max(f64::EPSILON);
                let multiplier = (fav / baseline)
                    .powf(self.click_dwell_low_fav_rate_penalty_alpha)
                    .max(self.click_dwell_low_fav_rate_penalty_floor)
                    .min(self.click_dwell_low_fav_rate_penalty_cap);
                Some(cd * multiplier)
            }
            (cd, None) => cd,
            (None, _) => None,
        }
    }

    fn dwell_weight_for(&self, candidate: &PostCandidate) -> f64 {
        if self.bidirectional_follow_dwell_weight_boost != 0.0
            && Self::bidirectional_boost_eligible(candidate)
        {
            return self.dwell + self.bidirectional_follow_dwell_weight_boost;
        }
        self.dwell
    }

    /// 各头当前生效权重，用于调试与实验对照输出。
    /// 上游消费者是 PhoenixExperiments/Debug SideEffect；本地对应出口
    /// 尚未接入（见 20260813 迁移文档），先保留上游方法形状。
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn applied_weights_map(&self) -> HashMap<String, f64> {
        HashMap::from([
            ("favorite".to_string(), self.favorite),
            ("reply".to_string(), self.reply),
            ("retweet".to_string(), self.retweet),
            ("photo_expand".to_string(), self.photo_expand),
            ("video_open".to_string(), self.video_open),
            ("click".to_string(), self.click),
            ("open_link".to_string(), self.open_link),
            ("profile_click".to_string(), self.profile_click),
            ("vqv".to_string(), self.vqv),
            ("share".to_string(), self.share),
            ("share_via_dm".to_string(), self.share_via_dm),
            ("share_via_copy_link".to_string(), self.share_via_copy_link),
            ("dwell".to_string(), self.dwell),
            ("quote".to_string(), self.quote),
            ("quoted_click".to_string(), self.quoted_click),
            ("quoted_vqv".to_string(), self.quoted_vqv),
            ("dwell_time".to_string(), self.cont_dwell_time),
            ("click_dwell_time".to_string(), self.cont_click_dwell_time),
            ("follow_author".to_string(), self.follow_author),
            ("post_unexplored".to_string(), self.post_unexplored),
            ("not_interested".to_string(), self.not_interested),
            ("block_author".to_string(), self.block_author),
            ("mute_author".to_string(), self.mute_author),
            ("report".to_string(), self.report),
            ("not_dwelled".to_string(), self.not_dwelled),
        ])
    }
}

pub struct RankingScorer;

impl RankingScorer {
    /// `score` 是当前 viewer 的行为预测值，不是帖子的原始互动次数。
    fn apply(score: Option<f64>, weight: f64) -> f64 {
        score.unwrap_or(0.0) * weight
    }

    pub(crate) fn compute_weighted_score(
        weights: &ScoringWeights,
        query: &ScoredPostsQuery,
        candidate: &PostCandidate,
    ) -> f64 {
        let (pos, neg) = Self::compute_weighted_parts(weights, query, candidate);
        Self::offset_score(pos - neg, weights)
    }

    /// 逐头加权后按符号拆为（正项和，负项和的绝对值）。
    pub(crate) fn compute_weighted_parts(
        weights: &ScoringWeights,
        query: &ScoredPostsQuery,
        candidate: &PostCandidate,
    ) -> (f64, f64) {
        let scores: &PhoenixScores = &candidate.phoenix_scores;

        let vqv_weight = candidates_util::vqv_weight(
            query,
            candidate,
            weights.min_video_duration_ms,
            weights.vqv,
        );

        let quoted_vqv_weight = candidates_util::quoted_vqv_weight(
            candidate,
            weights.min_video_duration_ms,
            weights.quoted_vqv,
            weights.enable_quoted_vqv_duration_check,
        );

        let post_unexplored_active = weights.post_unexplored_active_for(candidate);

        let base_dwell_time_term = Self::apply(scores.dwell_time, weights.cont_dwell_time);
        let dwell_time_term = match scores.post_unexplored_score {
            Some(post_unexplored)
                if weights.enable_multiplicative_post_unexplored && post_unexplored_active =>
            {
                base_dwell_time_term
                    * (1.0 + post_unexplored * weights.multiplicative_post_unexplored_alpha)
            }
            _ => base_dwell_time_term,
        };

        let post_unexplored_term = if post_unexplored_active {
            Self::apply(scores.post_unexplored_score, weights.post_unexplored)
        } else {
            0.0
        };

        let terms = [
            Self::apply(scores.favorite_score, weights.favorite),
            Self::apply(scores.reply_score, weights.reply_weight_for(candidate)),
            Self::apply(scores.retweet_score, weights.retweet),
            Self::apply(scores.photo_expand_score, weights.photo_expand),
            Self::apply(scores.video_open_score, weights.video_open),
            Self::apply(scores.click_score, weights.click),
            Self::apply(scores.open_link_score, weights.open_link),
            Self::apply(scores.profile_click_score, weights.profile_click),
            Self::apply(scores.vqv_score, vqv_weight),
            Self::apply(scores.share_score, weights.share),
            Self::apply(scores.share_via_dm_score, weights.share_via_dm),
            Self::apply(
                scores.share_via_copy_link_score,
                weights.share_via_copy_link,
            ),
            Self::apply(scores.dwell_score, weights.dwell_weight_for(candidate)),
            Self::apply(scores.quote_score, weights.quote),
            Self::apply(scores.quoted_click_score, weights.quoted_click),
            Self::apply(scores.quoted_vqv_score, quoted_vqv_weight),
            dwell_time_term,
            Self::apply(
                weights.low_fav_penalized_click_dwell(scores),
                weights.cont_click_dwell_time,
            ),
            Self::apply(
                scores.active_secs_5m_residual_norm,
                weights.cont_active_secs_5m_residual_norm,
            ),
            Self::apply(scores.follow_author_score, weights.follow_author),
            Self::apply(scores.not_interested_score, weights.not_interested),
            Self::apply(scores.block_author_score, weights.block_author),
            Self::apply(scores.mute_author_score, weights.mute_author),
            Self::apply(scores.report_score, weights.report),
            Self::apply(scores.not_dwelled_score, weights.not_dwelled),
            if weights.enable_multiplicative_post_unexplored {
                0.0
            } else {
                post_unexplored_term
            },
        ];

        let mut pos = 0.0;
        let mut neg = 0.0;
        for t in terms {
            if t >= 0.0 {
                pos += t;
            } else {
                neg -= t;
            }
        }
        (pos, neg)
    }

    /// 把加权分数映射为非负值并保序：负分归一化进
    /// [0, NEGATIVE_SCORES_OFFSET)，正分整体抬高 NEGATIVE_SCORES_OFFSET。
    /// 非负分数是后续乘法调整（多样性衰减、网外降权）语义成立的前提。
    pub(crate) fn offset_score(combined_score: f64, w: &ScoringWeights) -> f64 {
        if w.total_sum == 0.0 {
            combined_score.max(0.0)
        } else if combined_score < 0.0 {
            (combined_score + w.negative_sum) / w.total_sum * NEGATIVE_SCORES_OFFSET
        } else {
            combined_score + NEGATIVE_SCORES_OFFSET
        }
    }

    fn diversity_multiplier(decay_factor: f64, floor: f64, exponent: f64) -> f64 {
        (1.0 - floor) * decay_factor.powf(exponent) + floor
    }

    /// 每个候选在同作者内按分数序的位次 k（最高分为 0）。
    /// 上游把该位次连同池内排名保存在 `SlateContext`；本地尚未引入
    /// SlateContext 持久化（依赖请求缓存复用打分），只计算位次本身。
    fn author_position_exponents(candidates: &[PostCandidate], scores: &[f64]) -> Vec<f64> {
        let mut ordered: Vec<usize> = (0..candidates.len()).collect();
        ordered.sort_by(|&a, &b| scores[b].partial_cmp(&scores[a]).unwrap_or(Ordering::Equal));

        let mut author_counts: HashMap<crate::models::UserId, usize> = HashMap::new();
        let mut exponents = vec![0.0; candidates.len()];
        for index in ordered {
            let entry = author_counts
                .entry(candidates[index].author_id)
                .or_insert(0);
            exponents[index] = *entry as f64;
            *entry += 1;
        }
        exponents
    }

    fn author_diversity_multipliers(candidates: &[PostCandidate], scores: &[f64]) -> Vec<f64> {
        let decay_factor = p::AUTHOR_DIVERSITY_DECAY;
        let floor = p::AUTHOR_DIVERSITY_FLOOR;
        Self::author_position_exponents(candidates, scores)
            .into_iter()
            .map(|exponent| Self::diversity_multiplier(decay_factor, floor, exponent))
            .collect()
    }

    fn apply_author_diversity(candidates: &[PostCandidate], scores: &[f64]) -> Vec<f64> {
        let multipliers = Self::author_diversity_multipliers(candidates, scores);
        scores
            .iter()
            .zip(multipliers)
            .map(|(&score, multiplier)| score * multiplier)
            .collect()
    }

    /// 请求级网外降权因子。上游还有新用户特判
    /// （NewUserAgeThresholdSecs>0 且关注数达标时用 NEW_USER_OON_WEIGHT_FACTOR）；
    /// 上游默认阈值为 0 即关闭，本地无账号创建时间数据源，同样不触发。
    fn effective_oon_weight(query: &ScoredPostsQuery) -> f64 {
        if !query.topic_ids.is_empty() {
            return p::TOPIC_OON_WEIGHT_FACTOR;
        }
        p::OON_WEIGHT_FACTOR
    }

    /// 候选是否应用网外降权：网外候选恒应用；网内的回复/转发在
    /// ENABLE_OON_RESCORE_FOR_IN_NETWORK_REPLIES_RETWEETS 下也应用。
    fn oon_applies(candidate: &PostCandidate) -> bool {
        match candidate.in_network {
            Some(false) => true,
            Some(true) => {
                p::ENABLE_OON_RESCORE_FOR_IN_NETWORK_REPLIES_RETWEETS
                    && (candidate.in_reply_to_tweet_id.is_some()
                        || candidate.retweeted_tweet_id.is_some())
            }
            None => false,
        }
    }
}

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for RankingScorer {
    async fn score(
        &self,
        query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let weights = ScoringWeights::from_defaults();
        let enable_author_diversity = p::ENABLE_AUTHOR_DIVERSITY;

        let weighted_scores: Vec<f64> = candidates
            .iter()
            .map(|c| Self::compute_weighted_score(&weights, query, c))
            .collect();

        let diversity_adjusted = if enable_author_diversity {
            Self::apply_author_diversity(candidates, &weighted_scores)
        } else {
            weighted_scores.clone()
        };

        let effective_oon = Self::effective_oon_weight(query);
        let final_scores: Vec<f64> = candidates
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let after_diversity = diversity_adjusted[i];
                if Self::oon_applies(c) {
                    after_diversity * effective_oon
                } else {
                    after_diversity
                }
            })
            .collect();

        weighted_scores
            .iter()
            .zip(final_scores)
            .map(|(&weighted, score)| {
                Ok(PostCandidate {
                    weighted_score: Some(weighted),
                    score: Some(score),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.weighted_score = scored.weighted_score;
        candidate.score = scored.score;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weights() -> ScoringWeights {
        ScoringWeights::from_defaults()
    }

    fn score_all(query: &ScoredPostsQuery, candidates: &[PostCandidate]) -> Vec<PostCandidate> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        runtime
            .block_on(RankingScorer.score(query, candidates))
            .into_iter()
            .map(|result| result.expect("ranking result"))
            .collect()
    }

    #[test]
    fn positive_candidate_outranks_negative_candidate() {
        let liked = PostCandidate {
            phoenix_scores: PhoenixScores {
                favorite_score: Some(0.9),
                ..Default::default()
            },
            ..Default::default()
        };
        let reported = PostCandidate {
            phoenix_scores: PhoenixScores {
                report_score: Some(0.9),
                ..Default::default()
            },
            ..Default::default()
        };

        let w = weights();
        let query = ScoredPostsQuery::default();
        let liked_score = RankingScorer::compute_weighted_score(&w, &query, &liked);
        let reported_score = RankingScorer::compute_weighted_score(&w, &query, &reported);

        assert!(liked_score > reported_score);
        assert!(reported_score >= 0.0);
    }

    #[test]
    fn offset_score_maps_negatives_into_offset_band_and_preserves_order() {
        let w = weights();
        let very_bad = RankingScorer::offset_score(-100.0, &w);
        let bad = RankingScorer::offset_score(-1.0, &w);
        let neutral = RankingScorer::offset_score(0.0, &w);
        let good = RankingScorer::offset_score(5.0, &w);

        assert!(very_bad < bad);
        assert!(bad < neutral);
        assert!(neutral < good);
        assert_eq!(neutral, NEGATIVE_SCORES_OFFSET);
        assert!(bad >= 0.0);
        assert!(bad < NEGATIVE_SCORES_OFFSET);
    }

    #[test]
    fn quoted_vqv_weight_is_flat_when_duration_check_disabled() {
        // 上游真值：QuotedVqvWeight=0 且时长检查关闭，任何引用帖视频不改变分数。
        let with_quoted_video = PostCandidate {
            quoted_video_duration_ms: Some(60_000),
            phoenix_scores: PhoenixScores {
                quoted_vqv_score: Some(1.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let w = weights();
        let query = ScoredPostsQuery::default();
        let score = RankingScorer::compute_weighted_score(&w, &query, &with_quoted_video);
        assert_eq!(score, NEGATIVE_SCORES_OFFSET);
    }

    #[test]
    fn video_open_and_open_link_heads_are_weighted_into_score() {
        let candidate = PostCandidate {
            phoenix_scores: PhoenixScores {
                video_open_score: Some(1.0),
                open_link_score: Some(1.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let w = weights();
        let query = ScoredPostsQuery::default();
        let score = RankingScorer::compute_weighted_score(&w, &query, &candidate);
        let expected = p::VIDEO_OPEN_WEIGHT + p::OPEN_LINK_WEIGHT + NEGATIVE_SCORES_OFFSET;
        assert!((score - expected).abs() < 1e-9);
    }

    #[test]
    fn not_dwelled_probability_applies_negative_weight() {
        let neutral = PostCandidate::default();
        let not_dwelled = PostCandidate {
            phoenix_scores: PhoenixScores {
                not_dwelled_score: Some(1.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let w = weights();
        let query = ScoredPostsQuery::default();
        let neutral_score = RankingScorer::compute_weighted_score(&w, &query, &neutral);
        let not_dwelled_score = RankingScorer::compute_weighted_score(&w, &query, &not_dwelled);

        assert_eq!(neutral_score, NEGATIVE_SCORES_OFFSET);
        assert!(not_dwelled_score < neutral_score);
        assert!(not_dwelled_score >= 0.0);
    }

    #[test]
    fn bidirectional_boost_applies_only_to_mutual_original_posts() {
        let base_scores = PhoenixScores {
            reply_score: Some(1.0),
            ..Default::default()
        };
        let mutual_original = PostCandidate {
            is_mutual_follow_author: Some(true),
            phoenix_scores: base_scores.clone(),
            ..Default::default()
        };
        let mutual_reply = PostCandidate {
            is_mutual_follow_author: Some(true),
            in_reply_to_tweet_id: Some(1.into()),
            phoenix_scores: base_scores.clone(),
            ..Default::default()
        };
        let unknown_relationship = PostCandidate {
            phoenix_scores: base_scores,
            ..Default::default()
        };

        let w = weights();
        let query = ScoredPostsQuery::default();
        let boosted = RankingScorer::compute_weighted_score(&w, &query, &mutual_original);
        let reply_not_boosted = RankingScorer::compute_weighted_score(&w, &query, &mutual_reply);
        let default_weighted =
            RankingScorer::compute_weighted_score(&w, &query, &unknown_relationship);

        let expected_boosted =
            p::REPLY_WEIGHT + p::BIDIRECTIONAL_FOLLOW_REPLY_WEIGHT_BOOST + NEGATIVE_SCORES_OFFSET;
        assert!((boosted - expected_boosted).abs() < 1e-9);
        assert!((reply_not_boosted - (p::REPLY_WEIGHT + NEGATIVE_SCORES_OFFSET)).abs() < 1e-9);
        assert_eq!(reply_not_boosted, default_weighted);
    }

    #[test]
    fn applies_author_diversity_decay_in_score_order() {
        let query = ScoredPostsQuery::default();
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                author_id: 10.into(),
                in_network: Some(true),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.8),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                author_id: 10.into(),
                in_network: Some(true),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.7),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let scored = score_all(&query, &candidates);
        let first = &scored[0];
        let second = &scored[1];

        // 同作者第二条按 (1-floor)*decay^1+floor 衰减。
        let expected_multiplier = (1.0 - p::AUTHOR_DIVERSITY_FLOOR) * p::AUTHOR_DIVERSITY_DECAY
            + p::AUTHOR_DIVERSITY_FLOOR;
        assert_eq!(first.score, first.weighted_score);
        let second_expected = second.weighted_score.expect("weighted") * expected_multiplier;
        assert!((second.score.expect("score") - second_expected).abs() < 1e-9);
        assert!(first.score > second.score);
    }

    #[test]
    fn applies_oon_discount_to_out_of_network() {
        let query = ScoredPostsQuery::default();
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                author_id: 10.into(),
                in_network: Some(true),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                author_id: 20.into(),
                in_network: Some(false),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let scored = score_all(&query, &candidates);
        let in_network = &scored[0];
        let out_of_network = &scored[1];

        assert_eq!(in_network.weighted_score, out_of_network.weighted_score);
        let expected_oon = out_of_network.weighted_score.expect("weighted") * p::OON_WEIGHT_FACTOR;
        assert!((out_of_network.score.expect("score") - expected_oon).abs() < 1e-9);
        assert_eq!(in_network.score, in_network.weighted_score);
    }

    #[test]
    fn in_network_replies_and_retweets_also_get_oon_rescore() {
        // 上游 47c1bcd 行为：EnableOonRescoreForInNetworkRepliesRetweets 默认开，
        // 网内的回复/转发同样乘 OON 因子；网内原创帖不受影响。
        let query = ScoredPostsQuery::default();
        let candidates = vec![
            PostCandidate {
                tweet_id: 1.into(),
                author_id: 10.into(),
                in_network: Some(true),
                in_reply_to_tweet_id: Some(99.into()),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2.into(),
                author_id: 20.into(),
                in_network: Some(true),
                retweeted_tweet_id: Some(98.into()),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3.into(),
                author_id: 30.into(),
                in_network: Some(true),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let scored = score_all(&query, &candidates);
        for candidate in &scored[0..2] {
            let expected = candidate.weighted_score.expect("weighted") * p::OON_WEIGHT_FACTOR;
            assert!((candidate.score.expect("score") - expected).abs() < 1e-9);
        }
        assert_eq!(scored[2].score, scored[2].weighted_score);
    }

    #[test]
    fn topic_feed_uses_topic_oon_weight_factor() {
        let make_candidates = || {
            vec![PostCandidate {
                tweet_id: 1.into(),
                author_id: 10.into(),
                in_network: Some(false),
                phoenix_scores: PhoenixScores {
                    favorite_score: Some(0.5),
                    ..Default::default()
                },
                ..Default::default()
            }]
        };

        let default_scored = score_all(&ScoredPostsQuery::default(), &make_candidates());
        let topic_query = ScoredPostsQuery {
            topic_ids: vec![10],
            ..Default::default()
        };
        let topic_scored = score_all(&topic_query, &make_candidates());

        let weighted = default_scored[0].weighted_score.expect("weighted");
        assert!(
            (default_scored[0].score.expect("score") - weighted * p::OON_WEIGHT_FACTOR).abs()
                < 1e-9
        );
        assert!(
            (topic_scored[0].score.expect("score") - weighted * p::TOPIC_OON_WEIGHT_FACTOR).abs()
                < 1e-9
        );
    }

    #[test]
    fn supplemental_topics_keep_generic_oon_factor() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            ..Default::default()
        };
        let candidates = vec![PostCandidate {
            tweet_id: 1.into(),
            author_id: 10.into(),
            in_network: Some(false),
            phoenix_scores: PhoenixScores {
                favorite_score: Some(0.5),
                ..Default::default()
            },
            ..Default::default()
        }];

        let scored = score_all(&query, &candidates);
        let weighted = scored[0].weighted_score.expect("weighted");
        assert!((scored[0].score.expect("score") - weighted * p::OON_WEIGHT_FACTOR).abs() < 1e-9);
    }

    #[test]
    fn applied_weights_map_reports_upstream_defaults() {
        let map = weights().applied_weights_map();
        assert_eq!(map["reply"], 5.0);
        assert_eq!(map["share_via_copy_link"], 20.0);
        assert_eq!(map["report"], -234.0);
    }
}
