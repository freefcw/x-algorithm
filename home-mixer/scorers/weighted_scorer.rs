use crate::models::candidate::{PhoenixScores, PostCandidate};
use crate::models::query::ScoredPostsQuery;
use crate::params as p;
use crate::util::score_normalizer::normalize_score;
use tonic::async_trait;
use xai_candidate_pipeline::scorer::Scorer;

pub struct WeightedScorer;

#[async_trait]
impl Scorer<ScoredPostsQuery, PostCandidate> for WeightedScorer {
    async fn score(
        &self,
        _query: &ScoredPostsQuery,
        candidates: &[PostCandidate],
    ) -> Vec<Result<PostCandidate, String>> {
        let scored = candidates
            .iter()
            .map(|c| {
                let weighted_score = Self::compute_weighted_score(c);
                let normalized_weighted_score = normalize_score(c, weighted_score);

                Ok(PostCandidate {
                    weighted_score: Some(normalized_weighted_score),
                    ..Default::default()
                })
            })
            .collect();

        scored
    }

    fn update(&self, candidate: &mut PostCandidate, scored: PostCandidate) {
        candidate.weighted_score = scored.weighted_score;
    }
}

impl WeightedScorer {
    fn apply(score: Option<f64>, weight: f64) -> f64 {
        score.unwrap_or(0.0) * weight
    }

    fn compute_weighted_score(candidate: &PostCandidate) -> f64 {
        let s: &PhoenixScores = &candidate.phoenix_scores;

        let vqv_weight = Self::vqv_weight_eligibility(candidate);
        let quoted_vqv_weight = Self::quoted_vqv_weight_eligibility(candidate);

        let combined_score = Self::apply(s.favorite_score, p::FAVORITE_WEIGHT)
            + Self::apply(s.reply_score, p::REPLY_WEIGHT)
            + Self::apply(s.retweet_score, p::RETWEET_WEIGHT)
            + Self::apply(s.photo_expand_score, p::PHOTO_EXPAND_WEIGHT)
            + Self::apply(s.click_score, p::CLICK_WEIGHT)
            + Self::apply(s.profile_click_score, p::PROFILE_CLICK_WEIGHT)
            + Self::apply(s.vqv_score, vqv_weight)
            + Self::apply(s.share_score, p::SHARE_WEIGHT)
            + Self::apply(s.share_via_dm_score, p::SHARE_VIA_DM_WEIGHT)
            + Self::apply(s.share_via_copy_link_score, p::SHARE_VIA_COPY_LINK_WEIGHT)
            + Self::apply(s.dwell_score, p::DWELL_WEIGHT)
            + Self::apply(s.quote_score, p::QUOTE_WEIGHT)
            + Self::apply(s.quoted_click_score, p::QUOTED_CLICK_WEIGHT)
            + Self::apply(s.quoted_vqv_score, quoted_vqv_weight)
            + Self::apply(s.dwell_time, p::CONT_DWELL_TIME_WEIGHT)
            + Self::apply(s.click_dwell_time, p::CONT_CLICK_DWELL_TIME_WEIGHT)
            + Self::apply(s.follow_author_score, p::FOLLOW_AUTHOR_WEIGHT)
            + Self::apply(s.not_interested_score, p::NOT_INTERESTED_WEIGHT)
            + Self::apply(s.block_author_score, p::BLOCK_AUTHOR_WEIGHT)
            + Self::apply(s.mute_author_score, p::MUTE_AUTHOR_WEIGHT)
            + Self::apply(s.report_score, p::REPORT_WEIGHT)
            + Self::apply(s.not_dwelled_score, p::NOT_DWELLED_WEIGHT);

        Self::offset_score(combined_score)
    }

    fn vqv_weight_eligibility(candidate: &PostCandidate) -> f64 {
        if candidate
            .video_duration_ms
            .is_some_and(|ms| ms > p::MIN_VIDEO_DURATION_MS)
        {
            p::VQV_WEIGHT
        } else {
            0.0
        }
    }

    /// 引用帖 VQV 权重：与 VQV 同理，要求引用帖视频时长超过门槛。
    /// 可通过 ENABLE_QUOTED_VQV_DURATION_CHECK 关闭时长检查。
    fn quoted_vqv_weight_eligibility(candidate: &PostCandidate) -> f64 {
        if !p::ENABLE_QUOTED_VQV_DURATION_CHECK {
            return p::QUOTED_VQV_WEIGHT;
        }
        if candidate
            .quoted_video_duration_ms
            .is_some_and(|ms| ms > p::MIN_VIDEO_DURATION_MS)
        {
            p::QUOTED_VQV_WEIGHT
        } else {
            0.0
        }
    }

    /// 把加权分数映射为非负值，保持排序语义：
    ///
    /// - 负分（负向行为占优）归一化进 [0, NEGATIVE_SCORES_OFFSET)；
    /// - 正分整体抬高 NEGATIVE_SCORES_OFFSET，始终高于任何负分。
    ///
    /// 负分理论下界是 NEGATIVE_WEIGHTS_SUM（所有负向行为概率均为 1），
    /// 以它为分母做线性归一。分数非负也是后续乘法调整
    /// （作者多样性衰减、网外降权）语义成立的前提。
    fn offset_score(combined_score: f64) -> f64 {
        if p::WEIGHTS_SUM == 0.0 {
            combined_score.max(0.0)
        } else if combined_score < 0.0 {
            (combined_score - p::NEGATIVE_WEIGHTS_SUM) / -p::NEGATIVE_WEIGHTS_SUM
                * p::NEGATIVE_SCORES_OFFSET
        } else {
            combined_score + p::NEGATIVE_SCORES_OFFSET
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_vqv_requires_video_duration_above_the_threshold() {
        let at_threshold = PostCandidate {
            quoted_video_duration_ms: Some(p::MIN_VIDEO_DURATION_MS),
            phoenix_scores: PhoenixScores {
                quoted_vqv_score: Some(1.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let above_threshold = PostCandidate {
            quoted_video_duration_ms: Some(p::MIN_VIDEO_DURATION_MS + 1),
            ..at_threshold.clone()
        };

        let ineligible_score = WeightedScorer::compute_weighted_score(&at_threshold);
        let eligible_score = WeightedScorer::compute_weighted_score(&above_threshold);

        assert_eq!(ineligible_score, p::NEGATIVE_SCORES_OFFSET);
        assert_eq!(
            eligible_score,
            p::NEGATIVE_SCORES_OFFSET + p::QUOTED_VQV_WEIGHT
        );
    }

    #[test]
    fn not_dwelled_probability_applies_the_reserved_negative_weight() {
        let neutral = PostCandidate::default();
        let not_dwelled = PostCandidate {
            phoenix_scores: PhoenixScores {
                not_dwelled_score: Some(1.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let neutral_score = WeightedScorer::compute_weighted_score(&neutral);
        let not_dwelled_score = WeightedScorer::compute_weighted_score(&not_dwelled);

        assert_eq!(neutral_score, p::NEGATIVE_SCORES_OFFSET);
        assert_eq!(
            not_dwelled_score,
            WeightedScorer::offset_score(p::NOT_DWELLED_WEIGHT)
        );
        assert!(not_dwelled_score < neutral_score);
    }

    #[test]
    fn test_offset_score_negative_maps_into_offset_band() {
        // 负分必须落在 [0, NEGATIVE_SCORES_OFFSET) 区间，而不是被推得更负
        let s = WeightedScorer::offset_score(-1.0);
        assert!(s >= 0.0, "负分映射后必须非负，实际 {}", s);
        assert!(
            s < p::NEGATIVE_SCORES_OFFSET,
            "负分映射后必须低于正分底线 {}，实际 {}",
            p::NEGATIVE_SCORES_OFFSET,
            s
        );

        // 理论最低分（所有负向行为概率均为 1）映射到 0
        let floor = WeightedScorer::offset_score(p::NEGATIVE_WEIGHTS_SUM);
        assert!(floor.abs() < 1e-9, "理论最低分应映射为 0，实际 {}", floor);
    }

    #[test]
    fn test_offset_score_preserves_ordering() {
        // 越负的分数映射后仍然越小（保序），且任何正分都高于任何负分
        let very_bad = WeightedScorer::offset_score(-100.0);
        let bad = WeightedScorer::offset_score(-1.0);
        let neutral = WeightedScorer::offset_score(0.0);
        let good = WeightedScorer::offset_score(5.0);

        assert!(very_bad < bad);
        assert!(bad < neutral);
        assert!(neutral < good);
        assert_eq!(neutral, p::NEGATIVE_SCORES_OFFSET);
    }

    #[test]
    fn test_positive_candidate_outranks_negative_candidate() {
        // 业务语义：预测"会点赞"的帖子必须排在预测"会举报"的帖子前面
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

        let liked_score = WeightedScorer::compute_weighted_score(&liked);
        let reported_score = WeightedScorer::compute_weighted_score(&reported);

        assert!(
            liked_score > reported_score,
            "点赞候选 {} 应高于举报候选 {}",
            liked_score,
            reported_score
        );
        assert!(
            reported_score >= 0.0,
            "举报候选分数也必须非负（乘法降权的前提）"
        );
    }
}
