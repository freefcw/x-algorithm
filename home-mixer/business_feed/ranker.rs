use crate::business_feed::model::{BusinessCandidate, BusinessFeedItem, CandidateSource};
use std::collections::{HashMap, HashSet};

pub const DEFAULT_BUSINESS_FEED_PAGE_SIZE: usize = 35;
pub const MAX_BUSINESS_FEED_PAGE_SIZE: usize = 100;
pub const MAX_UPSTREAM_CANDIDATES: usize = 200;
pub const CANDIDATE_OVERFETCH_FACTOR: usize = 3;
pub const MAX_ITEMS_PER_CREATOR: usize = 2;

const FRESHNESS_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const ENGAGEMENT_HALF_LIFE_MS: f64 = 24.0 * 60.0 * 60.0 * 1000.0;
const NETWORK_BOOST: f64 = 2.0;
const FRESHNESS_WEIGHT: f64 = 1.5;
const ENGAGEMENT_WEIGHT: f64 = 0.2;
const SOURCE_SCORE_WEIGHT: f64 = 0.01;

#[derive(Clone, Debug, Default)]
pub struct BusinessRuleRanker;

impl BusinessRuleRanker {
    pub fn rank(
        &self,
        viewer_account_id: &str,
        seen_feed_ids: &[String],
        candidates: Vec<BusinessCandidate>,
        result_size: usize,
        now_ms: i64,
    ) -> Vec<BusinessFeedItem> {
        let seen: HashSet<&str> = seen_feed_ids.iter().map(String::as_str).collect();
        let mut unique = HashSet::new();
        let mut scored = candidates
            .into_iter()
            .filter(|candidate| {
                !candidate.content.feed_id.trim().is_empty()
                    && candidate.content.feed_id == candidate.reference.feed_id
                    && candidate.content.recommendation_eligible
                    && candidate.content.creator_account_id != viewer_account_id
                    && !seen.contains(candidate.content.feed_id.as_str())
                    && unique.insert(candidate.content.feed_id.clone())
            })
            .map(|candidate| score_candidate(candidate, now_ms))
            .collect::<Vec<_>>();

        scored.sort_by(|left, right| {
            right
                .item
                .score
                .total_cmp(&left.item.score)
                .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
                .then_with(|| left.item.feed_id.cmp(&right.item.feed_id))
        });

        select_balanced(scored, result_size)
            .into_iter()
            .enumerate()
            .map(|(position, mut scored)| {
                scored.item.position = position;
                scored.item
            })
            .collect()
    }
}

struct ScoredItem {
    item: BusinessFeedItem,
    created_at_ms: i64,
    creator_key: String,
}

fn score_candidate(candidate: BusinessCandidate, now_ms: i64) -> ScoredItem {
    let age_ms = now_ms
        .saturating_sub(candidate.content.created_at_ms)
        .max(0);
    let freshness = (1.0 - age_ms as f64 / FRESHNESS_WINDOW_MS as f64).clamp(0.0, 1.0);
    let engagement = non_negative(candidate.content.like_count)
        + 2.0 * non_negative(candidate.content.comment_count)
        + 3.0 * non_negative(candidate.content.gift_value);
    let decay = 0.5_f64.powf(age_ms as f64 / ENGAGEMENT_HALF_LIFE_MS);
    let engagement_score = engagement.ln_1p() * decay;
    let source_score = (candidate.reference.source_score.max(0) as f64).ln_1p();
    let network_boost = if candidate.reference.source == CandidateSource::Network {
        NETWORK_BOOST
    } else {
        0.0
    };
    let score = FRESHNESS_WEIGHT * freshness
        + network_boost
        + ENGAGEMENT_WEIGHT * engagement_score
        + SOURCE_SCORE_WEIGHT * source_score;

    let mut reasons = vec!["freshness"];
    if candidate.reference.source == CandidateSource::Network {
        reasons.push("followed_author");
    }
    if engagement > 0.0 {
        reasons.push("decayed_engagement");
    }

    let creator_key = if candidate.content.creator_member_id.trim().is_empty() {
        candidate.content.creator_account_id.clone()
    } else {
        candidate.content.creator_member_id.clone()
    };
    ScoredItem {
        item: BusinessFeedItem {
            feed_id: candidate.content.feed_id,
            creator_account_id: candidate.content.creator_account_id,
            creator_member_id: candidate.content.creator_member_id,
            score,
            source: candidate.reference.source,
            reason: reasons.join(","),
            position: 0,
        },
        created_at_ms: candidate.content.created_at_ms,
        creator_key,
    }
}

fn non_negative(value: i32) -> f64 {
    f64::from(value.max(0))
}

fn select_balanced(mut scored: Vec<ScoredItem>, result_size: usize) -> Vec<ScoredItem> {
    let mut selected = Vec::with_capacity(result_size.min(scored.len()));
    let mut creator_counts: HashMap<String, usize> = HashMap::new();
    let mut preferred_source = scored.first().map(|item| item.item.source);

    while selected.len() < result_size && !scored.is_empty() {
        let preferred_index = preferred_source.and_then(|source| {
            scored.iter().position(|item| {
                item.item.source == source
                    && creator_counts.get(&item.creator_key).copied().unwrap_or(0)
                        < MAX_ITEMS_PER_CREATOR
            })
        });
        let index = preferred_index.or_else(|| {
            scored.iter().position(|item| {
                creator_counts.get(&item.creator_key).copied().unwrap_or(0) < MAX_ITEMS_PER_CREATOR
            })
        });
        let Some(index) = index else {
            break;
        };
        let item = scored.remove(index);
        *creator_counts.entry(item.creator_key.clone()).or_default() += 1;
        preferred_source = Some(match item.item.source {
            CandidateSource::Network => CandidateSource::Fallback,
            CandidateSource::Fallback => CandidateSource::Network,
        });
        selected.push(item);
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::business_feed::model::{CandidateReference, RecommendationContent};

    const NOW: i64 = 10_000_000;

    fn candidate(
        feed_id: &str,
        account_id: &str,
        member_id: &str,
        source: CandidateSource,
        created_at_ms: i64,
        eligible: bool,
        likes: i32,
    ) -> BusinessCandidate {
        BusinessCandidate {
            reference: CandidateReference {
                feed_id: feed_id.to_string(),
                source,
                source_score: 0,
            },
            content: RecommendationContent {
                feed_id: feed_id.to_string(),
                creator_account_id: account_id.to_string(),
                creator_member_id: member_id.to_string(),
                created_at_ms,
                like_count: likes,
                comment_count: 0,
                gift_value: 0,
                recommendation_eligible: eligible,
            },
        }
    }

    #[test]
    fn filters_ineligible_seen_self_empty_and_duplicates() {
        let candidates = vec![
            candidate("", "a", "m1", CandidateSource::Fallback, NOW, true, 0),
            candidate("seen", "a", "m2", CandidateSource::Fallback, NOW, true, 0),
            candidate(
                "self",
                "viewer",
                "m3",
                CandidateSource::Fallback,
                NOW,
                true,
                0,
            ),
            candidate(
                "blocked",
                "a",
                "m4",
                CandidateSource::Fallback,
                NOW,
                false,
                0,
            ),
            candidate("ok", "a", "m5", CandidateSource::Fallback, NOW, true, 0),
            candidate("ok", "a", "m5", CandidateSource::Network, NOW, true, 0),
        ];

        let result = BusinessRuleRanker.rank("viewer", &["seen".to_string()], candidates, 10, NOW);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].feed_id, "ok");
    }

    #[test]
    fn explicit_time_network_and_engagement_drive_explainable_scores() {
        let result = BusinessRuleRanker.rank(
            "viewer",
            &[],
            vec![
                candidate("old", "a", "m1", CandidateSource::Fallback, 0, true, 0),
                candidate("hot", "b", "m2", CandidateSource::Fallback, NOW, true, 100),
                candidate("network", "c", "m3", CandidateSource::Network, NOW, true, 0),
            ],
            3,
            NOW,
        );

        assert_eq!(result[0].feed_id, "network");
        assert!(result[0].reason.contains("followed_author"));
        assert!(result
            .iter()
            .find(|item| item.feed_id == "hot")
            .unwrap()
            .reason
            .contains("decayed_engagement"));
        assert!(result.iter().all(|item| item.score.is_finite()));
    }

    #[test]
    fn balances_sources_and_caps_creator_member_or_account() {
        let result = BusinessRuleRanker.rank(
            "viewer",
            &[],
            vec![
                candidate("n1", "a", "same", CandidateSource::Network, NOW, true, 0),
                candidate(
                    "n2",
                    "a",
                    "same",
                    CandidateSource::Network,
                    NOW - 1,
                    true,
                    0,
                ),
                candidate(
                    "n3",
                    "a",
                    "same",
                    CandidateSource::Network,
                    NOW - 2,
                    true,
                    0,
                ),
                candidate("f1", "b", "", CandidateSource::Fallback, NOW, true, 0),
                candidate("f2", "b", "", CandidateSource::Fallback, NOW - 1, true, 0),
                candidate("f3", "b", "", CandidateSource::Fallback, NOW - 2, true, 0),
            ],
            6,
            NOW,
        );

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].source, CandidateSource::Network);
        assert_eq!(result[1].source, CandidateSource::Fallback);
        assert_eq!(
            result
                .iter()
                .filter(|item| item.creator_member_id == "same")
                .count(),
            2
        );
        assert_eq!(
            result
                .iter()
                .filter(|item| item.creator_account_id == "b")
                .count(),
            2
        );
        assert_eq!(
            result.iter().map(|item| item.position).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }
}
