use crate::models::candidate::PostCandidate;
use crate::models::query::{ScoredPostsQuery, TopicRecallMode};
use std::collections::{HashMap, HashSet};
use xai_candidate_pipeline::filter::{Filter, FilterResult};

/// 话题过滤实验维度。
///
/// 不同实验下同一帖子可能携带不同的话题 ID 列表（例如人工标注 vs 帖子内容推断）。
/// filter 根据当前实验维度选择对应列表做 include/exclude 判断。
///
/// 本地 PostCandidate 只有 filtered_topic_ids 和 unfiltered_topic_ids 两个字段，
/// Unfiltered 取 unfiltered_topic_ids，其余维度暂取 filtered_topic_ids。
/// 接入完整实验数据后，可扩展 Candidate 字段以区分各维度列表。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum TopicFilteringExperiment {
    #[default]
    Unfiltered,
    CuratedV0,
    CuratedV0V1,
    PostBased90Pct,
    PostBased75Pct,
    PostBased50Pct,
}

#[allow(dead_code)]
impl TopicFilteringExperiment {
    pub fn parse(s: &str) -> Self {
        match s {
            "CuratedV0" => Self::CuratedV0,
            "CuratedV0V1" => Self::CuratedV0V1,
            "PostBased90Pct" => Self::PostBased90Pct,
            "PostBased75Pct" => Self::PostBased75Pct,
            "PostBased50Pct" => Self::PostBased50Pct,
            _ => Self::Unfiltered,
        }
    }
}

/// 按话题 ID 覆盖实验配置。
///
/// 解析格式 "topic_id=ExperimentId,topic_id=ExperimentId" 的字符串，
/// 当请求包含某个被覆盖的话题时，使用指定实验维度而非默认值。
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct TopicFilteringOverrideMap {
    overrides: HashMap<i64, TopicFilteringExperiment>,
}

#[allow(dead_code)]
impl TopicFilteringOverrideMap {
    pub fn parse(raw: &str) -> Self {
        let mut overrides = HashMap::new();
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            if let Some((topic_str, experiment_str)) = entry.split_once('=') {
                if let Ok(topic_id) = topic_str.trim().parse::<i64>() {
                    let experiment = TopicFilteringExperiment::parse(experiment_str.trim());
                    overrides.insert(topic_id, experiment);
                }
            }
        }
        Self { overrides }
    }

    pub fn resolve(
        &self,
        query_topic_ids: &[i64],
        default: TopicFilteringExperiment,
    ) -> TopicFilteringExperiment {
        for &tid in query_topic_ids {
            if let Some(&exp) = self.overrides.get(&tid) {
                return exp;
            }
        }
        default
    }
}

/// 话题分类层级扩展。
///
/// 上游在此处硬编码了 X 的话题分类体系（体育 -> 足球 -> 英超 等），
/// 这些 ID 不适用于其他平台。本地保留接口骨架，category_ids 返回 None，
/// expand 返回原始集合。接入平台话题分类数据后可实现具体映射。
pub struct TopicIdExpansion;

impl TopicIdExpansion {
    /// 返回某话题所属类别下的全部子话题 ID。
    /// 返回 None 表示该话题没有已知的分类层级，filter 按原始 ID 匹配。
    pub fn category_ids(_topic_id: i64) -> Option<&'static [i64]> {
        None
    }

    /// 将一组话题 ID 扩展为包含其分类层级的完整集合。
    pub fn expand(topic_ids: &HashSet<i64>) -> HashSet<i64> {
        let mut result = topic_ids.clone();
        for &tid in topic_ids {
            if let Some(children) = Self::category_ids(tid) {
                result.extend(children.iter().copied());
            }
        }
        result
    }
}

pub struct TopicIdsFilter;

impl Filter<ScoredPostsQuery, PostCandidate> for TopicIdsFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.topic_recall_mode() == TopicRecallMode::Strict || !query.excluded_topic_ids.is_empty()
    }

    fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> FilterResult<PostCandidate> {
        let recall_mode = query.topic_recall_mode();
        let included: HashSet<i64> = match recall_mode {
            TopicRecallMode::Strict => {
                TopicIdExpansion::expand(&query.selected_topic_ids().iter().copied().collect())
            }
            TopicRecallMode::None | TopicRecallMode::Blend | TopicRecallMode::ColdStart => {
                HashSet::new()
            }
        };
        let excluded: HashSet<i64> = query.excluded_topic_ids.iter().copied().collect();

        let (kept, removed) = candidates.into_iter().partition(|candidate| {
            let candidate_topics = TopicIdExpansion::expand(
                &candidate
                    .retrieval_topic_ids
                    .iter()
                    .chain(content_topics(candidate))
                    .copied()
                    .collect(),
            );
            let matches_included = candidate_topics
                .iter()
                .any(|topic| included.contains(topic));
            let includes_requested = match recall_mode {
                TopicRecallMode::Strict => matches_included,
                TopicRecallMode::None | TopicRecallMode::Blend | TopicRecallMode::ColdStart => true,
            };
            let includes_excluded = candidate_topics
                .iter()
                .any(|topic| excluded.contains(topic));
            includes_requested && !includes_excluded
        });
        FilterResult { kept, removed }
    }
}

/// 选择候选帖子的内容话题列表：有 filtered 用 filtered，否则用 unfiltered。
fn content_topics(candidate: &PostCandidate) -> &Vec<i64> {
    if candidate.filtered_topic_ids.is_empty() {
        &candidate.unfiltered_topic_ids
    } else {
        &candidate.filtered_topic_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_requested_topics_and_removes_excluded_topics() {
        let query = ScoredPostsQuery {
            topic_ids: vec![10, 20],
            excluded_topic_ids: vec![99],
            ..ScoredPostsQuery::test_default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                filtered_topic_ids: vec![10],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                filtered_topic_ids: vec![30],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3,
                filtered_topic_ids: vec![20, 99],
                ..Default::default()
            },
        ];
        let result = TopicIdsFilter.filter(&query, candidates);

        assert_eq!(
            result
                .kept
                .iter()
                .map(|candidate| candidate.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(1)]
        );
        assert_eq!(result.removed.len(), 2);
    }

    #[test]
    fn supplemental_topics_do_not_filter_out_standard_candidates() {
        let query = ScoredPostsQuery {
            supplemental_topic_ids: vec![10],
            excluded_topic_ids: vec![99],
            ..ScoredPostsQuery::test_default()
        };
        let candidates = vec![
            PostCandidate {
                tweet_id: 1,
                filtered_topic_ids: vec![10],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 2,
                filtered_topic_ids: vec![30],
                ..Default::default()
            },
            PostCandidate {
                tweet_id: 3,
                filtered_topic_ids: vec![99],
                ..Default::default()
            },
        ];

        let result = TopicIdsFilter.filter(&query, candidates);

        assert_eq!(
            result
                .kept
                .iter()
                .map(|candidate| candidate.tweet_id)
                .collect::<Vec<_>>(),
            vec![crate::models::pid(1), crate::models::pid(2)]
        );
        assert_eq!(result.removed.len(), 1);
    }

    #[test]
    fn new_user_topics_are_owned_by_the_dedicated_filter() {
        let query = ScoredPostsQuery {
            new_user_topic_ids: vec![10],
            ..ScoredPostsQuery::test_default()
        };

        assert!(!TopicIdsFilter.enable(&query));
    }

    #[test]
    fn override_map_resolves_experiment_by_topic() {
        let map = TopicFilteringOverrideMap::parse("10=CuratedV0,20=PostBased50Pct");
        assert_eq!(
            map.resolve(&[10], TopicFilteringExperiment::Unfiltered),
            TopicFilteringExperiment::CuratedV0
        );
        assert_eq!(
            map.resolve(&[20], TopicFilteringExperiment::Unfiltered),
            TopicFilteringExperiment::PostBased50Pct
        );
        assert_eq!(
            map.resolve(&[99], TopicFilteringExperiment::Unfiltered),
            TopicFilteringExperiment::Unfiltered
        );
        assert_eq!(
            TopicFilteringOverrideMap::default()
                .resolve(&[10], TopicFilteringExperiment::CuratedV0V1),
            TopicFilteringExperiment::CuratedV0V1
        );
    }

    #[test]
    fn experiment_parse_falls_back_to_unfiltered() {
        assert_eq!(
            TopicFilteringExperiment::parse("unknown"),
            TopicFilteringExperiment::Unfiltered
        );
        assert_eq!(
            TopicFilteringExperiment::parse("PostBased90Pct"),
            TopicFilteringExperiment::PostBased90Pct
        );
    }

    #[test]
    fn expansion_returns_original_set_when_no_category_data() {
        let ids: HashSet<i64> = [10, 20].into_iter().collect();
        let expanded = TopicIdExpansion::expand(&ids);
        assert_eq!(expanded, ids);
    }
}
