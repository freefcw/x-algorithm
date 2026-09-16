// 推荐系统聚合兼容层
//
// 替代原始 `xai_recsys_aggregation` 私有 crate。
//
// 原始功能说明：
// 该模块负责将用户的原始行为序列进行预处理，
// 转换为 Phoenix 精排模型可以消费的格式。处理流程：
//
//   原始行为列表 (UserAction[])
//     → [Pre-filter] 全局过滤器（去除无效行为、重复行为等）
//     → [Aggregate] 聚合器（按帖子 ID 聚合同一帖子上的多次行为）
//     → [Post-filter] 后置过滤器（去除稀疏记录、保留高信息量记录）
//     → 聚合后的行为序列 (AggregatedUserAction[])
//
// 每个步骤通过 trait 抽象，支持不同的策略实现。
//
// 当前实现负责结构校验、请求时间窗口、同帖行为合并和稳定排序；
// 平台特有的机器人识别与产品策略仍由业务 UAS 适配器负责。

use crate::uas_compat::{AggregatedUserAction, UserAction};
use std::collections::HashMap;

// =============================================================================
// 聚合器 (Aggregator)
// =============================================================================

/// 用户行为聚合器 trait
///
/// 负责将按时间排列的原始行为记录聚合为按帖子分组的聚合行为。
/// 聚合逻辑：将同一帖子上的多次互动行为合并为一条记录。
pub trait UserActionAggregator: Send + Sync {
    /// 执行聚合
    ///
    /// # Arguments
    /// * `actions` - 经过预过滤的原始行为列表
    /// * `window_time_ms` - 时间窗口（毫秒），只保留最近的行为
    /// * `reference_time_ms` - 窗口终点（当前请求时间）
    ///
    /// # Returns
    /// 聚合后的行为列表
    fn run(
        &self,
        actions: &[UserAction],
        window_time_ms: u64,
        reference_time_ms: i64,
    ) -> Vec<AggregatedUserAction>;

    /// 聚合器名称（用于追踪和调试）
    fn name(&self) -> &'static str;
}

/// 默认聚合器
///
/// 只保留请求窗口内的有效行为，按帖子 ID 合并 action mask，并按首次行为时间升序输出。
pub struct DefaultAggregator;

/// action_mask 的长度：覆盖当前发布模型使用的 proto ActionName 枚举 0..=18。
///
/// 这是行为类型范围的唯一真源：UAS 投影 job 的事件校验与这里的聚合校验
/// 都通过 [`is_supported_action_type`] 判定，扩枚举时只需改这一处。
pub const ACTION_MASK_LEN: usize = 19;

/// 当前发布模型接受的最大行为类型编号（含）。
pub const MAX_SUPPORTED_ACTION_TYPE: i32 = ACTION_MASK_LEN as i32 - 1;

/// 行为类型是否落在当前模型的 action_mask 范围内（0 = UNSPECIFIED 不接受）。
pub fn is_supported_action_type(action_type: i32) -> bool {
    (1..=MAX_SUPPORTED_ACTION_TYPE).contains(&action_type)
}

fn validated_action(
    action: &UserAction,
) -> Option<(crate::models::PostId, crate::models::UserId, i64, usize)> {
    let tweet_id = action.tweet_id.filter(|id| !id.is_nil())?;
    let author_id = action.author_id.filter(|id| !id.is_nil())?;
    let action_time_ms = action.action_time_ms.filter(|time| *time >= 0)?;
    let action_type = action
        .action_type
        .filter(|value| is_supported_action_type(*value))?;
    Some((tweet_id, author_id, action_time_ms, action_type as usize))
}

impl UserActionAggregator for DefaultAggregator {
    fn run(
        &self,
        actions: &[UserAction],
        window_time_ms: u64,
        reference_time_ms: i64,
    ) -> Vec<AggregatedUserAction> {
        if reference_time_ms <= 0 {
            return Vec::new();
        }
        let window_time_ms = i64::try_from(window_time_ms).unwrap_or(i64::MAX);
        let cutoff_time_ms = reference_time_ms.saturating_sub(window_time_ms);
        let mut grouped = HashMap::<crate::models::PostId, AggregatedUserAction>::new();

        for action in actions {
            let Some((tweet_id, author_id, action_time_ms, action_index)) =
                validated_action(action)
            else {
                continue;
            };
            if action_time_ms < cutoff_time_ms || action_time_ms > reference_time_ms {
                continue;
            }

            let aggregated = grouped
                .entry(tweet_id)
                .or_insert_with(|| AggregatedUserAction {
                    tweet_id: Some(tweet_id),
                    action_mask: vec![false; ACTION_MASK_LEN],
                    ..Default::default()
                });
            aggregated.action_mask[action_index] = true;
            if aggregated
                .impressed_time_ms
                .is_none_or(|time| action_time_ms < time)
            {
                aggregated.author_id = Some(author_id);
                aggregated.impressed_time_ms = Some(action_time_ms);
            }
        }

        let mut aggregated = grouped.into_values().collect::<Vec<_>>();
        aggregated.sort_by_key(|action| {
            (
                action.impressed_time_ms.unwrap_or(i64::MAX),
                action.tweet_id.unwrap_or_default(),
            )
        });
        aggregated
    }

    fn name(&self) -> &'static str {
        "DefaultAggregator"
    }
}

// =============================================================================
// 预聚合过滤器 (Pre-aggregation Filters)
// =============================================================================

/// 原始行为过滤器 trait
///
/// 在聚合之前对原始行为列表进行过滤。
/// 典型用途：
///   - 去除来自机器人或爬虫的行为
///   - 去除重复行为
///   - 只保留特定类型的行为
pub trait UserActionFilter: Send + Sync {
    fn run(&self, actions: Vec<UserAction>) -> Vec<UserAction>;
}

/// 保留原始行为过滤器
///
/// 默认的预聚合过滤器，保留身份、时间和行为类型都有效的记录。
///
/// 本地公开 UAS 合同当前可验证：
///   1. 帖子和作者 ID 已提供且不是 NIL
///   2. 行为时间非负
///   3. 行为类型属于当前发布模型使用的 ActionName 范围
#[derive(Default)]
pub struct KeepOriginalUserActionFilter;

impl KeepOriginalUserActionFilter {
    pub fn new() -> Self {
        Self
    }
}

impl UserActionFilter for KeepOriginalUserActionFilter {
    fn run(&self, actions: Vec<UserAction>) -> Vec<UserAction> {
        actions
            .into_iter()
            .filter(|action| validated_action(action).is_some())
            .collect()
    }
}

// =============================================================================
// 后聚合过滤器 (Post-aggregation Filters)
// =============================================================================

/// 聚合后行为过滤器 trait
///
/// 在聚合之后对聚合行为列表进行二次过滤。
/// 典型用途：
///   - 去除行为掩码全空的记录（用户只有曝光但无任何互动）
///   - 去除稀疏记录（互动行为太少，信息量不足）
pub trait AggregatedActionFilter: Send + Sync {
    fn run(&self, actions: Vec<AggregatedUserAction>) -> Vec<AggregatedUserAction>;
}

/// 稠密聚合行为过滤器
///
/// 过滤掉行为掩码中全为 false 的记录。
///
/// 原始 X 实现中，此过滤器检查 action_mask 中是否有至少一个
/// true 值（即用户对该帖子有过至少一次真实互动），
/// 如果没有则视为"仅曝光"记录，被过滤掉以减少噪音。
#[derive(Default)]
pub struct DenseAggregatedActionFilter;

impl DenseAggregatedActionFilter {
    pub fn new() -> Self {
        Self
    }
}

impl AggregatedActionFilter for DenseAggregatedActionFilter {
    fn run(&self, actions: Vec<AggregatedUserAction>) -> Vec<AggregatedUserAction> {
        actions
            .into_iter()
            .filter(|action| action.action_mask.iter().any(|active| *active))
            .collect()
    }
}

// =============================================================================
// 公共聚合模块重导出（兼容原始 use 路径）
// =============================================================================

/// 聚合子模块 — 兼容原始 `xai_recsys_aggregation::aggregation` 导入路径
pub mod aggregation {
    pub use super::{DefaultAggregator, UserActionAggregator};
}

/// 过滤器子模块 — 兼容原始 `xai_recsys_aggregation::filters` 导入路径
pub mod filters {
    pub use super::{
        AggregatedActionFilter, DenseAggregatedActionFilter, KeepOriginalUserActionFilter,
        UserActionFilter,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{pid, uid};

    use crate::models::{PostId, UserId};

    fn action(
        tweet_id: Option<PostId>,
        author_id: Option<UserId>,
        action_time_ms: Option<i64>,
        action_type: Option<i32>,
    ) -> UserAction {
        UserAction {
            tweet_id,
            author_id,
            action_time_ms,
            action_type,
        }
    }

    #[test]
    fn filters_window_groups_actions_and_sorts_by_first_event() {
        let actions = vec![
            action(Some(pid(2)), Some(uid(20)), Some(990), Some(6)),
            action(Some(pid(1)), Some(uid(10)), Some(970), Some(2)),
            action(Some(pid(3)), Some(uid(30)), Some(899), Some(1)),
            action(Some(pid(1)), Some(uid(10)), Some(930), Some(1)),
            action(Some(pid(4)), Some(uid(40)), Some(1_001), Some(1)),
            action(Some(pid(5)), Some(uid(50)), Some(980), Some(0)),
            action(None, Some(uid(60)), Some(980), Some(1)),
            action(Some(pid(6)), None, Some(980), Some(1)),
            action(Some(PostId::NIL), Some(uid(70)), Some(980), Some(1)),
            action(Some(pid(7)), Some(UserId::NIL), Some(980), Some(1)),
            action(Some(pid(8)), Some(uid(80)), Some(980), None),
            action(
                Some(pid(9)),
                Some(uid(90)),
                Some(980),
                Some(ACTION_MASK_LEN as i32),
            ),
            action(Some(pid(10)), Some(uid(100)), Some(-1), Some(1)),
        ];

        let actions = KeepOriginalUserActionFilter::new().run(actions);
        let actions = DefaultAggregator.run(&actions, 100, 1_000);
        let actions = DenseAggregatedActionFilter::new().run(actions);

        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].tweet_id, Some(pid(1)));
        assert_eq!(actions[0].author_id, Some(uid(10)));
        assert_eq!(actions[0].impressed_time_ms, Some(930));
        assert!(actions[0].action_mask[1]);
        assert!(actions[0].action_mask[2]);
        assert_eq!(actions[1].tweet_id, Some(pid(2)));
        assert_eq!(actions[1].impressed_time_ms, Some(990));
        assert!(actions[1].action_mask[6]);
    }

    #[test]
    fn orders_equal_impression_times_by_tweet_id() {
        let actions = vec![
            action(Some(pid(9)), Some(uid(9)), Some(500), Some(1)),
            action(Some(pid(3)), Some(uid(3)), Some(500), Some(1)),
        ];

        let actions = DefaultAggregator.run(&actions, 1_000, 1_000);

        assert_eq!(
            actions
                .iter()
                .map(|action| action.tweet_id)
                .collect::<Vec<_>>(),
            vec![Some(pid(3)), Some(pid(9))]
        );
    }

    #[test]
    fn nonpositive_reference_time_returns_empty() {
        let actions = vec![action(Some(pid(1)), Some(uid(1)), Some(0), Some(1))];

        assert!(DefaultAggregator.run(&actions, 1_000, 0).is_empty());
        assert!(DefaultAggregator.run(&actions, 1_000, -5).is_empty());
    }

    #[test]
    fn dense_filter_drops_actions_without_any_interaction() {
        let mut interacted_mask = vec![false; ACTION_MASK_LEN];
        interacted_mask[3] = true;
        let actions = vec![
            AggregatedUserAction {
                tweet_id: Some(pid(1)),
                author_id: Some(uid(1)),
                impressed_time_ms: Some(10),
                action_mask: vec![false; ACTION_MASK_LEN],
                ..Default::default()
            },
            AggregatedUserAction {
                tweet_id: Some(pid(2)),
                author_id: Some(uid(2)),
                impressed_time_ms: Some(20),
                action_mask: interacted_mask,
                ..Default::default()
            },
        ];

        let actions = DenseAggregatedActionFilter::new().run(actions);

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].tweet_id, Some(pid(2)));
    }
}
