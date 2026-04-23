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
// 当前为 stub 实现，后续可根据你平台的用户行为数据特点
// 实现更精细的过滤和聚合策略。

use crate::uas_compat::{AggregatedUserAction, UserAction};

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
    /// * `_extra_param` - 保留参数（原始实现中用于 experiment bucket）
    ///
    /// # Returns
    /// 聚合后的行为列表
    fn run(
        &self,
        actions: &[UserAction],
        window_time_ms: u64,
        _extra_param: i32,
    ) -> Vec<AggregatedUserAction>;

    /// 聚合器名称（用于追踪和调试）
    fn name(&self) -> &'static str;
}

/// 默认聚合器
///
/// 按帖子 ID 分组，将同一帖子上的所有行为合并为一条 AggregatedUserAction。
/// 对于 Stub 实现，直接将每条 UserAction 转为一条 AggregatedUserAction。
///
/// 原始 X 实现中的聚合逻辑更复杂：
///   - 行为时间窗口验证
///   - 时间戳对齐（使用曝光时间作为锚点）
///   - 行为权重衰减（近期行为 > 远期行为）
pub struct DefaultAggregator;

impl UserActionAggregator for DefaultAggregator {
    fn run(
        &self,
        actions: &[UserAction],
        _window_time_ms: u64,
        _extra_param: i32,
    ) -> Vec<AggregatedUserAction> {
        // Stub: 将每条 UserAction 直接转为 AggregatedUserAction
        actions
            .iter()
            .map(|action| AggregatedUserAction {
                tweet_id: action.tweet_id,
                author_id: action.author_id,
                impressed_time_ms: action.action_time_ms,
                action_mask: vec![],
                product_surface: None,
            })
            .collect()
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
/// 默认的预聚合过滤器，保留所有原始行为不做过滤。
///
/// 原始 X 实现中，此过滤器会：
///   1. 丢弃来自已知机器人账号的行为
///   2. 只保留"原始"行为（用户主动发起的，而非程序化的）
///   3. 去除 24 小时内对同一帖子的重复行为
pub struct KeepOriginalUserActionFilter;

impl KeepOriginalUserActionFilter {
    pub fn new() -> Self {
        Self
    }
}

impl UserActionFilter for KeepOriginalUserActionFilter {
    fn run(&self, actions: Vec<UserAction>) -> Vec<UserAction> {
        // Stub: 保留所有行为
        actions
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
pub struct DenseAggregatedActionFilter;

impl DenseAggregatedActionFilter {
    pub fn new() -> Self {
        Self
    }
}

impl AggregatedActionFilter for DenseAggregatedActionFilter {
    fn run(&self, actions: Vec<AggregatedUserAction>) -> Vec<AggregatedUserAction> {
        // Stub: 保留所有聚合行为
        // 生产实现应过滤掉 action_mask 全为 false 的记录
        actions
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
