// 可见性过滤数据模型
//
// 替代原始 `xai_visibility_filtering::models` 模块。
//
// 这些类型用于表达内容安全审核的结果：
//   - FilteredReason: 为什么某条帖子被过滤
//   - SafetyResult: 安全检查的具体结果
//   - Action: 对被标记帖子应采取的动作（丢弃、降权、警告等）
//
// 在 Home Mixer 管道中的使用位置：
//   - VFCandidateHydrator: 调用 VF 客户端获取每条帖子的安全结果
//   - VFFilter: 根据安全结果决定是否丢弃帖子
//   - PostCandidate.visibility_decision: 存储帖子审核的明确状态

/// A missing response and an explicit allow are different business outcomes.
/// Filters use this state to apply the configured in-network/out-of-network
/// degradation policy without guessing from an Option value.
#[derive(Clone, Debug, Default)]
pub enum VisibilityDecision {
    #[default]
    Unchecked,
    Allowed,
    Restricted(FilteredReason),
    Unavailable(String),
}

/// 帖子被过滤的原因
///
/// 当一条帖子未通过内容安全检查时，会附上一个 FilteredReason，
/// 标明是被哪个审核策略标记以及应该如何处理。
#[derive(Clone, Debug)]
pub enum FilteredReason {
    /// 安全审核系统给出的结果
    /// 包含具体的安全检查结论和应采取的动作
    SafetyResult(SafetyResult),

    /// 通用过滤原因（用于非安全审核的其他过滤场景）
    /// 例如: 地区限制、年龄限制等
    GenericFiltered(String),
}

/// 安全审核结果
///
/// 由可见性过滤客户端（VFClient）返回，描述一条帖子
/// 在特定安全级别下的检查结论。
#[derive(Clone, Debug)]
pub struct SafetyResult {
    /// 应对该帖子采取的动作
    pub action: Action,
    /// 可选的审核描述文本
    pub description: Option<String>,
}

/// 对被标记帖子应采取的动作
///
/// 在 X 的原始系统中，动作类型影响帖子在不同场景下的展示方式：
/// - Drop: 完全不展示（最严格）
/// - Interstitial: 展示但附加警告遮罩，用户需手动点击查看
/// - Softintervention: 降级展示（降低排名但仍展示）
/// - Allow: 正常展示
#[derive(Clone, Debug)]
pub enum Action {
    /// 丢弃 — 不向用户展示此帖子
    /// 用于严重的安全违规（暴力、仇恨言论等）
    Drop(DropAction),

    /// 警告遮罩 — 展示帖子但附加可点击的安全警告
    /// 用于中等程度的敏感内容
    Interstitial,

    /// 软干预 — 降低帖子排名但仍展示
    /// 用于轻微敏感内容
    SoftIntervention,

    /// 允许 — 正常展示
    Allow,
}

/// Drop 动作的详细信息
#[derive(Clone, Debug, Default)]
pub struct DropAction {
    /// 丢弃原因代码
    pub reason_code: i32,
    /// 可读的丢弃原因描述
    pub description: String,
}

impl FilteredReason {
    /// 将 FilteredReason 转换为 proto 格式的 VisibilityFilteredReason
    ///
    /// 用于在 server.rs 中将内部可见性结果序列化到 gRPC 响应中
    pub fn into_proto(self) -> (i32, String) {
        match self {
            FilteredReason::SafetyResult(safety) => {
                let code = match &safety.action {
                    Action::Drop(drop_action) => drop_action.reason_code,
                    Action::Interstitial => 2,
                    Action::SoftIntervention => 3,
                    Action::Allow => 0,
                };
                let desc = safety
                    .description
                    .unwrap_or_else(|| "safety_filtered".to_string());
                (code, desc)
            }
            FilteredReason::GenericFiltered(reason) => (99, reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_proto_drop() {
        let reason = FilteredReason::SafetyResult(SafetyResult {
            action: Action::Drop(DropAction {
                reason_code: 404,
                description: "Drop reason".to_string(),
            }),
            description: Some("safety rule 1".to_string()),
        });
        let (code, desc) = reason.into_proto();
        assert_eq!(code, 404);
        assert_eq!(desc, "safety rule 1");
    }

    #[test]
    fn test_into_proto_interstitial() {
        let reason = FilteredReason::SafetyResult(SafetyResult {
            action: Action::Interstitial,
            description: None,
        });
        let (code, desc) = reason.into_proto();
        assert_eq!(code, 2);
        assert_eq!(desc, "safety_filtered");
    }

    #[test]
    fn test_into_proto_generic() {
        let reason = FilteredReason::GenericFiltered("geo_blocked".to_string());
        let (code, desc) = reason.into_proto();
        assert_eq!(code, 99);
        assert_eq!(desc, "geo_blocked");
    }
}
