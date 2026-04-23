// 分数归一化工具
//
// 对候选帖子的加权分数进行归一化处理。
//
// 原始 X 实现中的分数归一化考虑了以下因素:
//   1. 作者粉丝数的对数衰减 — 高粉账号的帖子自然获得更多互动，
//      需要进行归一化以给小号更公平的竞争机会
//   2. 帖子新鲜度调整 — 防止积累了大量互动信号的老帖垄断 Feed
//
// 当前使用简化版 stub 实现，直接返回原始分数。
// TODO: 根据你平台特征实现更精细的归一化策略

use crate::candidate_pipeline::candidate::PostCandidate;

/// 对候选帖子的加权分数进行归一化
///
/// # Arguments
/// * `candidate` - 候选帖子（可用于读取粉丝数、发布时间等特征）
/// * `weighted_score` - WeightedScorer 计算出的原始加权分数
///
/// # Returns
/// 归一化后的分数
///
/// # 归一化策略说明
/// 当前 stub 实现直接返回原始分数。
/// 生产环境可考虑:
///   - 对数粉丝归一化: score / log2(1 + followers_count)
///   - 帖子年龄衰减: score * exp(-age_hours / half_life_hours)
///   - 双重归一化: 先按粉丝数归一化，再做全局 percentile 归一化
pub fn normalize_score(_candidate: &PostCandidate, weighted_score: f64) -> f64 {
    // Stub 实现: 直接返回原始加权分数
    // 后续可根据平台数据分布调整
    weighted_score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_preserves_score() {
        let candidate = PostCandidate::default();
        let score = 42.5;
        assert_eq!(normalize_score(&candidate, score), score);
    }

    #[test]
    fn test_normalize_negative_score() {
        let candidate = PostCandidate::default();
        let score = -10.0;
        assert_eq!(normalize_score(&candidate, score), score);
    }
}
