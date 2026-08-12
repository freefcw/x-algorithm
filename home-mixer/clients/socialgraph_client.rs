//! Social Graph 社交关系图谱客户端端口。
//!
//! SocialGraph 是 X 内部管理社交关系的微服务（关注、屏蔽、静音、社交
//! 距离）。本地主链路的屏蔽/静音/关注列表当前经由用户特征适配器在
//! Query 级补全（U1）；本端口对应上游 `SocialGraphClientOps` 中候选级
//! 反向屏蔽查询，服务上游同名 `BlockedByHydrator`（CH-09）。
//!
//! 真实 Adapter 需要负责认证、超时、错误语义与批量上限，之后
//! `BlockedByHydrator` 才能进入装配。

use std::collections::HashSet;
use tonic::async_trait;

#[async_trait]
pub trait SocialGraphClientOps: Send + Sync {
    /// 返回 `author_ids` 中反向屏蔽了 `viewer_id` 的作者集合。
    async fn check_blocked_by(
        &self,
        viewer_id: u64,
        author_ids: &[u64],
    ) -> Result<HashSet<u64>, String>;
}
