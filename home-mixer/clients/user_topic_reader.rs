use crate::models::ids::UserId;
use tonic::async_trait;

#[async_trait]
pub trait UserTopicReader: Send + Sync {
    /// 返回 Adapter 已完成资格判断和策略选择后的补充话题。
    async fn get_supplemental_topic_ids(&self, user_id: UserId) -> Result<Vec<i64>, anyhow::Error>;
}

pub struct DemoUserTopicReader;

#[async_trait]
impl UserTopicReader for DemoUserTopicReader {
    async fn get_supplemental_topic_ids(
        &self,
        _user_id: UserId,
    ) -> Result<Vec<i64>, anyhow::Error> {
        Ok(vec![10, 20])
    }
}
