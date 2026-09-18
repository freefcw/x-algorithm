use crate::models::ids::{PostId, UserId};
use tonic::async_trait;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPost {
    pub tweet_id: PostId,
    pub author_id: UserId,
    pub matched_topic_ids: Vec<i64>,
}

#[async_trait]
pub trait TopicRetrievalClient: Send + Sync {
    async fn retrieve(
        &self,
        user_id: UserId,
        topic_ids: &[i64],
        max_results: usize,
    ) -> Result<Vec<TopicPost>, String>;
}
