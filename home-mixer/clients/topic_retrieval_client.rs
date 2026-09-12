use crate::models::ids::{ObjectId, PostId, UserId};
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

pub struct DemoTopicRetrievalClient;

#[async_trait]
impl TopicRetrievalClient for DemoTopicRetrievalClient {
    async fn retrieve(
        &self,
        _user_id: UserId,
        topic_ids: &[i64],
        max_results: usize,
    ) -> Result<Vec<TopicPost>, String> {
        if topic_ids.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = x_algorithm_proto::demo::now_ms();
        Ok((0..max_results)
            .filter_map(|index| {
                let topic_id = topic_ids[index % topic_ids.len()];
                let index_i64 = i64::try_from(index).ok()?;
                let ts_secs = u32::try_from(((now_ms - index_i64 * 1_000) / 1000).max(0)).ok()?;
                let tweet_id = ObjectId::from_parts(ts_secs, 1_000_000 + index as u64);
                let author_id = ObjectId::from_u64_be_padded(201 + (index as u64) % 40);
                Some(TopicPost {
                    tweet_id,
                    author_id,
                    matched_topic_ids: vec![topic_id],
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_client_returns_recent_candidates_for_requested_topics() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let posts = runtime
            .block_on(DemoTopicRetrievalClient.retrieve(crate::models::uid(42), &[10, 20], 4))
            .expect("topic candidates");

        assert_eq!(posts.len(), 4);
        assert_eq!(posts[0].matched_topic_ids, vec![10]);
        assert_eq!(posts[1].matched_topic_ids, vec![20]);
        assert!(posts.iter().all(|post| !post.tweet_id.is_nil()));
    }
}
