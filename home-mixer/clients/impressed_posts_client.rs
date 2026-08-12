//! 已曝光帖子客户端端口（QH-05）。
//!
//! 上游 `query_hydrators/impressed_posts_query_hydrator.rs` 从曝光存储服务
//! 读取用户近期已曝光的帖子 ID。本地当前由请求方在公开 proto 中携带
//! `impressed_post_ids`；接入服务端曝光存储时，Adapter 实现该端口并在装配
//! 中显式决定服务端数据与请求数据的优先级。

use tonic::async_trait;

#[async_trait]
pub trait ImpressedPostsClient: Send + Sync {
    async fn get(&self, user_id: u64) -> Result<Vec<u64>, String>;
}
