//! 曝光 Bloom Filter 客户端端口（QH-06）。
//!
//! 上游 `query_hydrators/impression_bloom_filter_query_hydrator.rs` 按
//! Home Timeline surface 从内部服务读取 thrift Bloom Filter 分段并转换为
//! proto 条目。本地公开 proto 已定义 `ImpressionBloomFilterEntry`，因此端口
//! 直接返回该类型；surface 固定为 Home Timeline，由 Adapter 内部处理。

use tonic::async_trait;
use x_algorithm_proto::home_mixer::ImpressionBloomFilterEntry;

#[async_trait]
pub trait ImpressionBloomFilterClient: Send + Sync {
    async fn get(&self, user_id: u64) -> Result<Vec<ImpressionBloomFilterEntry>, String>;
}
