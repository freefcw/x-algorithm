// Strato 客户端 Stub 实现
//
// 原始 Thunder 通过 xai_strato 从分布式缓存获取用户的关注列表。
// 在 MVP 阶段，关注列表由调用方（Home Mixer）在请求中直接传入，
// 此处提供一个 stub 实现。后续可替换为 Redis 或你平台的关系服务。

use anyhow::Result;
use log::warn;

pub struct StratoClient;

impl StratoClient {
    pub fn new() -> Self {
        StratoClient
    }

    /// 获取指定用户的关注列表
    ///
    /// MVP stub：始终返回空列表。
    /// 在实际部署中，应从 Redis 或用户关系服务获取。
    pub async fn fetch_following_list(
        &self,
        user_id: i64,
        _max_results: i32,
    ) -> Result<Vec<i64>> {
        warn!(
            "StratoClient stub: fetch_following_list for user {} returning empty list. \
             Replace with actual data source.",
            user_id
        );
        Ok(vec![])
    }
}
