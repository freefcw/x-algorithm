use anyhow::{Context, Result};
use prost::Message;
use x_algorithm_proto::thunder::InNetworkEvent;

/// Deserialize a proto binary message into InNetworkEvent (v2 管道)
pub fn deserialize_tweet_event_v2(payload: &[u8]) -> Result<InNetworkEvent> {
    InNetworkEvent::decode(payload).context("Failed to deserialize InNetworkEvent")
}

// NOTE: v1 Thrift 反序列化 (deserialize_tweet_event / deserialize_event)
// 已随 schema 模块一起移除。如需恢复，启用 `legacy` feature 并重建 schema 模块。
