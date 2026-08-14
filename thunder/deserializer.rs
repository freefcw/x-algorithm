// Kafka 事件反序列化，对齐上游 `47c1bcd` thunder/deserializer.rs。
//
// v2（proto InNetworkEvent）是本地默认管道；v1（Thrift TweetEvent/Event）
// 依赖 `schema` 模块与 thrift crate，由 `legacy` feature 启用。
// 差异（U1）：上游 v2 用私有 `xai_thunder_proto::InNetworkEvent`，本地
// 使用公开 proto 生成的 `x_algorithm_proto::thunder::InNetworkEvent`。

use anyhow::{Context, Result};
use prost::Message;
use x_algorithm_proto::thunder::InNetworkEvent;

#[cfg(feature = "legacy")]
use crate::schema::{events::Event, tweet_events::TweetEvent};
#[cfg(feature = "legacy")]
use thrift::protocol::{TBinaryInputProtocol, TSerializable};

#[cfg(feature = "legacy")]
pub fn deserialize_tweet_event(payload: &[u8]) -> Result<TweetEvent> {
    let mut cursor = std::io::Cursor::new(payload);
    let mut protocol = TBinaryInputProtocol::new(&mut cursor, true);

    TweetEvent::read_from_in_protocol(&mut protocol).context("Failed to deserialize TweetEvent")
}

#[cfg(feature = "legacy")]
pub fn deserialize_event(payload: &[u8]) -> Result<Event> {
    let mut cursor = std::io::Cursor::new(payload);
    let mut protocol = TBinaryInputProtocol::new(&mut cursor, true);

    Event::read_from_in_protocol(&mut protocol).context("Failed to deserialize Event")
}

/// Deserialize a proto binary message into InNetworkEvent (v2 管道)
pub fn deserialize_tweet_event_v2(payload: &[u8]) -> Result<InNetworkEvent> {
    InNetworkEvent::decode(payload).context("Failed to deserialize InNetworkEvent")
}
