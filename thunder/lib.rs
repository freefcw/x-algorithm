pub mod args;
pub mod config;
pub mod demo_seed;
pub mod deserializer;
pub mod http_server;
pub mod kafka;
pub mod kafka_utils;
pub mod metrics;
pub mod o2;
pub mod posts;
/// 上游 `47c1bcd` 开源的 Thrift 事件 schema（tweet/user/media 等），
/// 供 v1 legacy 管道反序列化使用；依赖 thrift crate，随 `legacy` 启用。
#[cfg(feature = "legacy")]
pub mod schema;
pub mod strato_client;
pub mod thunder_service;
