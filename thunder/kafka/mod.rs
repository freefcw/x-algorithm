// v1 legacy listener（Thrift TweetEvent 流）在 schema 开源后仍依赖未开源的
// `xai_kafka` / `xai_thunder_proto`，用 `xai_internal_deps` cfg 保留在树上：
// 任何 feature 组合都不编译它，待本地 rdkafka 适配落地后再解锁（见
// docs/update/20260813.md P3 记录）。v2（proto InNetworkEvent）是默认管道。
#[cfg(all(feature = "legacy", xai_internal_deps))]
pub mod tweet_events_listener;
pub mod tweet_events_listener_v2;
pub mod utils;
