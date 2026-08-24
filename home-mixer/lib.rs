// Home Mixer — 推荐 Feed 流编排主服务
//
// 本 crate 是 X 推荐算法的编排层入口，负责：
//   1. 接收客户端请求，获取用户特征
//   2. 向 Thunder 和 Phoenix 发起并行召回
//   3. 对候选帖子进行过滤、打分聚合
//   4. 返回最终排序后的 Feed 流

pub mod ads;
pub mod business_feed;
pub mod candidate_hydrators;
pub mod candidate_pipeline;
pub mod clients;
mod debug_access;
pub mod demo;
pub mod feature_policy;
pub mod feed_state;
pub mod feed_stats;
mod filters;
pub mod for_you_server;
pub mod models;
pub mod params;
pub mod query_builder;
pub mod query_hydrators;
pub mod runtime_config;
pub mod scored_posts_server;
pub mod scorers;
pub mod selectors;
pub mod server;
pub mod side_effects;
pub mod sources;
pub mod util;

// 内联替代模块（替代原始 xai_* 私有依赖）
pub mod post_text;
pub mod recsys_compat;
pub mod uas_compat;
pub mod visibility;

pub use candidate_pipeline::phoenix_candidate_pipeline::{
    PhoenixCandidatePipeline, TopicPersonalizationClients,
};
pub use feature_policy::HomeMixerFeatures;
pub use server::{HomeMixerConfig, HomeMixerServer};
