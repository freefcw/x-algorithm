// Home Mixer — 推荐 Feed 流编排主服务
//
// 本 crate 是 X 推荐算法的编排层入口，负责：
//   1. 接收客户端请求，获取用户特征
//   2. 向 Thunder 和 Phoenix 发起并行召回
//   3. 对候选帖子进行过滤、打分聚合
//   4. 返回最终排序后的 Feed 流

mod candidate_hydrators;
mod candidate_pipeline;
pub mod clients;
pub mod demo;
mod filters;
pub mod final_feed;
pub mod params;
mod query_hydrators;
pub mod scored_posts_server;
pub mod scorers;
mod selectors;
mod server;
mod side_effects;
mod sources;
pub mod util;

// 内联替代模块（替代原始 xai_* 私有依赖）
pub mod post_text;
pub mod recsys_compat;
pub mod uas_compat;
pub mod visibility;

pub use server::HomeMixerServer;
