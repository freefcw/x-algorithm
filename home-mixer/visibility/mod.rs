// 可见性过滤模块 (Visibility Filtering)
//
// 替代原始 `xai_visibility_filtering` 私有 crate。
//
// 原始功能说明：
// X 的可见性过滤系统是一套多层内容安全审核管道，负责：
//   1. 对帖子进行安全分级（暴力、色情、仇恨言论等）
//   2. 根据用户的安全偏好和展示场景决定是否显示
//   3. 按不同安全级别（TimelineHome vs TimelineHomeRecommendations）
//      执行不同严格程度的过滤策略
//
// 当前为 stub 实现，所有帖子默认通过安全检查。
// TODO: 接入你平台的内容安全审核系统（第三方 API 或自研）

pub mod models;
pub mod vf_client;
