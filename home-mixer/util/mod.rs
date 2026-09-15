// Home Mixer 工具模块
//
// 本模块提供 Home Mixer 管道中通用的辅助功能：
//   - request_util: 请求 ID 生成
//   - bloom_filter: 布隆过滤器（客户端已阅帖子去重）
//   - candidates_util: 候选帖子工具函数（关联帖 ID、VQV 有效权重）

pub mod bloom_filter;
pub mod candidates_util;
pub mod composition;
pub mod request_util;
