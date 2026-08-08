// Home Mixer 客户端模块
//
// 本模块包含 Home Mixer 与各上游微服务通信的客户端抽象和实现。
//
// 在 X 的原始架构中，Home Mixer 作为编排层需要调用多个内部微服务：
//   - Thunder: 网络内帖子的实时缓存（gRPC）
//   - Phoenix: 推荐模型精排预测和双塔召回（gRPC）
//   - Gizmoduck: 用户资料服务（获取 screen_name、粉丝数等）
//   - TES (Tweet Entity Service): 帖子实体服务（获取帖子文本、媒体等）
//   - Strato: 分布式缓存/存储（获取用户特征、关注列表等）
//   - UAS Fetcher: 用户行为序列获取器
//   - SocialGraph: 社交关系图谱服务
//
// 所有客户端都通过 trait 抽象，便于测试和替换实现。
// 当前均为 stub 实现，后续可逐步替换为你平台的真实微服务客户端。
//
// 原始代码中这些客户端被标记为 "Excluded from open source release for security reasons"，
// 因为它们包含内部服务地址、S2S 认证逻辑等敏感信息。

pub mod gizmoduck_client;
pub mod phoenix_prediction_client;
pub mod phoenix_retrieval_client;
pub mod s2s;
pub mod socialgraph_client;
pub mod strato_client;
pub mod thunder_client;
pub mod topic_retrieval_client;
pub mod tweet_entity_service_client;
pub mod uas_fetcher;
pub mod user_topic_reader;
