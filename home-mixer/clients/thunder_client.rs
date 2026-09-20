// Thunder 客户端
//
// 替代原始被阉割的 Thunder 客户端模块。
//
// 原始功能说明：
// ThunderClient 是 Home Mixer 与 Thunder 服务之间的 gRPC 客户端。
// Thunder 维护了用户关注列表中所有帖子的实时内存缓存，
// 提供亚毫秒级别的"网络内"帖子查询。
//
// 架构设计：
// - Thunder 部署在多个集群中（如 Amp、Pdisco 等），
//   每个集群可能有不同的数据分片策略
// - ThunderClient 维护到各集群的 gRPC 连接池
// - 使用随机负载均衡从连接池中选择连接
//
// 当前为简化实现，直接连接单一 Thunder 实例。
// TODO: 根据你的部署拓扑，添加连接池和负载均衡

use tonic::transport::Channel;

/// Thunder 集群枚举
///
/// X 的 Thunder 服务按功能分为多个独立集群：
///   - Amp: 主要的用户时间线数据集群
///   - Pdisco: 用于个性化发现场景的独立集群
///
/// 每个集群独立持有内存缓存，数据可能有所不同。
/// MVP 阶段只使用单一集群即可。
#[derive(Clone, Debug)]
pub enum ThunderCluster {
    /// 主集群 — 持有完整的用户时间线数据
    Amp,
    /// 发现集群 — 用于"发现"tab 或探索页面（可选）
    Pdisco,
}

/// Thunder gRPC 客户端
///
/// 管理到 Thunder 服务的 gRPC 连接。
/// 在 Home Mixer 管道中，由 ThunderSource 使用此客户端
/// 获取用户关注列表中的最新帖子。
pub struct ThunderClient {
    /// gRPC 连接通道
    channel: Channel,
}

impl ThunderClient {
    pub fn from_addr(endpoint: String) -> Result<Self, String> {
        let channel = Channel::from_shared(endpoint)
            .map_err(|error| format!("invalid Thunder endpoint: {error}"))?
            .connect_lazy();
        Ok(Self { channel })
    }

    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(endpoint) = std::env::var("THUNDER_GRPC_ADDR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        Self::from_addr(endpoint).map(Some)
    }

    /// 获取指定集群的一个随机 gRPC 通道
    ///
    /// 原始实现从连接池中随机选择一个连接以实现负载均衡。
    /// 当前简化实现返回唯一的通道。
    pub fn get_random_channel(&self, _cluster: ThunderCluster) -> Option<Channel> {
        Some(self.channel.clone())
    }
}
