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
    /// 创建 Thunder 客户端
    ///
    /// 连接到本地或远程的 Thunder gRPC 服务。
    /// 默认连接 localhost:50052（Thunder 的标准端口）。
    ///
    /// TODO: 从配置文件或环境变量读取 Thunder 地址
    pub async fn new() -> Self {
        let thunder_addr = std::env::var("THUNDER_GRPC_ADDR")
            .unwrap_or_else(|_| "http://localhost:50052".to_string());

        let channel = Channel::from_shared(thunder_addr)
            .expect("Invalid Thunder address")
            .connect_lazy();

        Self { channel }
    }

    /// 获取指定集群的一个随机 gRPC 通道
    ///
    /// 原始实现从连接池中随机选择一个连接以实现负载均衡。
    /// 当前简化实现返回唯一的通道。
    ///
    /// # Arguments
    /// * `_cluster` - 目标 Thunder 集群（当前忽略）
    ///
    /// # Returns
    /// Some(channel) 如果连接可用，None 如果无可用连接
    pub fn get_random_channel(&self, _cluster: ThunderCluster) -> Option<Channel> {
        // The current Thunder wire contract still carries business IDs as
        // integer fields. Keep this adapter behind the explicit legacy feature
        // so a no-default-features build cannot accidentally send ObjectIds
        // through a lossy u64 conversion. P3 will remove this gate when the
        // wire schema is migrated to strings.
        #[cfg(feature = "legacy-int-ids")]
        {
            Some(self.channel.clone())
        }
        #[cfg(not(feature = "legacy-int-ids"))]
        {
            None
        }
    }
}
