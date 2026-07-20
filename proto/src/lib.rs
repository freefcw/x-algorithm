// x-algorithm-proto: 推荐系统协议库
//
// 本 crate 汇聚了三大组件的通信协议：
//   - Thunder (in_network): 实时帖子缓存服务
//   - Home Mixer (home_mixer): Feed 流编排主服务
//   - Recsys/Phoenix (recsys): 推荐模型预测及双塔召回服务
//
// 生成的代码包括 Protobuf 消息结构体和 tonic gRPC 客户端/服务端桩。

/// 演示模式共享契约（thunder 与 home-mixer 的演示数据咬合约定）
pub mod demo;

/// Thunder 服务协议 —— 网络内帖子实时缓存
pub mod thunder {
    tonic::include_proto!("thunder");

    /// gRPC 反射所需的文件描述符集
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptor");
}

/// Home Mixer 服务协议 —— Feed 流编排
pub mod home_mixer {
    tonic::include_proto!("home_mixer");

    /// gRPC 反射所需的文件描述符集（与 thunder 共享同一个描述符文件）
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptor");
}

/// Recsys/Phoenix 服务协议 —— 精排预测与双塔召回
pub mod recsys {
    tonic::include_proto!("recsys");
}
