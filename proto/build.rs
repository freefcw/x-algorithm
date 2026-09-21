use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("definitions");

    // Phoenix 协议文件名刻意不叫 recsys.proto：phoenix/python/common/xai-proto 里旧 Phoenix 引擎的
    // 协议也叫 recsys.proto，两者若注册进同一个 protobuf descriptor pool 会撞名。
    // 包名仍是 recsys，Rust 侧 tonic::include_proto!("recsys") 不受文件名影响。
    let proto_files = &[
        proto_dir.join("in_network.proto"),
        proto_dir.join("home_mixer.proto"),
        proto_dir.join("recommendation_data.proto"),
        proto_dir.join("viewer_relation.proto"),
        proto_dir.join("phoenix_recsys.proto"),
        proto_dir.join("vm_ranker.proto"),
        proto_dir.join("id_registry.proto"),
    ];

    // 生成 Rust 代码，包含 gRPC 服务端和客户端桩（stubs）
    tonic_build::configure()
        // 生成 FILE_DESCRIPTOR_SET 以支持 gRPC 反射服务
        .file_descriptor_set_path(
            PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin"),
        )
        .compile_protos(proto_files, &[&proto_dir])?;

    Ok(())
}
