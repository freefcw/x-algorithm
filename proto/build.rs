use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("definitions");

    let proto_files = &[
        proto_dir.join("in_network.proto"),
        proto_dir.join("home_mixer.proto"),
        proto_dir.join("recommendation_data.proto"),
        proto_dir.join("recsys.proto"),
        proto_dir.join("vm_ranker.proto"),
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
