# 依赖分析报告

## 日期
2026-04-09

## 概述
本报告分析了 `home-mixer` 项目中缺少的外部依赖，并标记了哪些可以在 crates.io 上找到，哪些是内部私有依赖。

## 外部依赖（公共 crates.io 依赖）

以下依赖可以在 crates.io 上找到，已建议添加到 Cargo.toml：

| 依赖名 | 建议版本 | 用途 |
|--------|---------|------|
| clap | "4.6" | 命令行参数解析 |
| futures | "0.3" | 异步 Future 支持 |
| log | "0.4" | 日志记录 |
| serde | "1.0" | 序列化/反序列化 |
| serde_json | "1.0" | JSON 序列化（serde 通常需要） |
| tokio | "1.0" | 异步运行时（包含在 tonic 中） |
| tonic | "0.12" | gRPC 框架 |
| tonic-reflection | "0.12" | gRPC 服务反射 |
| anyhow | "1.0" | 错误处理 |
| axum | "0.7" | HTTP 路由器 |

## 内部依赖（私有依赖）

以下依赖是 XAI 内部私有依赖，不在 crates.io 上：

| 依赖名 | 说明 |
|--------|------|
| xai_candidate_pipeline | 候选者流水线框架（已存在于 ../candidate-pipeline 目录） |
| xai_home_mixer_proto | HomeMixer gRPC protobuf 定义 |
| xai_http_server | HTTP 服务器框架 |
| xai_post_text | 推文文本处理 |
| xai_recsys_aggregation | 推荐系统聚合 |
| xai_recsys_proto | 推荐系统 protobuf 定义 |
| xai_strato | Strato 存储/缓存客户端 |
| xai_thunder_proto | Thunder 服务 protobuf 定义 |
| xai_twittercontext_proto | Twitter 上下文 protobuf 定义 |
| xai_uas_thrift | 用户行为序列 Thrift 定义 |
| xai_visibility_filtering | 可见性过滤 |

## 补充说明

### 建议的完整 Cargo.toml

```toml
[package]
name = "home-mixer"
version = "0.1.0"
edition = "2021"

[dependencies]
# 命令行和日志
clap = { version = "4.6", features = ["derive"] }
log = "0.4"
anyhow = "1.0"

# 异步运行时
tokio = { version = "1.0", features = ["full"] }
futures = "0.3"

# 序列化
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# gRPC
tonic = "0.12"
tonic-reflection = "0.12"
prost = "0.13"

# HTTP
axum = "0.7"

# 内部依赖（需要配置 Git 或本地路径）
xai_candidate_pipeline = { path = "../candidate-pipeline" }
# xai_home_mixer_proto = { git = "internal-repo", ... }
# xai_http_server = { git = "internal-repo", ... }
# 其他内部依赖...

[[bin]]
name = "home-mixer"
path = "main.rs"

[lib]
path = "lib.rs"
```

### 依赖关系说明

1. **xai_candidate_pipeline**: 已存在于本项目的 `../candidate-pipeline` 目录，可以通过本地路径引用

2. **xai_home_mixer_proto**: 需要找到对应的 protobuf 定义文件，可能位于项目的 proto 目录或其他仓库

3. **其他 xai_* 依赖**: 这些都是内部私有依赖，需要：
   - 配置 Git 仓库引用（如果使用内部 Cargo registry）
   - 使用本地路径引用（如果这些 crate 都在同一 monorepo 中）
   - 使用替代实现或 mock（如果是开源版本）

### 建议的下一步操作

1. **立即可以添加的依赖**：
   ```bash
   cargo add clap log anyhow tokio futures serde serde_json tonic tonic-reflection prost axum
   ```

2. **本地路径依赖**：
   - 将 `xai_candidate_pipeline` 配置为本地路径依赖

3. **私有依赖处理**：
   - 如果这些依赖需要开源，考虑创建公开的替代实现
   - 如果是内部使用，配置正确的 Git 仓库或本地路径
   - 考虑使用 trait 和 mock 来解耦私有依赖

4. **Protobuf 依赖**：
   - 检查项目中是否有 proto 定义文件
   - 考虑使用 tonic-build 从 proto 文件生成代码
