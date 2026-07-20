# X 推荐算法迁移进度记录

状态：`historical`

本文档记录 `x-algorithm` 从 X 内部专有系统迁移为可独立运行的开源推荐系统的执行进度。

迁移路线图参考 [迁移指南](./x_algorithm_migration_guide.md)。本文保留早期迁移阶段视角；当前代码事实请优先阅读 [../README.md](../../README.md) 中列出的模块文档。

---

## 阶段一 ✅：重写通信协议 (Protobuf)

### 目标
用自建 `.proto` 文件替代不可获取的 `xai_thunder_proto`、`xai_home_mixer_proto`、`xai_recsys_proto` 三个私有 crate。

### 产出物

| 文件 | 说明 |
|------|------|
| `proto/definitions/in_network.proto` | Thunder 服务协议（LightPost、TweetCreateEvent/DeleteEvent、InNetworkEvent、InNetworkPostsService） |
| `proto/definitions/home_mixer.proto` | Home Mixer 服务协议（ScoredPostsQuery、ScoredPost、ScoredPostsResponse、ScoredPostsService） |
| `proto/definitions/recsys.proto` | Phoenix 精排/召回协议（ActionName 18种行为、UserActionSequence、PredictNextActions、Retrieve 两个 gRPC 服务） |
| `proto/build.rs` | tonic-build 编译期驱动 |
| `proto/src/lib.rs` | 导出 `thunder`、`home_mixer`、`recsys` 三个模块 |
| `proto/Cargo.toml` | prost 0.13 + tonic 0.12 |

### 数据结构溯源

所有消息体字段均通过**逆向分析**现有 Rust 代码得到：

| Proto 消息体 | 对照源码 |
|-------|-------|
| `LightPost` | `thunder/posts/post_store.rs` 中的使用 + `thunder/kafka/tweet_events_listener.rs` 中的构造 |
| `GetInNetworkPostsRequest` | `home-mixer/sources/thunder_source.rs` 中的请求构造 |
| `ScoredPostsQuery` | `home-mixer/server.rs` 中的字段访问 |
| `ScoredPost` | `home-mixer/server.rs` 中的响应构造 |
| `ActionName` (18种) | `home-mixer/scorers/phoenix_scorer.rs` 中的枚举引用 |
| `UserActionSequence` | `home-mixer/query_hydrators/user_action_seq_query_hydrator.rs` 中的完整构造链 |
| `PredictNextActionsResponse` | `home-mixer/scorers/phoenix_scorer.rs` 中的响应解析 |
| `RetrieveResponse` | `home-mixer/sources/phoenix_source.rs` 中的响应解析 |

### 验证
- ✅ `protoc 34.1` 语法检查通过
- ✅ `cargo build -p x-algorithm-proto` 编译通过

### 附带完成
- 创建根级 `Cargo.toml` workspace（包含 proto、thunder、home-mixer、candidate-pipeline）
- 补齐 `candidate-pipeline/Cargo.toml`（原始仓库缺失）

---

## 阶段二 ✅：重写 Thunder (内容缓存)

### 目标
移除 Thunder 中所有不可编译的 `xai_*` 私有依赖，用开源方案替代，使 Thunder crate 能完整编译。

### 私有依赖替换

| 原私有依赖 | 替换方案 | 涉及文件 |
|-----------|--------|----------|
| `xai_thunder_proto` | `x_algorithm_proto::thunder` | thunder_service.rs, post_store.rs, deserializer.rs, v2 listener |
| `xai_kafka` | `rdkafka` (StreamConsumer) | kafka_utils.rs, kafka/utils.rs, v2 listener |
| `xai_http_server` | `axum` + `tonic::transport::Server` | main.rs |
| `xai_wily` | 删除（链路追踪，MVP 跳过） | kafka_utils.rs |
| `xai_profiling` | 删除（性能分析，MVP 跳过） | main.rs |

### 重建的缺失模块

原始开源代码脱敏了以下 6 个内部模块，从代码引用逆向重建：

| 文件 | 内容 |
|------|------|
| `thunder/args.rs` | ~25 个 CLI 参数（clap derive），涵盖端口、Kafka 配置、Post 保留时间、并发限制等 |
| `thunder/config.rs` | 9 个业务常量（帖子保留策略、列表大小限制、视频时长阈值等） |
| `thunder/metrics.rs` | ~20 个 Prometheus 监控指标（gRPC 延迟、PostStore 统计、Kafka lag）+ Timer 辅助结构体 |
| `thunder/strato_client.rs` | 关注列表获取的 stub 实现（后续接 Redis 或平台关系服务） |
| `thunder/o2.rs` | 空占位模块 |
| `thunder/deserializer.rs` | 重写为仅保留 v2 Protobuf 反序列化 |

### 关键改造细节

#### v1 Kafka 管道 Legacy 化
- `tweet_events_listener.rs` 加上 `#[cfg(feature = "legacy")]` 门控
- 默认只编译 v2 Protobuf 管道，Thrift 作为可选 feature

#### main.rs 重写
- `xai_http_server::HttpServer` → `axum::serve` + `tonic::transport::Server`
- `xai_profiling::spawn_server` → 删除
- `CancellationToken` → `tokio_util::sync::CancellationToken`

#### 类型兼容性修复
- Proto `u64` → PostStore `i64`：在 thunder_service.rs 中增加 `.map(|id| id as i64)` 转换
- Rust 2024 `let chain` → 2021 嵌套 `if let`（post_store.rs）
- `CompressionEncoding::Zstd` → `Gzip`（tonic 0.12 兼容）

### 验证
- ✅ `cargo build -p thunder` 编译通过
- ✅ 零 `xai_*` 引用残留（仅注释和 feature-gated legacy 文件中存在）

---

## 阶段三：Phoenix 模型链路

早期迁移记录里这一阶段标记为“待执行”。当前仓库已经包含 `phoenix/` 推理、服务封装、样例数据和专题文档，但仍缺完整训练流水线。

当前状态请看：

- [../phoenix/README.md](../../phoenix/README.md)：Phoenix 当前实现边界。
- [../phoenix/06-training-and-data.md](../../phoenix/06-training-and-data.md)：训练侧现状与缺口。
- [../training/README.md](../../training/README.md)：训练数据规格和离线链路设计。

## 阶段四：组装 Home Mixer

早期迁移记录里这一阶段标记为“待执行”。当前仓库已经包含 `home-mixer/` 服务骨架、候选流装配、Thunder 集成和大量当前实现文档，但默认外部依赖仍多为 stub。

当前状态请看：

- [../home-mixer/README.md](../../home-mixer/README.md)：Home Mixer 当前实现。
- [../candidate-pipeline/README.md](../../candidate-pipeline/README.md)：候选流框架执行语义。
- [../thunder/README.md](../../thunder/README.md)：Thunder 当前实现和集成边界。
