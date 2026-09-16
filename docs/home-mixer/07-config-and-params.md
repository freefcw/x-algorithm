# 07. 配置、入口参数与参数手册

本篇把 `home-mixer` 当前所有可见的配置入口整理成一张图和几组表，方便回答三个问题：

1. 服务怎么启动
2. 配置从哪里进来
3. 参数改了会影响哪一段链路

## 1. 配置入口总览

当前 `home-mixer` 的配置入口并不多，主要分三层：

- 进程启动参数
- 环境变量
- 编译期常量 `params/`（`param.rs` 权重与召回上限，`config.rs` TopK / 超时）

```mermaid
flowchart TD
    A["启动配置"] --> B["CLI 参数<br/>main.rs"]
    A --> C["环境变量<br/>clients / side_effects"]
    A --> D["编译期常量<br/>params/"]

    B --> B1["grpc_port"]
    B --> B2["metrics_port"]
    B --> B3["reload_interval_minutes"]
    B --> B4["chunk_size"]

    C --> C0["MRPYQ_RECOMMENDATION_DATA_ADDR<br/>非 demo 必填"]
    C --> C1["THUNDER_GRPC_ADDR<br/>仅 demo"]
    C --> C2["Phoenix gRPC 地址"]
    C --> C3["HOME_MIXER_MODE"]
    C --> C4["HOME_MIXER_ENABLE_* / HOME_MIXER_VF_FAILURE_POLICY<br/>可选集成与失败策略"]

    D --> D1["召回上限"]
    D --> D2["打分权重"]
    D --> D3["多样性 / OON"]
    D --> D4["UAS 窗口"]
    D --> D5["过滤阈值"]
    D --> D6["TopK / ResultSize"]
```

## 2. 启动参数

启动参数定义在 `home-mixer/main.rs`。

| 参数 | 默认值 | 当前实际用途 | 备注 |
| --- | --- | --- | --- |
| `--grpc-port` | `50051` | gRPC 对外监听端口 | 实际生效 |
| `--metrics-port` | `9090` | HTTP 监听端口 | 当前 router 为空，占位为主 |
| `--reload-interval-minutes` | `5` | 仅打印到启动日志 | 当前代码未使用 |
| `--chunk-size` | `100` | 仅打印到启动日志 | 当前代码未使用 |

一个重要结论：

- `reload_interval_minutes` 和 `chunk_size` 在当前代码中只是参数保留位，不进入任何业务逻辑。

## 3. 环境变量

### 3.1 已实际读取

| 变量 | 读取位置 | 作用 | 默认行为 |
| --- | --- | --- | --- |
| `THUNDER_GRPC_ADDR` | `clients/thunder_client.rs` | Thunder gRPC 地址；仅 `HOME_MIXER_MODE=demo` 装配 | 默认 `http://localhost:50052`。非 demo 必须走 mrpyq，启动不会回退到整数 Thunder |
| `PHOENIX_PREDICT_GRPC_ADDR` | `clients/phoenix_prediction_client.rs` | Phoenix 精排 gRPC 地址 | 未设置时显式 Unavailable，Scorer 保留候选并走规则 fallback |
| `PHOENIX_RETRIEVAL_GRPC_ADDR` | `clients/phoenix_retrieval_client.rs` | Phoenix 召回 gRPC 地址 | 未设置时显式 Unavailable，Source 跳过网外召回路 |
| `PHOENIX_MOE_GRPC_ADDR` | `candidate_pipeline/phoenix_candidate_pipeline.rs` | Phoenix MoE 专家召回地址；只提供地址，不会自动启用 | 未设置时不装配 MoE Source |
| `PHOENIX_EXPECTED_MODEL_VERSION` | `clients/phoenix_prediction_client.rs` | 固定期望的网关 `model-version` trailing metadata | 未设置时只校验非空、非 `random`；设置后不一致的响应被拒绝并走规则回退 |
| `PHOENIX_ENGINE` | `clients/phoenix_prediction_client.rs` | 精排引擎选择：`slim`（当前唯一实现）/ `xrex`（保留） | 默认 `slim`；`xrex` 或未知值在装配时直接拒绝启动 |
| `HOME_MIXER_MODE` | `runtime_config.rs` | 运行意图：`demo` / `degraded` / `production_ready` | 默认 `degraded`（需 `MRPYQ_RECOMMENDATION_DATA_ADDR`）；调用方身份、TES、UAS 事件合同、Strato、VF、网内 / 兜底、Phoenix 元数据、served 落库合同未验收前，`production_ready` 拒绝启动 |
| `HOME_MIXER_REDIS_URL` | `runtime_config.rs` / `server.rs` | 共享 FeedStateStore 的 Redis 地址 | 非 Demo 必填；Demo 未配置时使用内存，显式配置后也可使用 Redis。连接失败会拒绝启动 |
| `HOME_MIXER_REDIS_CONNECT_TIMEOUT_MS` | `runtime_config.rs` | Redis 建连时间上限 | 默认 `1000` 毫秒，必须大于零 |
| `HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS` | `runtime_config.rs` | 单次 Redis 操作时间上限，包括等待重连 | 默认 `500` 毫秒，必须大于零 |
| `HOME_MIXER_REDIS_KEY_PREFIX` | `runtime_config.rs` | Redis key 前缀 | 默认 `home_mixer:feed_state`；同一部署的所有副本必须一致，用户 ID 使用 hash tag |
| `HOME_MIXER_FEED_STATE_TTL_SECS` | `runtime_config.rs` | 两个历史 key 的滑动过期时间，每次记录刷新 | 默认 7 天；`0` 禁用过期，并在下次记录时清除该用户旧 key 的过期时间 |
| `UAS_REDIS_URL` | `runtime_config.rs`（`UasConfig`） | UAS 投影 job 与 Home Mixer 共用的 Redis 地址 | 非 demo 缺省复用 `HOME_MIXER_REDIS_URL`；demo 只在显式设置时切到 Redis，否则保留合成序列。job 必须能从两者之一取到地址。连接失败拒绝启动 |
| `UAS_REDIS_KEY_PREFIX` | `runtime_config.rs` | 行为序列 ZSET 前缀 | 默认 `home_mixer:uas`，key 形如 `home_mixer:uas:{user_id}:actions`；job 与所有 Home Mixer 副本必须一致 |
| `UAS_MAX_ACTIONS` | `runtime_config.rs` | 每个用户保留的**原始行为**条数（不是聚合后的帖子数） | 默认 600（`UAS_STORE_MAX_ACTIONS`）；job 写入时删除最旧成员，Home Mixer 读取时也只取最新这么多条，因此两侧配置不一致时以较小值为准且总是保留最新的 |
| `UAS_REDIS_TTL_SECS` | `runtime_config.rs` | UAS key 的 Redis TTL，每次写入刷新 | 默认 604800 秒；设为 `0` 可关闭 TTL，并在下次写入时清除旧 key 的过期时间 |
| `UAS_REDIS_CONNECT_TIMEOUT_MS` | `runtime_config.rs` | UAS Redis 建连时间上限 | 默认 1000 毫秒，必须大于零 |
| `UAS_REDIS_REQUEST_TIMEOUT_MS` | `runtime_config.rs` | UAS Redis 单次读写上限，包括连接被替换后的一次重试 | 默认 500 毫秒，必须大于零；Home Mixer 侧另有 `UAS_FETCH_TIMEOUT_MS`（500 ms）包在外层 |
| `UAS_KAFKA_BROKERS` / `UAS_KAFKA_TOPIC` | `bin/uas_worker.rs` | 启用 Kafka 消费模式并指定现有行为 topic | 未设置 broker 时从 stdin 读取换行 JSON；Kafka 模式需要用 `--features kafka` 构建 |
| `UAS_KAFKA_GROUP_ID` / `UAS_KAFKA_AUTO_OFFSET_RESET` | `bin/uas_worker.rs` | 消费组与首次消费位置 | 默认 `home-mixer-uas-projector` / `earliest` |
| `UAS_KAFKA_SECURITY_PROTOCOL` | `bin/uas_worker.rs` | Kafka 安全协议 | 默认 `PLAINTEXT`；`SASL_PLAINTEXT` / `SASL_SSL` 还需配置 `UAS_KAFKA_SASL_MECHANISM`（默认 `PLAIN`）、`UAS_KAFKA_SASL_USERNAME`、`UAS_KAFKA_SASL_PASSWORD`；纯 `SSL` 不需要凭据 |
| `HOME_MIXER_ENABLE_PHOENIX_MOE` | `feature_policy.rs` | 显式启用 Phoenix MoE 旁路召回 | 默认关闭；启用但缺少 `PHOENIX_MOE_GRPC_ADDR` 时记录告警并跳过，主链继续 |
| `HOME_MIXER_ENABLE_REQUEST_CACHE_SIDE_EFFECT` | `feature_policy.rs` | 显式启用请求缓存 SideEffect | 默认关闭；启用前必须人工确认真实 Strato adapter、schema、认证和保留策略 |
| `HOME_MIXER_ENABLE_DEBUG_RPC` | `feature_policy.rs` / `debug_access.rs` | 启用 `DebugScoredPosts` | 默认关闭；开启时必须同时提供 `HOME_MIXER_DEBUG_TOKEN`，调用方通过 `x-home-mixer-debug-token` metadata 传入 |
| `HOME_MIXER_ENABLE_UNSIGNED_CACHED_POSTS` | `feature_policy.rs` / `runtime_config.rs` / `server.rs` | 允许请求直接携带未签名 `cached_posts` fixture | 默认关闭且只允许 `demo`；生产缓存必须使用服务端状态或签名/opaque 合同 |
| `HOME_MIXER_ENABLE_VM_RANKER` | `feature_policy.rs` | 显式启用 VM Ranker 二次重排 Scorer | 默认关闭；整数 proto 只能 round-trip 零填充演示 ID，非 demo 会告警并禁用；demo 下启用但缺少 `VM_RANKER_GRPC_ADDR` 时跳过，主链继续 |
| `HOME_MIXER_ENABLE_AUTHOR_COLD_START` | `feature_policy.rs` | 启用低曝光新作者提升，并在 scorer 前补作者粉丝数 | 默认关闭；当前只允许 `demo`，非 demo 会告警并禁用；候选缺 `view_count` 或作者粉丝数时严格不参与 |
| `HOME_MIXER_ENABLE_COLD_START_THOMPSON_SAMPLING` | `feature_policy.rs` | 在冷启动候选中启用 Beta Thompson Sampling | 默认关闭；只有 Author Cold Start 同时开启才生效 |
| `HOME_MIXER_VF_FAILURE_POLICY` | `feature_policy.rs` / `filters/vf_filter.rs` | VF 请求失败/超时、`Unchecked`、成功响应缺帖时的候选保留策略：`fail_closed` 全丢弃，`in_network_only` 仅保留 `in_network == Some(true)`，`allow_all` 全保留 | 默认 `fail_closed`；大小写不敏感、trim 后解析，未知值告警并按 `fail_closed`；非 demo 模式下设成 `allow_all` 会在启动时告警 |
| `VM_RANKER_GRPC_ADDR` | `candidate_pipeline/phoenix_candidate_pipeline.rs` | VM Ranker 服务地址；只提供地址不会自动启用 | 未设置时不装配 `VMRanker` Scorer |
| `VM_RANKER_VALUE_MODEL_ID` | `candidate_pipeline/phoenix_candidate_pipeline.rs` | 选择 value model；上游从 feature switch 读取，本地由装配显式配置 | 未设置时服务端按 `unknown` 记账并使用默认权重 |
| `MRPYQ_RECOMMENDATION_DATA_ADDR` | `clients/mrpyq_adapters.rs` / `clients/mrpyq_recommendation_data_client.rs` / `clients/mrpyq_viewer_relation_client.rs` | mrpyq gRPC 地址，同址提供 `RecommendationDataService` 与 `ViewerRelationService` | 非 demo **必填**：装配 TES / 网内 / 兜底 / VF / Strato，网内召回以 24-hex ObjectId 原样下发。未设置或地址不合法则启动失败，不回退到整数 Thunder |
| `MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS` | `clients/mrpyq_recommendation_data_client.rs` | mrpyq 推荐数据调用超时（毫秒） | 默认 `500`（`params/config.rs`） |
| `HOME_MIXER_DEMO` | `demo.rs` | `HOME_MIXER_MODE=demo` 的旧兼容别名 | 仅兼容已有脚本；新配置使用 `HOME_MIXER_MODE` |

Phoenix 两个主服务地址通常同时指向 `phoenix/scripts/run_grpc_gateway.py` 启动的网关（默认 `http://localhost:50053`）。完整启动组合见 [getting-started 第四步](../getting-started/05-第四步-跑通完整推荐链路.md)。

### 3.2 可选集成启用规则

`HomeMixerConfig::from_env()` 在进程装配时一次性生成 `HomeMixerFeatures`。Source、SideEffect 和其他业务组件不直接读取环境变量。

`HomeMixerConfig.feed_state` 明确选择内存或 Redis，由 `HomeMixerServer::build` 创建实例。业务模式缺少 Redis 配置时直接拒绝启动；Demo 默认内存。Redis adapter 使用异步多路复用连接，同一连接可处理多个并发请求，已移除 `HOME_MIXER_REDIS_POOL_SIZE`。当前客户端支持单节点或提供兼容端点的代理，不支持原生 Cluster 路由。

一次推荐请求只读取一份历史快照，For You 外层和 Scored Posts 内层复用；下一次请求重新读取。连接被服务端关闭（Redis 重启、主从切换、代理回收空闲连接）后，客户端在后台换用新连接，首次读取会在新连接上重试一次，两次尝试共用同一个 `HOME_MIXER_REDIS_REQUEST_TIMEOUT_MS` 预算，超时不重试；因此不会出现“第一次请求读不到历史、却把结果写进历史”的重复下发。读取最终仍失败时沿用流水线的错误隔离行为继续请求，但同一请求的写入通常也会失败并返回 `Unavailable`。写失败返回 gRPC `Unavailable`；写超时可能已经在 Redis 执行，adapter 不自动重放写请求。该存储用于有界下发历史，不是训练曝光事件日志，也不承诺同一用户并发请求之间严格不重复下发。

`HomeMixerConfig.uas`（`UasConfig`）同样在启动时一次解析：demo 默认 `Demo` 合成序列，显式 `UAS_REDIS_URL` 才切到 `Redis`；非 demo 取 `UAS_REDIS_URL`，缺省复用 `HOME_MIXER_REDIS_URL`，因此通过 `HomeMixerConfig::from_env()` 启动的业务服务总是装配 Redis UAS adapter。只有直接调用 `PhoenixCandidatePipeline::assemble_for_mode` 且两个地址都没有配置时才得到 `Disabled`（空序列、Phoenix 整体跳过），装配层会打 warn。UAS Redis 连接失败同样拒绝启动。

UAS 是进程内 `UserActionSequenceOps` 端口，不额外启动在线微服务。独立投影 job 是 `uas-worker`：Kafka 模式用 `cargo run -p home-mixer --features kafka --bin uas-worker`，配置 `UAS_KAFKA_BROKERS`、`UAS_KAFKA_TOPIC` 后消费 JSON 行为事件；没有 Kafka 时省略 broker（也无需 `kafka` feature），job 从 stdin 读取相同格式的换行 JSON。每条事件写入 `home_mixer:uas:{user_id}:actions` ZSET，score 是行为时间，按行为时间保留最近 7 天、最多 `UAS_MAX_ACTIONS` 条原始行为；Home Mixer 的 Redis adapter 读取窗口内**最新**的 `UAS_MAX_ACTIONS` 条，按时间升序交给现有聚合器（聚合后再按帖子截到 `UAS_MAX_SEQUENCE_LENGTH`）。

投影 job 的投递语义：

- 事件只在 JSON 边界校验一次（ID 为 24 位小写 hex 且非 nil、`action_time_ms > 0`、`action_type` 在模型 action_mask 范围 1..=18 内，范围与 `recsys_compat::ACTION_MASK_LEN` 同源）；不合法的毒丸消息记 warn 后丢弃并推进 offset，未知字段忽略。
- 早于 7 天窗口的重放事件、以及领先 job 本机时钟超过 `UAS_MAX_FUTURE_SKEW_MS`（5 分钟）的“未来”事件，都作为已处理事件跳过并分别计数（`skipped_outside_window` / `skipped_future`）；偏差在容忍范围内的事件按原时间戳写入，在 Home Mixer 的读窗口追上之前不可见。
- Redis 写入是幂等的（相同事件序列化为字节相同的成员），所以 job 先在进程内按 100 ms 起步、5 s 上限的指数退避重试，总预算 `UAS_PROJECTION_RETRY_BUDGET_MS`（60 s，小于 Kafka `max.poll.interval.ms`）；预算耗尽才退出，该 offset 不提交，由进程管理器重启后重放。同一分区之后的消息不会被提前处理。
- offset 由 job 在消息处理完成后手工 store（`enable.auto.offset.store=false`），librdkafka 周期自动提交；收到 SIGTERM / Ctrl-C 时同步提交已处理的 offset 再退出。job 每 60 秒输出一行计数（projected / skipped / invalid / storage_retries / storage_failures）。
- job 单实例串行处理，吞吐受 Redis 往返时间限制；需要更高吞吐时按 topic 分区横向多实例，ZSET 写入的幂等性保证多实例安全。

Home Mixer 读取侧对无法解码的成员（损坏数据、未知 `version`）逐条跳过并按次告警，不会因为一个坏成员让该用户整条序列失效；`StoredUserAction::VERSION` 升级时读取端必须继续解码旧版本直到旧成员过期。

事件 JSON 的最小合同如下；面向生产端（mrpyq）的完整约定——字段语义、`action_type` 映射与分批、投递语义、联调验收——见 [implementation/uas-event-contract.md](../implementation/uas-event-contract.md)：

```json
{"user_id":"000000000000000000000007","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":1789516800000,"action_type":3}
```

本地没有 Kafka 时，下面的 stdin 模式只用于开发环境把标准化事件投影到 Redis；它不是持久事件日志。生产环境仍需把事件先写入 Kafka 或已验收的持久化入口，再运行 job。注意示例里的固定时间戳过了 7 天就会落在窗口外被跳过（日志级别 debug），本地试跑请像下面这样用当前时间：

```bash
now_ms=$(($(date +%s) * 1000))
printf '{"user_id":"000000000000000000000007","tweet_id":"000000000000000000000009","author_id":"00000000000000000000000b","action_time_ms":%s,"action_type":3}\n' "$now_ms" \
  | UAS_REDIS_URL=redis://localhost:6379/ RUST_LOG=info cargo run -p home-mixer --bin uas-worker
```

状态配置与请求快照的回归测试随 `cargo test -p home-mixer` 执行；`uas-worker` 的投递语义单测用 `cargo test -p home-mixer --features kafka --bin uas-worker` 连同 Kafka 分支一起编译。真实 Redis 测试默认标记为 ignored，需要本机安装 `redis-server` 后单独执行；测试会启动独立进程和 Unix socket，结束后自动清理：

```bash
cargo test -p home-mixer --test redis_feed_state --test redis_uas -- --ignored --test-threads=1
```

可选集成遵循以下规则：

1. 默认关闭；主推荐链不能依赖旁路功能才能启动或返回结果。
2. 开关只代表操作员批准启用，不代表外部服务已经完成接入。
3. 启用前必须人工确认服务 owner、公开 schema、认证、超时、错误语义、降级、测试环境和数据保留策略。
4. 开关开启但必要地址缺失时，装配层记录告警并跳过组件，不能阻断主链启动。
5. Ads、Prompt、WhoToFollow、PushToHome 已挂进 ForYou 外层，但 `enable()` 恒 false；Kafka 和 Grox 模型能力不提供伪开关。Feed state 的 Redis 配置见上表。

请求缓存 SideEffect 即使显式开启，当前非 demo 的 `MrpyqStratoClient`（mrpyq 没有 served 状态写契约）和 demo 的 `DemoStratoClient` 都会明确拒绝持久化写入。必须完成人工接入和持久化验收后，才能把它视为生产数据闭环。

### 3.3 QueryBuilder 外部策略

两个 RPC 共用同一个 `QueryBuilder`。它负责 viewer ID 校验、公共 proto 映射、请求 ID 以及全局/请求级 MoE 开关合并。

QueryBuilder 不持有 Gizmoduck client，也不发起 viewer RPC。`in_network_only` 只取请求显式值：`true` 进入网内专用路径，`false` 同时允许网内和网外。Gizmoduck 仅由 candidate hydrator 用于作者资料批量查询，超时预算为 500 ms。

### 3.4 证书路径

`clients/s2s.rs` 里固定了三条路径：

| 常量 | 默认路径 | 用途 |
| --- | --- | --- |
| `S2S_CHAIN_PATH` | `/etc/pki/tls/certs/s2s-chain.pem` | CA 链 |
| `S2S_CRT_PATH` | `/etc/pki/tls/certs/s2s-cert.pem` | 客户端证书 |
| `S2S_KEY_PATH` | `/etc/pki/tls/private/s2s-key.pem` | 客户端私钥 |

但要注意：

- 当前没有任何已装配的客户端读取这些路径；mrpyq 与 Phoenix 通道都是明文 gRPC
- `demo` 注入显式 Allow adapter；`degraded` 注入 `MrpyqFirstStageEligibilityClient`，它只承载 mrpyq 一级 `recommendation_eligible`，没有 viewer 级判定
- VF 未知（失败/超时/缺帖）时按 `HOME_MIXER_VF_FAILURE_POLICY` 处理：默认 `fail_closed` 全丢弃，`in_network_only` 仅保留网内，`allow_all` 需显式配置；`production_ready` 在真实 VF 合同缺失时拒绝启动

## 4. 服务级参数

### 4.1 gRPC 传输参数

| 常量 | 值 | 使用点 |
| --- | --- | --- |
| `MAX_GRPC_MESSAGE_SIZE` | `128 * 1024 * 1024` | gRPC server 的编码/解码消息大小限制 |
| `THUNDER_REQUEST_TIMEOUT_MS` | `500` | Thunder 网内召回上限 |
| `UAS_FETCH_TIMEOUT_MS` | `500` | request-scoped UAS 读取上限 |
| `USER_FEATURES_FETCH_TIMEOUT_MS` | `500` | request-scoped Strato 用户特征读取上限 |
| `USER_TOPIC_READ_TIMEOUT_MS` | `500` | 补充话题 profile 读取上限 |
| `STRATO_WRITE_TIMEOUT_MS` | `500` | 异步请求信息写回上限 |
| `TES_REQUEST_TIMEOUT_MS` | `500` | TES core/media/subscription 单批调用上限 |
| `GIZMODOUCK_REQUEST_TIMEOUT_MS` | `500` | post-selection 作者资料批次上限 |
| `PHOENIX_RETRIEVAL_TIMEOUT_MS` | `3000` | Phoenix 标准/MoE 召回上限 |
| `PHOENIX_PREDICTION_TIMEOUT_MS` | `5000` | Phoenix 精排上限 |
| `TOPIC_RETRIEVAL_TIMEOUT_MS` | `500` | Topic 召回上限 |
| `VF_REQUEST_TIMEOUT_MS` | `500` | 单组可见性检查上限 |
| `VM_RANKER_TIMEOUT_MS` | `500` | 可选 VM Ranker 二次重排上限 |
| `MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS` | `500` | mrpyq 单次 RPC 调用上限（可由同名环境变量覆盖） |
| `MRPYQ_RECALL_BUDGET_MS` | `1500` | mrpyq 一次召回跨所有分页的总预算；预算耗尽时保留已读页 |

### 4.2 对外监听结构

```mermaid
flowchart LR
    Client1["gRPC 客户端"] --> G["0.0.0.0:grpc_port"]
    Client2["HTTP/探活方"] --> H["0.0.0.0:metrics_port"]

    G --> S["ScoredPostsService"]
    G --> F["ForYouFeedService"]
    H --> R["空 axum Router"]
```

当前 HTTP 端口的现实状态是：

- 进程会监听
- 但没有明确的 health/metrics route

## 5. 召回参数

| 常量 | 值 | 影响组件 | 影响说明 |
| --- | --- | --- | --- |
| `THUNDER_MAX_RESULTS` | `400`（上游 `1200`，U1 下调） | `ThunderSource` | 网内召回上限；召回源是 mrpyq 关注 inbox（硬顶 2000 条），`AgeFilter` 只留 `MAX_POST_AGE` 以内、出口 `RESULT_SIZE` 条，1200 取不满也用不上 |
| `PHOENIX_MAX_RESULTS` | `1000` | `PhoenixSource` | 网外召回上限 |
| `TOPIC_MAX_RESULTS` | `100` | `PhoenixTopicsSource` | 话题源单次上限 |
| `TWEET_MIXER_MAX_RESULTS` | `800` | `TweetMixerSource`（端口已定义，默认不装配） | TweetMixer 召回上限 |
| `PHOENIX_MOE_MAX_RESULTS` | `200` | —（已定义未消费） | MoE 专家召回上限；本地 `PhoenixMoeSource` 当前实际使用 `PHOENIX_MAX_RESULTS` |

Thunder + Phoenix 决定主链进入补全前的候选池规模；话题源另计。

## 6. 打分权重参数

### 6.1 正向离散行为

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `FAVORITE_WEIGHT` | `0.5` | 点赞 |
| `REPLY_WEIGHT` | `5.0` | 回复 |
| `RETWEET_WEIGHT` | `0.0`（上游 `1.0`，产品无转推，U5 置 0） | 转发 |
| `PHOTO_EXPAND_WEIGHT` | `0.05` | 图片展开 |
| `VIDEO_OPEN_WEIGHT` | `0.05` | 打开视频 |
| `CLICK_WEIGHT` | `0.4` | 点击详情 |
| `OPEN_LINK_WEIGHT` | `0.2` | 打开链接 |
| `PROFILE_CLICK_WEIGHT` | `0.0` | 点击作者主页 |
| `POST_UNEXPLORED_WEIGHT` | `0.02` | 低探索帖加分（默认只加给网内） |
| `VQV_WEIGHT` | `0.05` | 视频有效观看 |
| `SHARE_WEIGHT` | `2.0` | 分享 |
| `SHARE_VIA_DM_WEIGHT` | `5.0` | 私信分享 |
| `SHARE_VIA_COPY_LINK_WEIGHT` | `20.0` | 复制链接分享 |
| `DWELL_WEIGHT` | `0.0` | 二值停留 |
| `QUOTE_WEIGHT` | `0.0`（上游 `5.0`，产品无引用转发，U5 置 0） | 引用转发 |
| `QUOTED_CLICK_WEIGHT` | `0.0`（上游 `0.05`，U5 置 0） | 点击引用帖 |
| `QUOTED_VQV_WEIGHT` | `0.0` | 点击引用帖视频（当前恒零） |
| `FOLLOW_AUTHOR_WEIGHT` | `4.0` | 关注作者 |

### 6.2 连续行为

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `CONT_DWELL_TIME_WEIGHT` | `0.004` | 连续停留时间 |
| `CONT_CLICK_DWELL_TIME_WEIGHT` | `0.0` | 点击后停留（协议尚无对应连续动作，恒 `None`） |
| `CONT_ACTIVE_SECS_5M_RESIDUAL_NORM_WEIGHT` | `0.0` | 5 分钟活跃残差（同上，恒 `None`） |

低探索帖加分还有一个乘法变体开关组（默认走加法路径）：

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `ENABLE_MULTIPLICATIVE_POST_UNEXPLORED` | `false` | 改用乘法形式的低探索调整 |
| `MULTIPLICATIVE_POST_UNEXPLORED_ALPHA` | `0.0` | 乘法强度 |
| `POST_UNEXPLORED_WEIGHT_IN_NETWORK_ONLY` | `true` | 低探索加分只作用于网内候选 |

### 6.3 负向行为

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `NOT_INTERESTED_WEIGHT` | `-43.2` | 不感兴趣 |
| `BLOCK_AUTHOR_WEIGHT` | `-31.2` | 拉黑作者 |
| `MUTE_AUTHOR_WEIGHT` | `-58.8` | 静音作者 |
| `REPORT_WEIGHT` | `-234.0` | 举报 |
| `NOT_DWELLED_WEIGHT` | `-0.02` | 未停留 |

### 6.4 归一化相关

| 常量 | 值 | 当前作用 |
| --- | --- | --- |
| `NEGATIVE_SCORES_OFFSET` | `0.001` | `RankingScorer` 把负分映射进 `[0, offset)`，正分整体抬高该值 |

正负权重和在 `ScoringWeights::from_defaults()` 里现场求和，不再单独维护 `WEIGHTS_SUM` / `NEGATIVE_WEIGHTS_SUM`。

### 6.5 双向关注加成（数据未接入，暂不触发）

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `BIDIRECTIONAL_FOLLOW_REPLY_WEIGHT_BOOST` | `15.0` | 互关作者回复概率加成 |
| `BIDIRECTIONAL_FOLLOW_DWELL_WEIGHT_BOOST` | `0.0` | 互关作者停留加成 |

候选 `is_mutual_follow_author` 由上游 `BidirectionalFollowHydrator` 写入，本地无该数据端口（U3），`None` 时加成不触发。

### 6.6 点击停留低点赞率惩罚（默认关闭）

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `ENABLE_CLICK_DWELL_LOW_FAV_RATE_PENALTY` | `false` | 总开关 |
| `CLICK_DWELL_LOW_FAV_RATE_PENALTY_BASELINE` | `0.01` | 点赞率基线 |
| `CLICK_DWELL_LOW_FAV_RATE_PENALTY_ALPHA` | `0.5` | 惩罚强度 |
| `CLICK_DWELL_LOW_FAV_RATE_PENALTY_FLOOR` | `0.01` | 惩罚下限 |
| `CLICK_DWELL_LOW_FAV_RATE_PENALTY_CAP` | `1.0` | 惩罚上限 |

## 7. 多样性与网外降权参数

| 常量 | 值 | 影响组件 | 作用 |
| --- | --- | --- | --- |
| `OON_WEIGHT_FACTOR` | `0.75` | `RankingScorer` | 网外降权；网内回复/转发默认也乘（`ENABLE_OON_RESCORE_FOR_IN_NETWORK_REPLIES_RETWEETS`） |
| `TOPIC_OON_WEIGHT_FACTOR` | `0.5` | `RankingScorer` | 话题请求的网外降权 |
| `ENABLE_AUTHOR_DIVERSITY` | `true` | `RankingScorer` | 作者多样性总开关 |
| `AUTHOR_DIVERSITY_DECAY` | `0.5` | `RankingScorer` | 同作者重复衰减 |
| `AUTHOR_DIVERSITY_FLOOR` | `0.25` | `RankingScorer` | 衰减地板 |
| `NEW_USER_OON_WEIGHT_FACTOR` | `0.00001` | —（已定义未消费） | 新用户网外强降权；上游特判本地未实现 |
| `NEW_USER_MIN_FOLLOWING` | `5` | —（已定义未消费） | 新用户特判最少关注数；同上 |
| `NEW_USER_AGE_THRESHOLD_SECS` | `0` | —（已定义未消费） | 新用户账号年龄门槛；`0` 表示关闭，本地也无账号创建时间数据源 |

## 8. 冷启动探索参数（默认关闭）

对应 `AuthorColdStartScorer`，需同时设置 `HOME_MIXER_ENABLE_AUTHOR_COLD_START`（仅 demo）：

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `ENABLE_VIEWER_COLD_START` | `false` | 只是 `ColdStartConfig::default()`；生产装配以 `HOME_MIXER_ENABLE_AUTHOR_COLD_START` 为准，会覆盖这个常量 |
| `ENABLE_COLD_START_THOMPSON_SAMPLING` | `false` | 同上，装配以 `HOME_MIXER_ENABLE_COLD_START_THOMPSON_SAMPLING` 为准 |
| `COLD_START_IMPRESSION_THRESHOLD` | `1000` | 低于该曝光数的作者进入探索池 |
| `COLD_START_SLOT_MIN` / `COLD_START_SLOT_MAX` | `15` / `16` | 探索槽位区间 |
| `COLD_START_FOLLOWER_CAP` | `1000` | 作者粉丝数上限 |
| `LOW_IMPRESSIONS_MAX_POSITION_RATIO` | `0.85` | 低曝光候选最大位次比例 |
| `COLD_START_BETA_ALPHA0` / `COLD_START_BETA_BETA0` | `0.75` / `49.25` | Beta 先验参数 |
| `COLD_START_TS_TOP_K` | `5` | Thompson Sampling 取 Top-K |
| `COLD_START_IMPRESSION_SCALE` | `1.0` | 曝光量缩放 |

## 9. UAS 参数

| 常量 | 值 | 使用点 | 作用 |
| --- | --- | --- | --- |
| `UAS_WINDOW_TIME_MS` | `7 天` | `UserActionSeqQueryHydrator` | 聚合时间窗口 |
| `UAS_MAX_SEQUENCE_LENGTH` | `300` | `UserActionSeqQueryHydrator` | 序列截断上限 |

## 10. 过滤参数

| 常量 | 值 | 影响组件 | 作用 |
| --- | --- | --- | --- |
| `MAX_POST_AGE` | `48 小时` | `AgeFilter` | 帖子年龄限制 |
| `MIN_VIDEO_DURATION_MS` | `10000` | `RankingScorer` | 是否启用 VQV 权重 |
| `ENABLE_QUOTED_VQV_DURATION_CHECK` | `false` | `RankingScorer` | 引用帖 VQV 是否检查时长门槛 |

## 11. 输出参数

| 常量 | 值 | 影响组件 | 作用 |
| --- | --- | --- | --- |
| `TOP_K_CANDIDATES_TO_SELECT` | `50` | `TopKScoreSelector` | 选择阶段保留数量 |
| `RESULT_SIZE` | `35` | pipeline 最终裁剪 | 最终响应上限 |
| `WHO_TO_FOLLOW_POSITION` | `6` | —（已定义未消费） | 上游插入位次常量未被引用；`BlenderConfig` 默认位次是 10，但 `WhoToFollowSource.enable()` 恒 false，当前插不进去 |

两个值取自上游 `47c1bcd` 的真实配置，取代了此前本地自拟的 100 / 50。

```mermaid
flowchart LR
    A["召回后候选"] --> B["Scorers"]
    B --> C["TopKScoreSelector<br/>保留 50"]
    C --> D["Post-selection Filters"]
    D --> E["最终 truncate<br/>保留 35"]
```

## 12. 本地状态常量（U2，无上游对应）

Feed state 每用户的容量上限对内存和 Redis 实现均生效；用户总数上限只适用于内存，Redis 按 TTL 回收：

| 常量 | 值 | 含义 |
| --- | --- | --- |
| `LOCAL_SERVED_HISTORY_LIMIT` | `500` | 每用户保留的最近已下发帖子数 |
| `LOCAL_REQUEST_TIMESTAMP_LIMIT` | `50` | 每用户保留的最近请求时间戳数 |
| `LOCAL_STATE_USER_LIMIT` | `10,000` | 全局最多保留的用户数 |

## 13. 当前配置体系的现实评价

当前配置体系是“骨架完整、动态化不足”的状态：

- 有明确的参数分层
- 主要排序和过滤阈值都集中在 `params/`
- 但很多参数还是编译期常量，不是运行时配置
- 启动参数里也有两个暂未接入业务逻辑的保留位

如果后续要走向生产，优先建议动态化的是：

1. mrpyq / Phoenix 等外部服务地址与超时
2. 核心排序权重
3. 召回上限、TopK、ResultSize
4. Age / OON / Diversity 等策略阈值
