# UAS 行为事件流合同：对 mrpyq 的消息推送要求

> **状态**：`proposed`（推荐侧消费端 `uas-worker` 已实现并有单测 / Redis 集成测试；mrpyq 侧生产端待排期）
> **日期**：2026-09-16
> **读者**：mrpyq feed / webapi 后端、客户端埋点、推荐服务开发
> **事实边界**：本文所有「消费端行为」均引自本仓库当前代码并标注文件；对 mrpyq 的「要求」是推荐侧提出的接口约定，未经 mrpyq 侧确认
> **配套文档**：客户端埋点见 [user-action-collect.md](./user-action-collect.md)；身份维度约定见 [mrpyq-member-dimension-requirements.md](./mrpyq-member-dimension-requirements.md)；消费端环境变量与运行细节见 [home-mixer/07-config-and-params.md §3.2](../home-mixer/07-config-and-params.md)

---

## 1. 这条流是什么、不是什么

Phoenix 召回与精排的输入是「这个皮最近对哪些帖子做过什么」。这条行为事件流就是它的唯一来源：

```
mrpyq 业务事件 ──Kafka topic──▶ uas-worker ──Redis ZSET──▶ Home Mixer 聚合 ──▶ Phoenix Retrieve / PredictNextActions
```

- **消费方**：`home-mixer/bin/uas_worker.rs`，独立进程，用 `cargo run -p home-mixer --features kafka --bin uas-worker` 构建运行。它把事件投影到 Redis，Home Mixer 请求时读取（`home-mixer/clients/uas_fetcher.rs`）。
- **没有它的后果**：没有投影数据的用户在每次请求里 `PhoenixSource` 不召回、`PhoenixScorer` 整批标 `phoenix_missing_sequence`，候选由 `RuleFallbackScorer` 规则排序。配置了 Phoenix 地址也不会让模型参与。
- **它不是**：曝光日志、训练归因事件流、已下发去重历史。曝光（用户只是看到了帖子）**不要**发到这个 topic，见 §3.3。

---

## 2. 消息格式

### 2.1 载体

- 一条 Kafka message 的 **value** 是一个 UTF-8 编码的 JSON 对象。不是数组，不带 envelope，不是多行 JSON Lines。
- **key 和 headers 不读取**（消费端只取 `payload_view::<str>()`）。partition key 的建议见 §5。
- 一条行为一条消息。同一用户对同一帖子先点赞再评论，是两条消息。

### 2.2 字段

| 字段 | 类型 | 必填 | 含义 | 校验 |
|---|---|---|---|---|
| `user_id` | string | 是 | **做出行为的 viewer 的 `member_id`（皮）**。与 `home_mixer.proto` 的 `viewer_id` 是同一个 ID | 24 位小写 hex ObjectId，非 nil（不能全 0） |
| `tweet_id` | string | 是 | 被操作帖子的 `feed_id`，与 `RecommendationContent.feed_id` 相同 | 同上 |
| `author_id` | string | 是 | 帖子作者的 `creator_member_id`，与 `BatchGetRecommendationContents` 返回的该字段相同 | 同上 |
| `action_time_ms` | int64 | 是 | 行为**发生**时间，UTC epoch 毫秒。不是发送时间、不是消费时间 | `> 0` |
| `action_type` | int32 | 是 | `proto/definitions/phoenix_recsys.proto` 的 `ActionName` 枚举值 | `1..=18`；`0`、`19`、`20` 拒绝 |
| `product_surface` | int32 | 新生产者必填 | 行为发生时的产品入口。`0` 首页推荐、`1` 关注流、`2` 搜索、`3` 话题；`4..=15` 预留 | `0..=15` 的整数。省略时消费端当 `0`。字符串（如 `"timeline"`）整条丢弃 |

三个 ID 必须在同一身份空间（皮），不能混入 `account_id`、`user_id`、`user_no`。否则聚合出的历史序列和候选帖子的作者对不上，模型输入等于噪音。

客户端入口编码与触发时机见 [user-action-collect.md](./user-action-collect.md)。同帖多条行为聚合时保留**最早一条**的 `product_surface`。

### 2.3 示例

```json
{"user_id":"66f1a2b3c4d5e6f708192a3b","tweet_id":"66f1a2b3c4d5e6f708192a3c","author_id":"66f1a2b3c4d5e6f708192a3d","action_time_ms":1789516800000,"action_type":1,"product_surface":0}
```

### 2.4 校验与失败行为

校验只在 JSON 边界做一次（`UserActionEvent::validate`，`home-mixer/clients/uas_fetcher.rs`）。不通过的消息：

- 记一条 `warn` 日志（`dropping invalid UAS event at <topic>[<partition>]@<offset>: <reason>`），计入 `invalid` 计数；
- offset **照常推进**，不重试、不阻塞分区。

也就是说，**格式错误等于数据静默丢失**，mrpyq 侧必须在发送前保证格式正确。非 UTF-8 payload、空 payload 同样按无效处理。

### 2.5 扩展字段

消费端忽略未知字段（`serde` 默认行为），所以可以在消息里附加字段而不破坏兼容。`event_id` 仍可附带、消费端暂不读，见 §8.1。

---

## 3. `action_type` 映射

编号是 proto `ActionName` 的枚举值；「精排权重」取自 `home-mixer/params/param.rs`，乘的是模型对该行为的预测概率，绝对值越大对排序影响越大。`RuleFallbackScorer` 不用这些权重，它们只在模型路径生效。

### 3.1 第一批：必须有

权重非零且产品存在的行为。缺任何一个，模型就学不到那类信号。

| 编号 | `ActionName` | 产品动作 | 触发时机（建议） | 精排权重 |
|---|---|---|---|---|
| 1 | `SERVER_TWEET_FAV` | 点赞 | 服务端点赞落库成功后 | 0.5 |
| 2 | `SERVER_TWEET_REPLY` | 评论 / 回复帖子 | 服务端评论落库成功后 | 5.0 |
| 6 | `CLIENT_TWEET_CLICK` | 从 Feed 点进帖子详情 | 客户端进入详情页 | 0.4 |
| 9 | `CLIENT_TWEET_SHARE` | 分享（任意渠道） | 客户端分享面板任一渠道完成 | 2.0 |
| 10 | `CLIENT_TWEET_CLICK_SEND_VIA_DIRECT_MESSAGE` | 通过私信 / 站内消息分享 | 发送完成 | 5.0 |
| 11 | `CLIENT_TWEET_SHARE_VIA_COPY_LINK` | 复制帖子链接 | 复制完成 | 20.0 |
| 14 | `CLIENT_TWEET_FOLLOW_AUTHOR` | 从帖子上关注作者 | **客户端**在帖子上完成关注后上报并带 `tweet_id`（后端关注落库无法关联到具体帖子） | 4.0 |
| 16 | `CLIENT_TWEET_BLOCK_AUTHOR` | 拉黑作者 | **客户端**从帖子入口完成拉黑后上报并带 `tweet_id`（同上） | -31.2 |
| 17 | `CLIENT_TWEET_MUTE_AUTHOR` | 静音 / 不看作者 | **客户端**从帖子入口完成后上报并带 `tweet_id`（同上） | -58.8 |
| 18 | `CLIENT_TWEET_REPORT` | 举报帖子 | 举报提交成功后 | -234.0 |

分享同时命中 9 和 10 / 11 时，两条都发（例如复制链接分享发 9 和 11 两条）。

### 3.2 第二批：有就发

权重很小或为 0，但会进入用户历史特征，对召回 / 精排的序列表征有用。

| 编号 | `ActionName` | 产品动作 | 触发时机（建议） | 精排权重 |
|---|---|---|---|---|
| 5 | `CLIENT_TWEET_PHOTO_EXPAND` | 点开大图 | 客户端图片放大 | 0.05 |
| 8 | `CLIENT_TWEET_VIDEO_QUALITY_VIEW` | 视频有效观看 | 需 mrpyq 定义阈值；建议连续观看 ≥ 10 s 或播完（与 `MIN_VIDEO_DURATION_MS = 10_000` 对齐），每条帖子每次会话最多一次 | 0.05 |
| 12 | `CLIENT_TWEET_RECAP_DWELLED` | 停留 | 帖子在可视区停留 ≥ 2 s 记一次 | 0 |
| 7 | `CLIENT_TWEET_CLICK_PROFILE` | 点作者头像 / 进作者主页 | 客户端进入作者主页 | 0 |

### 3.3 不要发

| 类别 | 原因 |
|---|---|
| 3 转发、4 引用转发、13 点击引用帖 | 产品没有这些功能，权重已置 0（U5） |
| 15 不感兴趣 | 产品当前没有可采集的入口；权重置 0、从 home-mixer 必需列表移除（见 [phoenix-training-data-decisions.md §1](./phoenix-training-data-decisions.md)），将来有入口再加回 |
| 取消点赞、取消关注、解除拉黑等撤销类 | 枚举没有对应值；UAS 是行为历史，表达不了撤销 |
| **曝光 / 浏览 / 刷到** | 没有枚举位；聚合后 `action_mask` 全 false 的记录会被 `DenseAggregatedActionFilter` 丢掉。曝光属于训练归因的 served 事件流，是另一条待建的流，不要混进来 |
| 0、19、20 | 消费端直接拒绝 |

### 3.4 客户端事件依赖埋点通道

第一批里 6、9、10、11、14、16、17 和第二批全部依赖**客户端**上报：6、9、10、11 本身是客户端动作；14、16、17 虽然有后端落库，但后端不知道动作来自哪条帖子，`tweet_id` 只能由客户端补。需要确认 mrpyq 是否有客户端埋点上报入口可以转发到同一 topic。当前结论（2026-09-16）：后端可直接采集的只有 1、2、18，第一版模型 head 集合据此收缩，其余待客户端埋点上线后按 [phoenix-training-data-decisions.md §1.4](./phoenix-training-data-decisions.md) 加回；这不阻塞上线，但要在排期里明确。

---

## 4. 身份空间

与 [mrpyq-member-dimension-requirements.md](./mrpyq-member-dimension-requirements.md) 的原则一致：接口层只出现皮的 `member_id`。

| 字段 | mrpyq 侧取值来源 |
|---|---|
| `user_id` | 做出行为的皮自己的 ObjectId（发帖链路里 `MemberId: profile.Id` 用的那个 `profile.Id`），不是登录账号 |
| `tweet_id` | 帖子主键，即 `RecommendationContent.feed_id` 返回的同一个值 |
| `author_id` | 帖子落库时写入的 `Feed.MemberId`（皮 ID），即 `RecommendationContent.creator_member_id` |

一个账号下有多个皮时，行为归属到实际操作的那个皮。这是 Home Mixer 按皮出 Feed 的前提。

---

## 5. 投递语义：mrpyq 需要保证 / 不需要保证

消费端实现见 `home-mixer/bin/uas_worker.rs` 与 `home-mixer/clients/uas_fetcher.rs`。

| 项 | 要求 | 消费端依据 |
|---|---|---|
| 投递次数 | **at-least-once 即可**，重复投递无害 | 写入幂等：相同六元组（含 `product_surface`）序列化为字节相同的 ZSET 成员（`StoredUserAction` 字段顺序固定），重放不产生重复 |
| 顺序 | **不要求有序** | ZSET 按 `action_time_ms` 排序，乱序到达无影响 |
| partition key | 建议用 `user_id` | 消费端不依赖；但多实例按分区扩展时同一用户落同一实例，避免热点交叉 |
| 时间窗 | `action_time_ms` 与真实时间偏差控制在分钟级，生产机 NTP 对时 | 早于 `now − 7 天` 的事件跳过不写（`skipped_outside_window`）；晚于 `now + 5 分钟` 的当异常跳过（`skipped_future`）；两者都推进 offset |
| 历史回填 | 只有最近 7 天的有意义 | 首次消费 `auto.offset.reset=earliest` 会从头读，超过 7 天的被跳过，不报错但浪费时间 |
| 每用户存量 | 无需限流 | 每用户只保留最新 600 条原始行为（`UAS_STORE_MAX_ACTIONS`），key TTL 7 天随写刷新 |
| 消息大小 | 约 200 字节 | — |
| 压缩 | producer 侧决定，对消费端透明 | 本仓库 `rdkafka` 启用 `libz`（gzip）与 `zstd` feature |
| retention | 建议 ≥ 7 天 | 与在线窗口一致，消费端故障恢复后能补齐窗口内数据 |
| 发送时机 | 服务端动作在**落库成功后**发；客户端动作在动作完成时发 | 发了却没落库的点赞会污染历史 |

数据在模型侧的实际可见范围由 `uas-worker`、Redis 投影和 xrex serving 合同共同决定；不要引用已删除 gateway 的历史 32 条规格。当前窗口、序列长度和特征宽度以 [Phoenix 训练与数据](../phoenix/06-training-and-data.md)及实际 serving 配置为准。

---

## 6. 消费端配置与 mrpyq 需交付的信息

### 6.1 消费端读取的环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `UAS_KAFKA_BROKERS` | 必填 | bootstrap servers |
| `UAS_KAFKA_TOPIC` | 必填 | topic 名 |
| `UAS_KAFKA_GROUP_ID` | `home-mixer-uas-projector` | 消费组 |
| `UAS_KAFKA_AUTO_OFFSET_RESET` | `earliest` | 无 committed offset 时的起点 |
| `UAS_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` |
| `UAS_KAFKA_SASL_MECHANISM` / `UAS_KAFKA_SASL_USERNAME` / `UAS_KAFKA_SASL_PASSWORD` | `PLAIN` / — / — | 仅 SASL 协议需要。`SSL` / `SASL_SSL` / SCRAM 要求以 `--features kafka-ssl` 构建（容器镜像默认如此） |
| `UAS_WORKER_METRICS_PORT` | `9091` | 管理 HTTP 端口：`/healthz`、`/readyz`（订阅成功后 200）、`/metrics`；`0` 关闭 |

offset 管理：`enable.auto.commit=true` + `enable.auto.offset.store=false`，每条消息处理完（写入、跳过或判定无效）后手工 store，librdkafka 周期提交；收到 SIGTERM / Ctrl-C 时同步提交后退出。每条有效事件先确保 user / post / author 已在 ID Registry 注册（本地有上限缓存命中时不发 RPC），再写 UAS Redis；任一依赖失败都在进程内按 100 ms 起步、5 s 上限指数退避重试，总预算 60 s。预算耗尽 job 退出、该 offset 不提交，由进程管理器重启后重放。

### 6.2 mrpyq 需交付

- [ ] broker 地址、topic 名
- [ ] 认证方式与凭据（或确认内网 PLAINTEXT）
- [ ] 分区数与预计事件 QPS（用于决定 `uas-worker` 实例数；单实例串行，吞吐同时受 ID Registry allocation 与 UAS Redis 往返限制）
- [ ] retention 设置（建议 ≥ 7 天）
- [ ] §3 表中每一类动作是否可发、由服务端还是客户端发、触发时机
- [ ] 视频有效观看（8）和停留（12）的阈值定义
- [ ] 生产机时钟同步确认

---

## 7. 联调与验收

1. **格式自检（不需要 Kafka）**：mrpyq 侧导出几条真实事件 JSON，每行一条，用 stdin 模式投影到本地 Redis，确认没有 `dropping invalid UAS event` 日志：

   ```bash
   cat events.jsonl | UAS_REDIS_URL=redis://localhost:6379/ RUST_LOG=info cargo run -p home-mixer --bin uas-worker
   ```

2. **Kafka 试跑**：mrpyq 往测试 topic 发事件，推荐侧以 Kafka 模式运行 `uas-worker`，观察每 60 s 一行的统计日志 `uas-worker: projected=... skipped_outside_window=... skipped_future=... invalid=... storage_retries=... storage_failures=...`，或抓 `:9091/metrics` 的 `uas_worker_events_total{outcome}`（两者同源）。依赖失败用 `home_mixer_client_calls_total` 归因：`id_registry/allocate_grpc` 对应身份注册，`redis_uas/write` 对应 Redis 投影。`invalid` 应为 0；`skipped_future` 持续非零说明 mrpyq 侧时钟快；`uas_worker_consumer_lag` 不收敛说明单实例吞吐不够。

3. **Redis 内容检查**（注意 key 带 hash tag 花括号）：

   ```bash
   redis-cli ZRANGE 'home_mixer:uas:{66f1a2b3c4d5e6f708192a3b}:actions' 0 -1 WITHSCORES
   ```

   成员是 `{"version":2,"tweet_id":...,"author_id":...,"action_time_ms":...,"action_type":...,"product_surface":...}`，score 是 `action_time_ms`。窗口内的 v1 成员仍可读，`product_surface` 当 0。

4. **Home Mixer 侧确认**：对该皮发一次推荐请求。之前 QueryHydrator 阶段会有 `failed: ... No user actions found for user <id>` 的错误日志，接通后消失；若同时配置了 `PHOENIX_RETRIEVAL_GRPC_ADDR` / `PHOENIX_PREDICT_GRPC_ADDR`，会出现 `phoenix rpc Retrieve ...` 与 `phoenix rpc PredictNextActions ... scored=N` 日志，说明模型路径已被调用。

5. **幂等回归**：把同一批消息重发一遍，`ZCARD` 不变。

---

## 8. 已知限制与后续

### 8.1 建议现在就写进消息、消费端后续再接的字段

| 字段 | 类型 | 用途 | 现状 |
|---|---|---|---|
| `event_id` | string | 幂等键、排障关联 | 本流不需要；但曝光 / 训练归因流一定需要，现在统一省事 |

`product_surface` 已进入事件、Redis `StoredUserAction` v2 和 `DefaultAggregator`。客户端编码见 [user-action-collect.md](./user-action-collect.md)。当前请求的候选侧 `candidate_product_surface` 仍为 0，不影响历史序列。

### 8.2 撤销类动作

取消点赞等不进入历史。如果产品需要「撤销后不再把该行为当正样本」，要在训练归因侧处理，UAS 在线序列不承担。

### 8.3 训练文档的行为编码与在线合同不一致（已修）

**2026-09-17 已对齐**：训练输入字段和编码以 [Phoenix 训练与数据](../phoenix/06-training-and-data.md) 为准；日志层使用 proto `ActionName` 枚举值，ID 字段使用 24 位 hex ObjectId 字符串。

补充澄清：日志层枚举与模型内部张量列序由 xrex 数据加载器统一转换。数据平台只接触 proto 编号，不要自行改写列序；具体字段以 [Phoenix 训练与数据](../phoenix/06-training-and-data.md)为准。

规则不变：**给 mrpyq 的一律用 proto 编号**。

### 8.4 吞吐

`uas-worker` 单实例串行处理。稳态每条事件有一次 UAS Redis ZSET 写入；首次看到某个身份、本地 allocation 缓存未命中或缓存达到上限时，还会先执行一次 ID Registry `AllocateBatch`。Registry 内部可能为新映射执行多轮 Redis 操作，因此不能再按“一事件一次 Redis 往返”估算容量。事件 QPS 高于单实例能力时按 topic 分区起多实例，mapping 注册和 ZSET 写入均幂等。每个实例在 `UAS_WORKER_METRICS_PORT`（默认 9091）暴露 `uas_worker_consumer_lag{topic,partition}` 与 `uas_worker_last_projected_action_timestamp_seconds`；lag 持续增长或投影时间戳落后当前时间过久，就是该压测依赖或增加实例的信号。

---

## 附：消费端代码索引

| 关注点 | 位置 |
|---|---|
| 事件结构与校验 | `home-mixer/clients/uas_fetcher.rs`：`UserActionEvent`、`UserActionEvent::validate`、`StoredUserAction` |
| 行为类型范围真源 | `home-mixer/recsys_compat/mod.rs`：`ACTION_MASK_LEN`、`is_supported_action_type` |
| 枚举定义 | `proto/definitions/phoenix_recsys.proto`：`ActionName` |
| Kafka 消费、offset、重试、关停 | `home-mixer/bin/uas_worker.rs` |
| 窗口、存量、时钟偏差常量 | `home-mixer/params/config.rs`：`UAS_WINDOW_TIME_MS`、`UAS_STORE_MAX_ACTIONS`、`UAS_MAX_FUTURE_SKEW_MS`、`UAS_PROJECTION_RETRY_BUDGET_MS` |
| 读取侧聚合 | `home-mixer/query_hydrators/user_action_seq_query_hydrator.rs` |
| 精排权重 | `home-mixer/params/param.rs`、`home-mixer/scorers/phoenix_scorer.rs` |
| Redis 集成测试 | `home-mixer/tests/redis_uas.rs`（`cargo test -p home-mixer --test redis_uas -- --ignored`） |
