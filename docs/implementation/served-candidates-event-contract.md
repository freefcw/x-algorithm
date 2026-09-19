# 服务端曝光事件流合同：Home Mixer 下发记录（SE-11）

> **状态**：`implemented`（生产端 `ServedCandidatesKafkaSideEffect` + `ServedCandidatesSink` 已实现并有单测 / 端到端测试；topic、保留期与离线消费方待确定）
> **日期**：2026-09-16
> **读者**：数据 / 训练侧、mrpyq webapi 与客户端埋点、推荐服务开发
> **事实边界**：本文「生产端行为」引自本仓库当前代码并标注文件；对埋点侧和数据侧的「要求」是推荐侧提出的约定，未经确认
> **配套文档**：行为（反馈）事件流见 [uas-event-contract.md](./uas-event-contract.md)；环境变量见 [home-mixer/07-config-and-params.md §3](../home-mixer/07-config-and-params.md)；xrex 训练输入见 [Phoenix 训练与数据](../phoenix/06-training-and-data.md)

---

## 1. 这条流是什么、不是什么

Phoenix 第一版模型的训练样本 = **曝光**（谁在什么时候看到了哪些帖子、排在第几位）× **反馈**（之后对其中哪些帖子做了什么）。反馈来自 [UAS 行为事件流](./uas-event-contract.md)；曝光的服务端半边就是这条流：

```
Home Mixer ScoredPosts 请求 ──最终下发列表──▶ ServedCandidatesKafkaSideEffect ──ServedCandidatesSink──▶ Kafka topic / JSON Lines
                                                                                                    └──▶ 离线落仓 ──join 反馈──▶ 训练样本
```

- **生产方**：`home-mixer/side_effects/served_candidates_kafka_side_effect.rs`（事件 schema）+ `home-mixer/clients/served_candidates_sink.rs`（传输）。装配在内层 `PhoenixCandidatePipeline`，配置 `SERVED_EVENTS_*` 后启用，每个非空响应一条事件。
- **它记录的是服务端下发**，不是客户端真实展示。客户端只渲染了前 N 条、用户只滑到了第 K 条，这些信息服务端不知道。客户端曝光埋点（§6）回传后按 `request_id` + `post_id` 关联，才能得到"真实曝光"。
- **它不是**：去重历史（那是 `FeedStateServedPersistence` 写的 Redis ZSET，无 position）、行为事件（点赞 / 评论走 UAS topic）。
- **没有它的后果**：没有负样本。只有互动日志时，`data_preprocessor.py` 只能拿随机帖子当负例，模型学到的是"热门 vs 随机"，不是"看到但没点 vs 看到并点了"。

---

## 2. 消息格式

### 2.1 载体

- 一条 Kafka message 的 **value** 是一个 UTF-8 编码的 JSON 对象；**key** 是 `viewer_id`（24 位 hex 字符串），同一用户的曝光落在同一分区，便于按用户与行为事件 join。
- 一次请求一条消息，`candidates` 数组按下发顺序排列。
- JSON Lines 模式（`SERVED_EVENTS_JSONL_PATH`）写出的每一行与 Kafka value 完全相同。

### 2.2 顶层字段

| 字段 | 类型 | 含义 | 来源 |
|---|---|---|---|
| `schema_version` | int | 固定 `1`。字段只增不改；不兼容变更递增版本，消费方按版本解码 | `SERVED_CANDIDATES_EVENT_SCHEMA_VERSION` |
| `request_id` | string | 请求标识，**幂等键**：同一 `request_id` 重复投递时按整条覆盖。与服务端日志 `request_id=...`、ForYou 响应的 `request_id` 相同 | `ScoredPostsQuery.request_id` |
| `prediction_request_id` | uint64 | 发给 Phoenix 精排的请求 ID，用于关联模型侧日志 | `ScoredPostsQuery.prediction_id` |
| `viewer_id` | string | viewer 的 `member_id`（皮），24 位小写 hex ObjectId，与 UAS 事件的 `user_id` 同一身份空间 | `ScoredPostsQuery.user_id` |
| `request_time_ms` | int64 | 请求时间，UTC epoch 毫秒。反馈归因窗口以它为起点 | `ScoredPostsQuery.request_time_ms` |
| `is_shadow_traffic` | bool | 影子流量标记。**不再是发布门槛**（装配即发布），由消费方决定是否纳入训练 | proto 透传 |
| `in_network_only` | bool | 请求是否只要网内候选 | proto 透传 |
| `is_bottom_request` | bool | 是否下翻请求 | proto 透传 |
| `client_app_id` | int32 | 客户端标识 | proto 透传 |
| `candidates` | array | 见 §2.3；非空（空响应不发事件） | 最终下发列表 |

### 2.3 `candidates[]` 字段

| 字段 | 类型 | 含义 |
|---|---|---|
| `position` | uint32 | 最终列表名次，从 0 开始。ForYou 外层混入 WhoToFollow / Prompt 等模块后客户端位置可能不同，但内层排序名次以此为准 |
| `post_id` | string | 帖子 `feed_id`，24 位小写 hex ObjectId |
| `author_id` | string | 作者 `creator_member_id`（皮） |
| `retweeted_post_id` | string \| null | 转帖时的原帖 ID。精排对转帖打的是原帖，模型侧关联用它 |
| `served_type` | string \| null | proto `ServedType` 枚举名：`FOR_YOU_IN_NETWORK` / `FOR_YOU_PHOENIX_RETRIEVAL` / `FOR_YOU_PHOENIX_TOPICS` / `FOR_YOU_CACHED_POST` 等。注意兜底召回目前也标 `FOR_YOU_PHOENIX_RETRIEVAL`（`FallbackSource` 复用该枚举，见 trunk-review R9） |
| `in_network` | bool \| null | 是否网内候选 |
| `score` | double \| null | 参与选择的最终分 |
| `weighted_score` | double \| null | 多目标加权分（多样性 / 网外降权之前）；规则兜底时为 null |
| `degraded_reason` | string \| null | 非空表示该请求不是模型排序：`phoenix_missing_sequence`（无行为序列）、`phoenix_unavailable: ...`（网关失败或元数据被拒）。训练样本应按此字段区分"模型流量"与"规则流量" |
| `created_at_ms` | uint64 \| null | 帖子发布时间 |

可选字段统一以 `null` 出现，不省略，消费方只需处理一种形状。

### 2.4 示例

```json
{"schema_version":1,"request_id":"1789552148488-66f1a2b3c4d5e6f708192a3b","prediction_request_id":4242,"viewer_id":"66f1a2b3c4d5e6f708192a3b","request_time_ms":1789552148488,"is_shadow_traffic":false,"in_network_only":false,"is_bottom_request":false,"client_app_id":9,"candidates":[{"position":0,"post_id":"66f1a2b3c4d5e6f708192a3c","author_id":"66f1a2b3c4d5e6f708192a3d","retweeted_post_id":null,"served_type":"FOR_YOU_PHOENIX_RETRIEVAL","in_network":false,"score":0.7312,"weighted_score":0.9021,"degraded_reason":null,"created_at_ms":1789540000000}]}
```

---

## 3. 投递语义

- **异步、不进请求预算**：side effect 在响应之后由 `CandidatePipeline::run_side_effects` 在进程级任务追踪器上执行（`candidate-pipeline/candidate_pipeline.rs`），失败只记 `error` 日志（`stage=SideEffect component=ServedCandidatesKafkaSideEffect failed: ...`），不影响响应、不重试；单次运行上限 `SIDE_EFFECT_TIMEOUT_MS`（10 s），超时按失败计。
- **关停排空**：home-mixer 收到 SIGTERM 后先排空 gRPC 在途请求，再把 `--drain-timeout-secs`（默认 20 s）剩余的预算用来等仍在运行的 side effect，最后调用 sink 的 `shutdown` flush librdkafka 队列。正常滚动更新不丢事件。
- **at-most-once（生产端）**：进程在发送前崩溃、或被 SIGKILL（排空超出平台宽限期）则该请求的曝光丢失。Kafka sink 开启 `enable.idempotence=true`（acks=all），broker 侧重试不会重复。丢失率由 §7 的指标对账。
- **单条投递上限** `SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS`（默认 5 s）。
- **消费方幂等**：按 `request_id` 去重 / 覆盖。

---

## 4. 配置

| 变量 | 作用 |
|---|---|
| `SERVED_EVENTS_KAFKA_BROKERS` + `SERVED_EVENTS_KAFKA_TOPIC` | 启用 Kafka sink（需 `cargo build -p home-mixer --features kafka`） |
| `SERVED_EVENTS_KAFKA_SECURITY_PROTOCOL` / `SERVED_EVENTS_KAFKA_SASL_MECHANISM` / `SERVED_EVENTS_KAFKA_SASL_USERNAME` / `SERVED_EVENTS_KAFKA_SASL_PASSWORD` | 与 `uas-worker` 的 `UAS_KAFKA_*` 同规则 |
| `SERVED_EVENTS_KAFKA_DELIVERY_TIMEOUT_MS` | 默认 5000 |
| `SERVED_EVENTS_JSONL_PATH` | 本地联调 / 抓样本：追加写 JSON Lines 文件；与 Kafka 变量互斥，多副本不要用 |

未配置任一目标时不装配 side effect；非 demo 启动时打 `warn`：`served-candidates exposure log is disabled ...`。

本地验证：

```bash
SERVED_EVENTS_JSONL_PATH=/tmp/served.jsonl cargo run -p home-mixer
# 使用真实的 ScoredPosts/ForYou RPC 客户端发送测试请求
tail -1 /tmp/served.jsonl | python3 -m json.tool
```

---

## 5. 对数据 / 训练侧的要求

参考实现：`phoenix/scripts/build_training_inputs.py` 直接消费两条流的 JSON Lines 落地文件，完成下面 2–3 的 join 与标签构造，产出 `data_preprocessor.py --impressions-dir` 的输入；数仓侧可以按同一规则用 SQL 复刻。

1. **落仓**：topic → 对象存储 / 数仓，按 `request_time_ms` 日分区，保留期建议 ≥ 90 天（训练回溯）。
2. **归因 join**：曝光 `(viewer_id, post_id, request_time_ms)` × UAS 行为 `(user_id, tweet_id, action_time_ms, action_type)`，条件 `action_time_ms ∈ [request_time_ms, request_time_ms + 归因窗口]`；同一帖在窗口内多次下发时归到 `request_time_ms` 最大（最近）的一次。归因窗口是 join 任务的配置项 `--attribution-window-minutes`，默认 30、允许 60（决策见 [phoenix-training-data-decisions.md §2](./phoenix-training-data-decisions.md)）。
3. **标签**：每个 `(request_id, post_id)` 一行，19 维 multi-hot 按 `phoenix/data_preprocessor.py::BEHAVIOR_FIELDS` 顺序；`action_type` → 列名映射见 `phoenix/services/model_contract.py::ACTION_IDX_TO_ENUM` 的反向。没有任何行为的曝光是负样本。
4. **过滤**：`degraded_reason != null` 的请求可以作为样本（曝光本身真实），但评估"模型 vs 规则"时必须分开统计；`is_shadow_traffic=true` 默认排除。
5. **ID 空间**：所有 ID 是 24 位小写 hex ObjectId 字符串，训练侧原样喂 `hash_id_to_ints`，不要转整数、不要映射到 `account_id`。

---

## 6. 对埋点侧的要求（客户端真实曝光）

服务端事件只知道"下发了什么"。要得到真实曝光，客户端需要在帖子进入可视区域（建议停留 ≥ 1 s 或可见面积 ≥ 50%）时上报：

| 字段 | 说明 |
|---|---|
| `request_id` | 从 ForYou / ScoredPosts 响应透传，不可缺 |
| `post_id` | 被展示的帖子 |
| `impressed_time_ms` | 进入可视区域时间 |
| `position` | 客户端实际展示位置（可选，用于对账） |

发到哪个 topic 待定：可以是 UAS topic 的一个新 `action_type`（需要 proto 新增枚举，当前 `1..=18` 之外会被 `uas-worker` 拒绝），也可以是独立 topic。推荐侧建议独立 topic，避免把"看到"混进模型历史序列（`uas-event-contract.md` §3.3 同样要求曝光不进 UAS）。

---

## 7. 监控与对账

- 生产端指标（`/metrics`，见 [home-mixer/07 §4.3](../home-mixer/07-config-and-params.md#43-指标)）：`home_mixer_served_events_total{result="ok"|"error"}`（交给 sink 的事件数）、`home_mixer_served_event_candidates_total`（成功事件携带的候选数）、`home_mixer_served_event_publish_duration_seconds`（Kafka ack 耗时）；side effect 层还有 `home_mixer_side_effect_runs_total{component="ServedCandidatesKafkaSideEffect",result}`。每次成功 / 失败也有 request-scoped 日志。
- 对账口径：`home_mixer_served_events_total{result="ok"} ≈ home_mixer_rpc_requests_total{code="OK"}` 中非空响应的部分（空响应不发事件）。差值即曝光丢失率。
- 反馈关联率：有 ≥ 1 条行为的曝光占比；异常低说明 join 键或时钟不一致。

---

## 8. 决策状态

已定（2026-09-16，详见 [phoenix-training-data-decisions.md](./phoenix-training-data-decisions.md)）：

- Kafka topic `home-mixer.served-candidates`，首版 6 分区，QPS 明确后只增不减；不以"与 UAS topic 同分区"为设计前提，归因 join 在数仓按键完成。
- 保留：Kafka 7 天，数仓 ≥ 90 天。
- 归因窗口：配置项，默认 30 分钟，允许 60。
- 影子流量：记录但训练默认排除。

仍开放：

- 客户端真实曝光走独立 topic（§6），字段与 topic 名待埋点侧确认。
- 是否记录逐 head 预测概率：v1 不记，需要时以 `schema_version=2` 追加。
