# 主干收敛方案实现审查报告

> **审查对象**：`docs/implementation/phoenix-pipeline-trunk-plan.md`（基线版，状态 `decision`，路径 `/Users/hejun/work/mp/x-algorithm/docs/implementation/phoenix-pipeline-trunk-plan.md`）与 `mp-trunk` 工作树实现
> **审查日期**：2026-09-12
> **审查方式**：逐条比对计划 §1–§12 的模块、接口、数据流与代码实体；实跑 `cargo test` / `cargo clippy` / `pytest` 复核验收项
> **审查范围**：`home-mixer/`、`candidate-pipeline/`、`proto/`、`thunder/`、`vm-ranker/`、`phoenix/`、`docs/upstream-sync/`、`testdata/`
>
> **落库说明（2026-09-13）**：下文是提交前工作树的审查快照，正文结论不回写。随后已按三次提交落库：`2725693`（P3 上游可移植代码）、`fa83cbf`（P1+P2 身份与主干契约；U5 六个专用文件已物理删除）、本文档所在的 docs 提交。计划 §13 的 382/305/304 与本次审查实测 377/297 不一致，以各次运行当时计数为准。

---

## 0. 结论摘要

| 维度 | 结论 |
| --- | --- |
| 架构主干 | **符合计划**。`PhoenixCandidatePipeline::build_with_clients()` 是唯一装配点；`candidate-pipeline` 框架、U4 ID newtype、U5 卸装（装配层）、P2 新增组件均已到位 |
| 数据流顺序 | **符合计划**。Query 水合 → 召回 → 候选水合 → 过滤 → 精排 → TopK → 选后水合 → 选后过滤 → 落库 的阶段与组件顺序与 §3 逐项一致 |
| 未实现项 | **9 项**（其中 3 项属 §9 P2 明确要求的交付内容）：U1 业务适配器清单全部缺失、feedback RPC、Bearer/viewer 双校验、请求级 deadline 下传、`ImpressedPosts` 水合未装配、真实 `ServedPersistence`、U5 物理删除 |
| 偏差项 | **8 项**，多数是为 `legacy-int-ids` 延期或计划外 API 引入的妥协 |
| 高风险问题 | **2 项**：① **P1–P3 全部成果未提交**（HEAD 仅到 P0）；② 非 Demo 模式下 served 落库静默 no-op，破坏"落库成功才响应"的归因纪律 |
| 验收复核 | `cargo test --workspace` **377 passed / 0 failed**；`cargo clippy --workspace --all-targets -- -D warnings` **0 warning**；`uv run pytest -q` **115 passed**；`./scripts/run_demo.sh` **端到端通过，返回 35 条**。计划 §9 与 §13 记录的数字（382 / 305 / 304）与实测不符 |
| §6.4 u64 去向 | **已复核，符合计划**。`models/` 内 ID 位置零 `u64`/`i64` 残留；剩余 170 处全部落在计划保留类别（时间戳 / 计数 / 阈值 / 本地 ID / 话题 ID） |

---

## 1. 交付状态（最高优先级发现）

**当前 HEAD = `b573d67 docs: 记录 P0 执行结果`。P1（ID 替换）、P2（契约落位 + U5 卸装）、P3（追上游）的全部实现均未提交。**

```
git status --porcelain | wc -l   →  167
  ??  13  （未跟踪）
  A    6  （已暂存新增，均为 phoenix/xrex）
  D    5
  M  140
  MM   3
```

- **未跟踪（untracked）的关键实现文件**：
  `home-mixer/models/ids.rs`、`home-mixer/clients/in_network_posts_client.rs`、`home-mixer/clients/served_persistence.rs`、`home-mixer/filters/first_stage_eligible_filter.rs`、`home-mixer/scorers/rule_fallback_scorer.rs`、`home-mixer/sources/fallback_source.rs`、`home-mixer/tests/p2_pipeline_assembly.rs`、`phoenix/tests/test_object_id_hash.py`、`testdata/`（黄金向量）、`docs/upstream-sync/` 四份新能力清单
- **验证**：`git show HEAD:home-mixer/candidate_pipeline/phoenix_candidate_pipeline.rs | grep -c RuleFallbackScorer` → `0`；`git ls-files --error-unmatch home-mixer/models/ids.rs` → 报错
- **风险**：`git checkout .` / `git clean -fd` / 切换分支即丢失全部 P1–P3；§13 执行记录中"P1 has landed"指的是工作树状态，不是仓库状态
- **建议**：按 P1 / P2 / P3 三个独立提交落库（计划 §11 已要求 ID 替换单独一个 PR），提交前补齐 `testdata/` 与四份能力清单

---

## 2. 逐模块对照

图例：**✅ 一致** ｜ **⚠️ 偏差** ｜ **❌ 未实现**

### 2.1 框架 / 治理（§5 第一行、§9 P0）

| 计划项 | 状态 | 实现位置与说明 |
| --- | --- | --- |
| `candidate-pipeline` 为唯一编排框架 | ✅ | `candidate-pipeline/`，21 项单测通过 |
| `PhoenixCandidatePipeline` + `build_with_clients()` 为唯一装配点 | ✅ | `phoenix_candidate_pipeline.rs:163`；入参结构 `PhoenixDependencies`（含 `fallback_client`、`moe_retrieval_client` 可选端口） |
| `runtime_config` + `feature_policy` + `debug_access` | ✅ | `runtime_config.rs`（Demo/Degraded/ProductionReady 三态）、`feature_policy.rs`（7 个开关默认 false）、`debug_access.rs` |
| `docs/upstream-sync` 增加 U4 / U5 规则 | ✅ | `upstream-first-maintenance.md:24–40`，U4/U5 定义与计划 §4 逐字一致，并附 U4 摩擦实测 |
| `mrpyq_recommendation_data_client` 保留 | ⚠️ | 文件存在且含完整 `GrpcMrpyqRecommendationDataClient`，但**除自身外无任何引用**（`clients/mod.rs` 仅注册模块）——孤立代码，未接入流水线 |
| `phoenix_recsys.proto`（string ID）+ 保留 mp 的其余 4 个 proto | ✅ | `proto/build.rs` 编译 5 个 proto；`phoenix_recsys.proto` 命名与"避开 Python descriptor 撞名"注释均保留 |
| `recommendation-service/` 删除 | ✅ | 目录不存在（`mp` 分支本无此 crate，故为空操作） |
| `BusinessFeedService` + `business_feed/` 删除 | ✅ | 全仓库无 `business_feed/`；`server.rs` 仅注册 ScoredPosts / ForYou 两个服务 |
| `production_ready` 拒绝条件改为"业务适配器契约未验证" | ✅ | `runtime_config.rs:68–72`，拒绝文案列出 caller identity / TES / UAS / Strato / VF / in-network / Phoenix metadata / served persist |

### 2.2 ID 方案 A（§6）

| 计划项 | 状态 | 实现位置与说明 |
| --- | --- | --- |
| `ObjectId([u8; 12])` 私有字段 + `PostId`/`UserId` 别名 | ✅ | `models/ids.rs:13–16` |
| `NIL` / `parse` / `parse_optional` / `is_nil` / `as_bytes` / `timestamp_secs` / `to_u64_hash` / `from_parts` | ✅ | 全部实现；`parse` 严格拒绝大写、非 24 位、空串；`parse_optional("")` → `None`，`"0"` fail-closed |
| `Display` / `Debug` 固定小写 24-hex；`FromStr`；serde 字符串 | ✅ | `ids.rs:130–164`，含单测锁定 |
| `#[cfg(test)] From<u64>`（末 8 字节零填充） | ⚠️ | 存在，但同时暴露了**计划外**的生产 API `from_u64_be_padded` / `to_u64_be_padded`（`ids.rs:112–127`）。§6.2 明确"生产禁止从整数构造"，此二函数在非 test 编译下可用 |
| `to_u64_hash` = md5(12 原始字节)[0..8] BE，清高位，0→1 | ✅ | `ids.rs:88–98`；`md5` 已在 `home-mixer/Cargo.toml` |
| 不取末 8 字节（避免进程内计数器回绕碰撞） | ✅ | 取 `digest[..8]` |
| Rust + Python 各一份实现，共享黄金向量，`cargo test` 与 `pytest` 同时校验 | ✅ | `ids.rs:256` 读 `testdata/object_id_u64_hash.json`；`phoenix/services/model_contract.py:20 object_id_to_u64_hash`；`phoenix/tests/test_object_id_hash.py` 读同一文件并校验大写/短串拒绝 |
| `home_mixer.proto` ID 改 string | ✅ | `viewer_id` / `seen_ids` / `served_ids` / `tweet_id` / `author_id` / `retweeted_*` / `ancestors` / `screen_names` 键全为 `string`；`prediction_request_id` 保留 `uint64`（符合 §6.4"本地相关 ID 保留 u64"） |
| `PostCandidate.created_at_ms: Option<u64>`（U2）+ AgeFilter 改读、缺失回退 `timestamp_secs()` | ✅ | `models/candidate.rs:27`；`filters/age_filter.rs:17–22` |
| 删除 `util/snowflake.rs` | ✅ | `util/` 仅剩 `bloom_filter.rs` / `candidates_util.rs` / `mod.rs` / `request_util.rs` |
| `demo.rs` 改 `demo_object_id(seq)` / `from_parts(ts, seq)` | ⚠️ | 新增了 `demo_author_object_id_hex` / `padded_object_id_hex` / `phoenix_demo_post_id`，但 **`DEMO_AUTHOR_IDS` 仍是 `[i64; 5]`，`snowflake_id()` 仍存在**且被 `thunder/demo_seed.rs:13,33` 使用 |
| Bloom 改字节输入：`murmur_hash(&[u8])` | ✅ | `util/bloom_filter.rs:63` |
| thunder / vm-ranker 两个 gRPC 适配器加 feature gate | ✅ | `Cargo.toml` `default = ["legacy-int-ids"]`；`thunder_client` / `vm_ranker_client` / `GrpcVMRankerClient` / `ThunderClient` 均 `#[cfg(feature = "legacy-int-ids")]` |
| 测试字面量改 `pid(n)` / `uid(n)` | ✅ | 全仓库广泛使用，`ids.rs:173–179` 提供助手 |

### 2.3 Query 水合（§3、§5）

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| ScoringSeq / RetrievalSeq ← `UserActionSequenceOps` | ✅ | `phoenix_candidate_pipeline.rs:181–185`，二者共享同一 request-scoped `OnceCell` 缓存 |
| Followed / Blocked / Muted / SafetyFeatures ← `StratoClient` | ✅ | `:186–195` |
| ImpressedPosts ← `ImpressedPostsClient` | ❌ | 端口与 `ImpressedPostsQueryHydrator` 均已实现（`clients/impressed_posts_client.rs`、`query_hydrators/impressed_posts_query_hydrator.rs`），但**未出现在 `build_with_clients` 的 query_hydrators 列表**中。当前曝光历史只能由请求方通过 `impressed_post_ids` 传入 |
| ServedHistory + PastRequestTimestamps + `InMemoryFeedStateStore` | ✅ | `with_feed_state_store` / `install_feed_state_store`（`:146–161`）；`ScoredPostsServer::new` 注入（`scored_posts_server.rs:32–36`）；`p2_pipeline_assembly.rs` 断言二者位于 query_hydrators[0]/[1] |
| SubscribedUserIds QH 删除 | ⚠️ | 文件仍在且 `query_hydrators/mod.rs` 注册（仅未装配） |
| UserTopics / Bloom 端口可选 | ✅ | 均未装配；`UserTopicsQueryHydrator` 仅在显式传入 `TopicPersonalizationClients` 时插入 |

### 2.4 召回（§3、§5）

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| `ThunderSource` ← `InNetworkPostsClient` trait 抽取 | ✅ | `clients/in_network_posts_client.rs:26` 定义 trait（`get_in_network_posts` + 默认 `get_fallback_posts`）；`ThunderClient` 以 `#[cfg(feature="legacy-int-ids")]` 实现之；`DisabledInNetworkPostsClient` fail-closed 返回 `Err` |
| `PhoenixSource` ← `PhoenixRetrievalClient` | ✅ | `sources/phoenix_source.rs`；`ProdPhoenixRetrievalClient` 无地址时显式不可用 |
| `FallbackSource`（U2 新增）← 业务 FALLBACK 池 | ⚠️ | `sources/fallback_source.rs`；`enable()` = `!in_network_only && !has_cached_posts`；**仅 Demo 装配**，非 Demo 留空（`p2_pipeline_assembly.rs` 断言 Degraded 不含 FallbackSource）。已在 `run_demo.sh` 中实测产出候选，但 `served_type` 复用 `ForYouPhoenixRetrieval`，与真实 Phoenix 召回不可区分（见 R9） |
| Topics / MoE / CachedPosts "保留形态，不生效" | ⚠️ | Topics/MoE 开关默认关 ✓；**`CachedPostsSource` 被无条件装配**（`:226`），`enable()` = `query.has_cached_posts`，即请求携带 `cached_posts` 时确实生效。§5 将其列在"可选 = 留在磁盘、不装配或开关默认关" |
| TweetMixer / Ads / WTF / Prompts / PushToHome 槽位保留不生效 | ✅ | ForYou 侧 `AdsSource::disabled()`、WTF/Prompts/PushToHome 三个 source 的 `source()` 均返回 `Ok(Vec::new())`；Phoenix 侧未装配 TweetMixer |

### 2.5 候选水合（§3、§5）

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| TES 五件（CoreData / VideoDuration / HasMedia / Language / FilteredTopics） | ✅ | 共享 `TesHydrationProvider` 批处理（`:230–242`） |
| InNetwork 水合 | ✅ | `InNetworkCandidateHydrator` |
| Gizmoduck（选后） | ✅ | 预选在 `features.author_cold_start` 时额外插入（`:243–247`） |
| VF 候选水合（选后） | ✅ | `VFCandidateHydrator`（`:315`） |
| BlockedBy 端口可选 | ✅ | 未装配 |
| Quote / Subscription Hydrator 删除 | ❌ | `candidate_hydrators/quote_hydrator.rs`、`subscription_hydrator.rs` 仍在磁盘且**未加 `#[allow(dead_code)]`**，在 `mod.rs` 注册 |

### 2.6 过滤（§3、§5）

计划 §3 顺序：`DropDup → CoreDataMissing → FirstStageEligible → Age → Self → Seen → SeenBackup → Served → MutedKeyword → AuthorSocialgraph → Video → Topic*`

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| 上述 13 个过滤器的串行顺序 | ✅ | `phoenix_candidate_pipeline.rs:250–264` 与计划逐项一致（Topic* = `TopicIdsFilter` + `NewUserTopicIdsFilter`） |
| `FirstStageEligibleFilter`（U2 新增） | ✅ | `filters/first_stage_eligible_filter.rs`；`None` → 保留，`Some(false)` → Drop |
| `AgeFilter` 改读 `created_at_ms` | ✅ | 缺失且 `timestamp_secs()==0` 时 Drop |
| `VFFilter` + `DedupConversation`（选后） | ✅ | `:319–320` |
| RetweetDedup / IneligibleSubscription / AncillaryVF 删除 | ❌ | 三个文件仍在磁盘（`ancillary_vf_filter.rs` / `ineligible_subscription_filter.rs` / `retweet_deduplication_filter.rs`，前两者加了 `#[allow(dead_code)]`），在 `mod.rs` 注册；仅装配层移除 |

### 2.7 精排 / 选择（§3、§5、§9 P2）

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| `PhoenixScorer` ← `PhoenixPredictionClient` | ✅ | `scorers/phoenix_scorer.rs`；失败/缺序列写 `degraded_reason`（`phoenix_unavailable: …` / `phoenix_missing_sequence`） |
| 元数据 + 响应校验只存在于适配器内，`PhoenixScorer` 只见 `Result` | ✅ | `clients/phoenix_prediction_client.rs:140–296`：`feature-schema` / `model-version` / `random-weights` / `supported-actions` 四项元数据校验 + 单 distribution set / 非空 tweet_id / 无重复 / 无未知候选 / 作者一致 / 19 头 / 2 连续值 / 无 NaN / 无缺失 共 9 类响应校验 |
| `SlimPhoenixPredictionClient` 含 mp-slim 校验逻辑 | ✅ | `:299–329`，双层校验（inner + 外层再校验） |
| `RankingScorer` 权重表调整，不改码 | ✅ | `ranking_scorer.rs:75,86` 读 `p::RETWEET_WEIGHT` / `p::QUOTE_WEIGHT` |
| retweet / quote / quoted_* 权重设 0 | ✅ | `params/param.rs:41,64,66,68` = `0.0`（含注释 `P2/U5`） |
| `RuleFallbackScorer`（U2 新增），装配在 `RankingScorer` 之后，整批替代 + `degraded_reason` | ✅ | `scorers/rule_fallback_scorer.rs`；触发条件为"批次非空且并非全部候选都有可用 Phoenix 头"；`update()` 清空 `phoenix_scores` / `prediction_request_id` / `last_scored_at_ms` / `weighted_score` |
| `VMRanker` + `AuthorColdStart` 开关 | ✅ | 均需 feature/env 且非 Demo 时 AuthorColdStart 被强制关闭（`features_for_mode`） |
| `TopK(50)` | ✅ | `params/config.rs:16 TOP_K_CANDIDATES_TO_SELECT = 50`；`RESULT_SIZE = 35`（上游真值） |
| Blender（ForYou 外壳） | ✅ | `BlenderConfig::default()`：`ads_strategy = Disabled`；prompts/WTF 位置非零但对应 source 返回空，等价直出 |

### 2.8 落库 / 副作用 / 外壳（§3、§5、§7）

| 计划项 | 状态 | 说明 |
| --- | --- | --- |
| `ServedPersist` 成功才响应（服务层同步步骤） | ⚠️ | `scored_posts_server.rs:102–117` 在 `execute()` 之后同步 persist，`persist_error` 非空时 `server.rs:152–156` 返回 `Status::unavailable` ✓。**但 `HomeMixerServer::build` 在非 Demo 模式调用 `without_served_persist()`**（`server.rs:51–55`）→ `served_persist = None` → `persist_error` 恒为 `None` → 非 Demo 下该步骤静默失效 |
| 独立 `ServedPersistence` 端口 + 内存实现仅 Demo/测试 | ✅ | `clients/served_persistence.rs`；trait 文档明确"生产 adapter 必须持久且幂等" |
| `InMemoryFeedStateStore` 记 served | ✅ | 与 ServedPersistence 共用同一 bounded store |
| `ResponseStats` 保留 | ⚠️ | `ResponseStatsSideEffect` 只装配在 **ForYou** 管线（`for_you_candidate_pipeline.rs:62`），Phoenix 管线侧仅有 `PhoenixRequestCacheSideEffect`。计划 §3 把 ResponseStats 画在 Phoenix 外层 |
| RequestCache / Kafka seen / served 端口保留不装配 | ✅ | `feature_policy.rs` 中 `request_cache_side_effect` 默认 false；Kafka side effect 文件存在但未装配 |
| `feedback` RPC（独立 RPC 或 HTTP 薄层 → 业务适配器） | ❌ | 全仓库无 `feedback` 标识符（`grep -rn feedback home-mixer` 零命中） |
| gRPC `ScoredPostsService` / `ForYouFeedService` | ✅ | `server.rs` 注册两服务 + reflection + gzip/zstd 压缩；`DebugScoredPosts` 受 `debug_access` 门禁 |
| HTTP `/v1/feed` 薄层、cursor 会话 | ✅（可选） | 均未实现，属计划"可选"列 |
| Bearer + `X-Viewer-Account-Id` 双校验 | ❌ | 无 tonic interceptor、无 HTTP 薄层、无 metadata 鉴权代码；`main.rs` 无任何拦截器 |
| 请求级 deadline 下传 | ❌ | 仅有各适配器内部固定超时（`*_TIMEOUT_MS`，如 `PHOENIX_PREDICTION_TIMEOUT_MS=5000`）与 `predict_with_timeout` 单点包装；未从 gRPC 请求 deadline 向上下游传播 |
| `PHOENIX_ENGINE=slim\|xrex` 装配层引擎选择 | ✅ | `phoenix_prediction_client.rs:332–354`：`slim` 通过，`xrex` 与未知值显式 `bail!` |
| 独立 crate `thunder` / `vm-ranker` 留 workspace 不部署 | ✅ | `Cargo.toml` members 含二者；`phoenix` 独立 workspace 并 `exclude` |

### 2.9 U1 业务适配器清单（§5 P2）

计划要求实现 8 个适配器。**实测结果：`grep -rn "struct Mrpyq\|Mrpyq.*Client\|Mrpyq.*Fetcher" home-mixer` 仅命中 `mrpyq_recommendation_data_client.rs` 自身的 `MrpyqRecommendationDataConfig` / `MrpyqClientError` / `MrpyqRecommendationDataClient`（trait）/ `GrpcMrpyqRecommendationDataClient` / `DisabledMrpyqRecommendationDataClient`。**

| 计划适配器 | 状态 |
| --- | --- |
| `MrpyqTESClient` | ❌ 未实现（仅 `DemoTESClient` / `DisabledTESClient`） |
| `MrpyqStratoClient` | ❌ 未实现（仅 `DemoStratoClient` / `DisabledStratoClient`） |
| `MrpyqUasFetcher` | ❌ 未实现（仅 `DemoUserActionSequenceFetcher` / `DisabledUserActionSequenceFetcher`） |
| `MrpyqVisibilityFilteringClient` | ❌ 未实现（仅 `DemoVisibilityFilteringClient` / `DisabledVisibilityFilteringClient`） |
| `MrpyqGizmoduckClient` | ❌ 未实现（仅 `DemoGizmoduckClient` / `DisabledGizmoduckClient`） |
| `MrpyqInNetworkPostsClient` | ❌ 未实现（仅 `DisabledInNetworkPostsClient` + feature-gated `ThunderClient` 实现） |
| `MrpyqImpressedPostsClient` | ❌ 未实现（仅 trait） |
| served / feedback 后端客户端 | ❌ 未实现（仅 `InMemoryServedPersistence`） |
| `SlimPhoenixPredictionClient`（含 mp-slim 校验逻辑） | ✅ 唯一落地项 |

**这是 P2 与计划最大的差距**：`PhoenixDependencies` 的 8 个端口全部只有 Demo / Disabled 实现，真实业务数据无法流过流水线框架——即计划 §2.2 要解决的核心问题（"让业务数据流过流水线框架"）尚未闭环。

---

## 3. 未实现项汇总（按计划条目）

| # | 计划条目 | 计划位置 | 影响 |
| --- | --- | --- | --- |
| 1 | 8 个 U1 业务适配器 | §5、§9 P2 | 阻塞真实业务接入；主干仍只能跑 Demo 数据 |
| 2 | feedback RPC | §5、§7、§9 P2 | 反馈无法落库，训练归因缺一半输入 |
| 3 | Bearer + `X-Viewer-Account-Id` 双校验 | §7 | 无调用方身份校验；`production_ready` 拒绝文案已把它列为未验证项 |
| 4 | 请求级 deadline 下传 | §7 | 上游慢请求无法通过 deadline 传导；仅有固定超时兜底 |
| 5 | `ImpressedPostsQueryHydrator` 装配 | §3、§5 | 服务端曝光存储不可用，只能用请求体 `impressed_post_ids` |
| 6 | 真实 `ServedPersistence` 适配器 | §5、§7 | 非 Demo 无 served 归因（见风险 R1） |
| 7 | U5 六个专用文件物理删除 | §9 P2 | 计划要求"物理删除"，实际仅从装配移除；文件继续参与编译 |
| 8 | Thunder / VM Ranker string proto（P3） | §6.4、§9 P3 | 已由 `legacy-int-ids` 隔离，明确"不部署"；属计划内延期 |
| 9 | cursor 会话分页 | §7、§10 | 计划明确"不搬"；非缺陷 |

---

## 4. 偏差项汇总

| # | 偏差 | 计划要求 | 实际 | 评价 |
| --- | --- | --- | --- | --- |
| D1 | **P1–P3 未提交** | §9 按批次交付；§11 要求 ID 替换单独 PR | HEAD 仅 P0；167 个文件未提交 | **严重**，交付流程缺陷 |
| D2 | §13 测试数字与实测不符 | §13 记录 382 / 305 / 304 | 实测 377 / 297 | 记录失真；疑因工作树持续改动 |
| D3 | `ObjectId::from_u64_be_padded` / `to_u64_be_padded` 为生产可见 API | §6.2"生产禁止从整数构造"，仅 `#[cfg(test)] From<u64>` | 两个 `pub fn` 在非 test 编译下可用 | 为 `legacy-int-ids` 妥协；P3 应随整数 proto 一并清理 |
| D4 | `demo.rs` 的 `snowflake_id` 未移除 | §6.4 要求改为 `demo_object_id` / `from_parts` | `snowflake_id` 保留，被 `thunder/demo_seed.rs` 使用；`DEMO_AUTHOR_IDS` 仍为 `[i64;5]` | 与 Thunder 整数 proto 延期一致，但 §6.4 只完成一半 |
| D5 | `CachedPostsSource` 无条件装配 | §3"保留形态，不生效"；§5 列为"不装配或开关默认关" | 常驻装配，`enable() = has_cached_posts` | 由 `unsigned_cached_posts` feature（默认拒绝）兜底，风险可控但语义与计划不符 |
| D6 | `ResponseStats` 落在 ForYou 管线 | §3 画在 Phoenix 外层 | `ResponseStatsSideEffect` 仅在 `ForYouCandidatePipeline` | 功能存在、位置不同；Phoenix 管线的可观测性依赖 `scored_posts_server.rs` 的日志 |
| D7 | `recsys_compat` / `uas_compat` 仍为 stub | §8.3"历史序列按上游 7 天 / 300 条聚合" | 窗口常量 ✓（7 天 / 300），但 `DefaultAggregator` 直通、`DenseAggregatedActionFilter` 与 `KeepOriginalUserActionFilter` 均为 no-op | 聚合语义缺失；对模型效果是实质风险，文档已注明 |
| D8 | 三个 U5 过滤器 / 两个 U5 水合器 / 一个 U5 QH 保留在 `mod.rs` | §9 P2"删 …" | 全部仍在磁盘并注册 | 见未实现项 #7 |

---

## 5. 潜在问题与风险

**R1（高）非 Demo 模式 served 落库静默失效。**
`HomeMixerServer::build` 对 `Degraded` / `ProductionReady` 调用 `without_served_persist()`，使 `persist_error` 恒为 `None`，`ScoredPostsServer` 永远不返回 `Unavailable`。计划 §3 的"served 落库成功才响应"与 §7 的"served 成功才 2xx"在非 Demo 下形同虚设，训练归因链断裂且无任何告警。
*建议*：非 Demo 且未注入真实 `ServedPersistence` 时应显式拒绝启动（或对每次请求返回 `Unavailable`），而不是静默降级为 no-op；至少在启动日志中 fail-loud。

**R2（高）`FirstStageEligibleFilter` 是 fail-open，与 §7 的 fail-closed 纪律不一致。**
`recommendation_eligible == None` → 保留。VF 路径是 fail-closed（缺项 → Drop、超时 → Unavailable），首筛却是 fail-open。生产 TES 适配器一旦漏填该字段，过滤器完全失效且无计数/告警。
*建议*：非 Demo 下把 `None` 视为"契约未验证"，或在适配器层对缺失计数并按阈值拒绝请求。

**R3（中）`AgeFilter` 对非时间编码 ObjectId 的回退无意义。**
§6.2 假设 `[u8;12]` 字典序即时间序，但 Phoenix 演示网关的召回 ID 是 `md5("phoenix-demo-post-{i}")[:12]`，其前 4 字节是随机值。`AgeFilter` 回退到 `timestamp_secs()` 时，随机值可能落在未来（age=0 → 保留）或远古（→ 丢弃），行为不确定。
*建议*：确保所有真实召回/水合路径都填 `created_at_ms`；对缺失该字段的候选在非 Demo 下显式告警或 Drop，而不是静默走 ID 回退。

**R4（中）`RuleFallbackScorer` 的"部分缺失即整批降级"可能过度降级。**
触发条件是"并非全部候选都有可用 Phoenix 头"。若 Phoenix 因截断/部分超时只返回部分候选的预测，整批个性化分数会被规则分覆盖，损失远大于必要。
*建议*：区分"整批为空/全部失败"（整批替代）与"部分缺失"（仅对缺失候选回退或标记），并补一条部分缺失场景的测试。

**R5（中）`legacy-int-ids` 默认开启且静默丢弃真实 ID。**
`default = ["legacy-int-ids"]` 使默认构建包含 Thunder/VM Ranker 整数适配器；`thunder_u64()` 对真实 96-bit ObjectId 返回 `None`，`thunder_request` 用 `.unwrap_or(0)` 把 user_id 降级为 0，`filter_map` 静默丢弃无法 round-trip 的 ID。
*建议*：默认关闭该 feature，或在装配真实 ID 时启动即 fail；至少对丢弃计数并 warn。

**R6（中）U5 组件保留在编译单元内。**
`quote_hydrator.rs` / `subscription_hydrator.rs` / `subscribed_user_ids_query_hydrator.rs` / `retweet_deduplication_filter.rs` / `ineligible_subscription_filter.rs` / `ancillary_vf_filter.rs` 仍在 `mod.rs` 注册。计划 §9 要求物理删除，§4 U5 规则也写"物理删除专用组件文件并从装配移除"。保留会持续产生上游同步时的合并噪音，也削弱 U5 分类的可执行性。
*建议*：按计划物理删除，或把 U5 规则改为"从装配移除 + `#[cfg(feature = "u5-retained")]` 门禁"，并在维护文档中明确取舍。

**R7（已复核，非问题）`u64` 残留符合 §6.4 逐类去向。**
实测 `home-mixer/` 内 `u64` 出现 170 处（`mp` 基线为 356 处，降幅约 52%），且 **ID 位置零残留**：`grep -E "(Vec|HashMap|HashSet|BTreeMap|BTreeSet|Option)<(u64|i64)>"` 在 `models/` 内无任何 ID 容器命中。剩余 170 处逐类核对结果：

| 类别 | 实例 | 计划要求 | 结论 |
| --- | --- | --- | --- |
| 时间戳 / 时间窗口 | `created_at_ms`、`last_scored_at_ms`、`past_request_timestamps_ms`、`action_time_ms`、`impressed_time_ms`、`last_modified_epoch_ms`、`*_TIMEOUT_MS`、`MAX_POST_AGE` | 保留 u64 | ✅ |
| 计数 / 阈值 / 上限 | `view_count`、`favorite_count`、`reply_count`、`repost_count`、`quote_count`、`follower_count`、`COLD_START_*` | 保留 u64 | ✅ |
| 本地相关 ID | `prediction_id`、`prediction_request_id`、`request_time_ms` | 保留 u64 | ✅ |
| 话题 ID | `topic_ids`、`excluded_topic_ids`、`new_user_topic_ids`、`supplemental_topic_ids`、`retrieval_topic_ids`、`filtered_topic_ids`、`unfiltered_topic_ids` | 保留上游形态（§3"Topics 保留形态"） | ✅ |
| `ids.rs` 内部算术 | `from_parts(seq: u64)`、`to_u64_hash`、零填充 helper | 哈希算术保留 | ✅ |

Snowflake 推时间已收敛到 `proto/src/demo.rs`（仅 `thunder/demo_seed.rs` 消费，随整数 proto 延期）；`home-mixer/util/snowflake.rs` 已删除。

**R8（低）`phoenix_demo_post_id` 与 `demo_object_id` 是两份实现。**
`proto/src/demo.rs:24` 用 Rust `md5` 复刻 `phoenix/services/grpc_gateway.py:89 demo_object_id`，靠单测 `phoenix_demo_post_id(0) == "4ed94de62235affc0e1b5a2e"` 锁定一致。跨语言复刻 + 单点锁定，长期有漂移风险（与 `to_u64_hash` 用共享黄金向量文件的做法相比更脆弱）。
*建议*：把 demo ID 也纳入 `testdata/` 共享向量，或由 Python 侧生成后落盘供 Rust 读取。

**R9（低）`FallbackSource` 复用 `ServedType::ForYouPhoenixRetrieval`，兜底候选不可区分。**
`fallback_source.rs:23` 把兜底候选标为 `ForYouPhoenixRetrieval`，`in_network = false`。`home_mixer.proto` 的 `ServedType` 枚举没有兜底专用值，因此兜底流量在响应、埋点与观测中与 Phoenix 真实召回完全混同。
实测证据：`run_demo.sh` 输出 35 条中，第 18、25 条 ID 为 `6aa55e93 000000000089544e` / `6aa55cb3 0000000000895456`，即 `DemoFallbackPostsClient` 的 `from_parts(ts, 9_000_000 + index)` 编码（index 14 / 22），但输出标签显示为"Phoenix 网外"。
*建议*：在 `ServedType` 增加兜底专用枚举值（或至少用 `served_type` 之外的独立标记字段），否则无法从线上数据区分兜底占比与 Phoenix 召回质量。

---

## 6. 改进建议（按优先级）

**P0 — 立即**
1. 把 P1 / P2 / P3 分三次提交落库，包含全部 untracked 文件（`ids.rs`、`in_network_posts_client.rs`、`served_persistence.rs`、`first_stage_eligible_filter.rs`、`rule_fallback_scorer.rs`、`fallback_source.rs`、`p2_pipeline_assembly.rs`、`test_object_id_hash.py`、`testdata/`、四份能力清单）。
2. 修正 §13 执行记录中的测试数字（382 → 377、305/304 → 297），或在每次提交时自动生成计数。

**P1 — 接入前必须**
3. 实现 `ServedPersistence` 的真实适配器；在此之前把非 Demo 模式的 served 落库从"静默 no-op"改为"显式拒绝/告警"。
4. 让 `FirstStageEligibleFilter` 在非 Demo 下对 `recommendation_eligible == None` fail-closed，或至少在适配器层计数告警。
5. 实现 8 个 U1 业务适配器（先做 `MrpyqInNetworkPostsClient` + `MrpyqTESClient` + `MrpyqUasFetcher` 打通主链），并把 `mrpyq_recommendation_data_client` 接到 `InNetworkPostsClient` / `FallbackSource` / `TESClient` 端口上，消除孤立代码。
6. 实现 feedback RPC 与幂等落库。

**P2 — 契约与纪律**
7. 物理删除 6 个 U5 专用组件，或改为 feature 门禁并在维护文档写明。
8. 补 Bearer + `X-Viewer-Account-Id` 双校验（tonic interceptor 或薄 HTTP 层）与请求级 deadline 下传。
9. 装配 `ImpressedPostsQueryHydrator`，并明确服务端曝光存储与请求体 `impressed_post_ids` 的优先级。
10. 收敛 `from_u64_be_padded` / `to_u64_be_padded` 到 feature 门禁后，P3 改 string proto 时删除。
11. 为 `RuleFallbackScorer` 增加"部分缺失"与"整批失败"的区分与测试。
12. 补齐 `recsys_compat` 的聚合语义（按帖分组、时间窗口校验、机器人过滤），或明确标注为已知效果上限。

**P3 — 文档与同步**
13. 把 `demo_object_id` 纳入共享黄金向量，消除跨语言复刻。
14. 在 `AgeFilter` 对 `created_at_ms` 缺失的路径上增加非 Demo 告警。

---

## 7. 验收项复核（实测）

| 验收项 | 计划要求 | 实测 | 结论 |
| --- | --- | --- | --- |
| `cargo test --workspace` | 354 项通过（P0） | **377 passed / 0 failed** | ✅ 超基线 |
| 明细 | — | home-mixer lib 304 + p2_pipeline_assembly 4 + p4_final_feed 25 + user_topic_reader_contract 2 + thunder 5 + proto 4 + vm-ranker 12 + candidate-pipeline 21 | — |
| `cargo test -p home-mixer --no-default-features --lib` | P1 记录 305；P2 记录 304 | **297 passed** | ⚠️ 与记录不符 |
| `cargo clippy --workspace --all-targets -- -D warnings` | P2 要求 0 warning | **0 warning / 0 error** | ✅ |
| `cd phoenix && uv run pytest -q` | 通过 | **115 passed（24.2s）** | ✅ |
| `./scripts/run_demo.sh` | 可跑 | **通过（16s）**：返回 **35 条**（网内 3 + 网外 32），覆盖 Thunder 网内 / Phoenix 网外 / Phoenix 话题 / 兜底四类来源；Phoenix 网外召回已恢复（P0 阶段该路径返回 0 条）；脚本 trap 清理后进程已释放 | ✅ 已独立验证 |
| P3 四份能力清单 | 四份新清单，锚点前移 `6bb4594` | `49815da` / `75d93d9` / `fee1d0f` / `6bb4594` 四份齐备，逐项给出 U0/U1/U2/U3/U5 分类与处置证据；U3 未吸收项（StableHLO bundle、`return_logits_list`、生产 proto 枚举、Kafka/GPU 数据面、`seed_tweet_id`）均显式记录并给出重入条件；`upstream-first-maintenance.md` 锚点已前移 | ✅ |
| `TEMP(U4-P1)` 桥接标记清零 | P1 要求 | 全仓库零命中 | ✅ |
| 真实业务 fixture 端到端 | P2 要求 | 未实现（无业务适配器） | ❌ |

---

## 8. 审查边界说明

- 本报告只审查**代码与计划的一致性**，不评估业务效果、线上性能与容量。
- §6.4 的 u64 去向已按类别复核（见 R7），未逐行比对 356 处的原始清单。
- `run_demo.sh` 已复跑并通过（见 §7）；未验证并发、容量与多副本行为。
- `phoenix/` 内部（xrex / crates / 训练脚本）仅在 §6.3 派生函数与 §2.3 保留项范围内抽查；P3 的 xrex 吸收细节按四份能力清单的自我声明采信，未逐文件比对上游 diff（`phoenix/xrex` 有 30 个文件已暂存未提交，属 GPU/Kafka 路径，macOS 无法运行时验证）。
- 审查时工作树含 167 项未提交改动，结论对应**该工作树快照**，不代表任何已提交版本。
