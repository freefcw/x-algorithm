# 主干收敛方案实现审查报告

> **审查对象**：`docs/implementation/phoenix-pipeline-trunk-plan.md`（基线版，状态 `decision`，路径 `/Users/hejun/work/mp/x-algorithm/docs/implementation/phoenix-pipeline-trunk-plan.md`）与 `mp-trunk` 工作树实现
> **审查日期**：2026-09-12
> **审查方式**：逐条比对计划 §1–§12 的模块、接口、数据流与代码实体；实跑 `cargo test` / `cargo clippy` / `pytest` 复核验收项
> **审查范围**：`home-mixer/`、`candidate-pipeline/`、`proto/`、`thunder/`、`vm-ranker/`、`phoenix/`、`docs/upstream-sync/`、`testdata/`
>
> **落库说明（2026-09-13）**：§0–§8 是**提交前工作树**的审查快照，作为历史记录原样保留，正文结论不回写。随后历史提交为 `2725693`（P3 上游可移植代码）、`fa83cbf`（P1+P2 身份与主干契约）、`9ea77f6`（docs 同步）；本轮拆分提交见 §9.1。
>
> ⚠️ **快照中的部分结论已随落库失效**——特别是 §0 的"P1–P3 未提交"与"U1 业务适配器清单全部缺失"、§1、D1、D8/R6、R1 与 §7 的测试计数。**当前状态以 §9「落库后复审」为准**；mrpyq 适配器一线另见 §11（专项审查快照）与 §12（修复执行记录，冲突以其为准）。

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

**本段是 2026-09-12 的历史快照：当时 HEAD = `b573d67 docs: 记录 P0 执行结果`，P1（ID 替换）、P2（契约落位 + U5 卸装）、P3（追上游）的全部实现均未提交。**

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

> ⚠️ **本小节是 §0–§8 里唯一被回写的一处**（2026-09-14）。原因：这张表被后续章节反复引用，留着"全部 ❌"会持续误导。§0–§8 其余部分仍是提交前快照，不回写。原始审查结论见本节末尾。

计划要求实现 8 个适配器。下表状态复核于 2026-09-14 晚，**基于当时的工作树未提交改动**（`clients/mrpyq_adapters.rs` / `clients/mrpyq_viewer_relation_client.rs`）；按当时已提交的 HEAD `9ea77f6` 看仍是 0/8。

| 计划适配器 | 状态 |
| --- | --- |
| `MrpyqTESClient` | ✅ 已实现（`mrpyq_adapters.rs:378`，经 `ContentCache` 与 VF 共享一次水合） |
| `MrpyqInNetworkPostsClient` | ✅ 已实现（`:215`，同时承载 `FallbackSource` 的 `get_fallback_posts`） |
| `MrpyqVisibilityFilteringClient` | ⚠️ **部分**：实现为 `MrpyqFirstStageEligibilityClient`（`:436`），只承载帖子维度的一级 `recommendation_eligible`，**不做 viewer 级判定**（见 M1 / §12.2） |
| `MrpyqStratoClient` | ⚠️ **适配器就位、后端未实现**：`:478` + `proto/definitions/viewer_relation.proto`，但 mrpyq 侧对 `ViewerRelation` 零命中，该 RPC 还没写，端口实际仍是 Disabled（见 §12.2 残留 ①） |
| `MrpyqUasFetcher` | ❌ 未实现（仅 `DemoUserActionSequenceFetcher` / `DisabledUserActionSequenceFetcher`） |
| `MrpyqGizmoduckClient` | ❌ 未实现（仅 `DemoGizmoduckClient` / `DisabledGizmoduckClient`） |
| `MrpyqImpressedPostsClient` | ❌ 未实现（仅 trait，唯一实现是测试 fake） |
| served / feedback 后端客户端 | ❌ 未实现（仅 `InMemoryServedPersistence`） |
| `SlimPhoenixPredictionClient`（含 mp-slim 校验逻辑） | ✅ 已实现 |

**当前进度 5/9**（VF 记为部分、Strato 记为等后端）。计划 §2.2 的核心问题——"让业务数据流过流水线框架"——在 TES / 网内 / 兜底三条主链上已经闭环；剩下的缺口集中在 **viewer 维度（Strato 等后端）** 和 **训练归因侧（UAS / impressed / served / feedback）**。

> **原始结论（2026-09-12，对应提交前快照）**：8 个适配器全部 ❌ 未实现，`grep` 仅命中 `mrpyq_recommendation_data_client.rs` 自身的类型；`SlimPhoenixPredictionClient` 是唯一落地项。当时判定"这是 P2 与计划最大的差距"。下文 §3 #1、§6 #5、§7、§9.2、§9.3 仍按这一结论书写，未回写。

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
- `phoenix/` 内部（xrex / crates / 训练脚本）仅在 §6.3 派生函数与 §2.3 保留项范围内抽查；P3 的 xrex 吸收细节按四份能力清单的自我声明采信，未逐文件比对上游 diff（`phoenix/xrex` 有 30 个文件已暂存未提交，属 GPU/Kafka 路径，macOS 无法运行时验证）。→ **该缺口已在 §10 补审关闭**（上游 `49815da`/`75d93d9`/`fee1d0f`/`6bb4594` 提交在本地可达，已逐项比对）。
- 审查时工作树含 167 项未提交改动，结论对应**该工作树快照**，不代表任何已提交版本。

---

## 9. 落库后复审（代码基线 = `bc5f1d4`）

> 本节先在历史三次提交落库后形成，随后以 `bc5f1d4` 为代码基线完成本轮装配、VM Ranker、VF 和运行文档拆分。§0–§8 与 §11 仍保留为历史快照；与本节当前值冲突的，以本节为准。

### 9.1 交付状态：已落库

| 提交 | 内容 | 规模 |
| --- | --- | --- |
| `2725693` | phoenix：吸收上游 `49815da`–`6bb4594` 可移植变更 | 37 files, +9200 / −866 |
| `fa83cbf` | home-mixer：流水线身份改为 `ObjectId`，落下主干契约与 U5 卸装 | 126 files, +4042 / −2677 |
| `9ea77f6` | docs：同步主干收敛执行记录并去掉 BusinessFeed 文档面 | 19 files, +377 / −178 |
| `ab0f276` | home-mixer：接入 mrpyq 非 demo 装配 | 6 files, +229 / −110 |
| `20ae972` | home-mixer：加固 VM Ranker ObjectId 边界 | 2 files, +158 / −93 |
| `f3ab2f4` | home-mixer：VF 默认 fail-closed | 3 files, +82 / −24 |
| `bc5f1d4` | docs：同步运行时契约与召回上限 | 5 files, +10 / −9 |
| **历史合计** `b573d67..9ea77f6` | — | **181 files, +13619 / −3721** |

本轮代码与运行文档已拆为上述四个提交；审查记录和 mrpyq 契约文档随后独立提交。**§1 的"P1–P3 全部未提交"与偏差 D1 已解除。**

### 9.2 旧结论翻转表

| 原编号 | 原结论 | 复审结果 | 证据 |
| --- | --- | --- | --- |
| §1 / D1 | P1–P3 全部未提交，HEAD 仅到 P0 | **已解除** | 三次提交；仅 `.workbuddy-ai/` 未跟踪 |
| §2.3 | SubscribedUserIds QH 保留在磁盘 | **已删除** | 文件不存在，`query_hydrators/mod.rs` 无注册 |
| §2.5 | Quote / Subscription Hydrator 保留 | **已删除** | 两文件均不存在 |
| §2.6 | RetweetDedup / IneligibleSubscription / AncillaryVF 保留 | **已删除** | 三文件均不存在 |
| D8 / R6 | U5 六个专用组件参与编译 | **已物理删除** | 六条路径全部 `No such file` |
| R1 | 非 Demo 模式 served 落库静默 no-op | **已修复** | `without_served_persist()` 全仓库**零命中**；`scored_posts_server.rs:27` 改为非可选 `served_persist: Arc<dyn ServedPersistence>`，`with_served_persist()` 直接赋值（`:66–67`） |
| §7 | 377 / 297 | **391 / 307** | 见 9.5 |
| §2.9 / §3 #1 | U1 业务适配器 0/8，"P2 与计划最大的差距" | **部分解除（工作树，未提交）** | TES / 网内 / 兜底 / VF（仅一级位）四个端口已挂真实后端，Strato 适配器就位等后端；进度 5/9。§2.9 已回写，§3 #1、§6 #5、§7、§9.3 未回写 |

### 9.3 新增机制：VF 失败策略参数化

`home-mixer/feature_policy.rs` 新增：

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VfFailurePolicy { #[default] FailClosed, InNetworkOnly, AllowAll }
```

- 由环境变量 `HOME_MIXER_VF_FAILURE_POLICY` 驱动（`fail_closed` 默认；`in_network_only` 和显式 `allow_all` 可选；非法值告警后回落 `FailClosed`），`HomeMixerFeatures.vf_failure_policy` 字段承载。
- 装配于 `phoenix_candidate_pipeline.rs:320` → `VFFilter::new(features.vf_failure_policy)`。
- `filters/vf_filter.rs:37–46`：`Unchecked | Unavailable` 时，**仅当**策略为 `InNetworkOnly` 且 `in_network != Some(true)` 才 Drop。

**评价**：计划 §7 的 fail-closed 已落地为默认 `FailClosed`；`in_network_only` 和显式 `allow_all` 保留为运行时策略，其中 `allow_all` 在非 demo 模式会产生启动告警。

### 9.4 复审新增发现

**N1（中）训练与服务帖龄窗口不一致，且代码注释自己承认需对齐。**
- `phoenix/data_preprocessor.py:49`：`MAX_AGE_DAYS = 7  # 负样本候选的最大帖龄（生产取值需与 Home Mixer AgeFilter 对齐）`
- `home-mixer/params/config.rs:25`：`pub const MAX_POST_AGE: u64 = 48 * 60 * 60;`（2 天）

训练负样本池覆盖 7 天，线上 `AgeFilter` 只放 2 天，训练/服务分布不一致；注释已把"对齐"列为待办却未落地。
*建议*：把该值收敛到单一来源（Python 读 Rust 常量，或落一份 `testdata/` 对齐文件），并在 §10 待确认清单显式登记。

**N2（中）`eval_ranker.py` 的评估指标与计划 §8.4 不符。**
- 计划 §8.4 明确："`eval_ranker.py` 同一评估集对比 **AUC / NDCG** 后决定切流"。
- 实测 `phoenix/scripts/eval_ranker.py`：输出 `hr@1` / `mrr`（`:235–240`），另有 `random_hr@1` 与 `baseline_scores()`（规则近似）作对照；**全脚本无 AUC / NDCG**。

影响：切换 xrex 时缺少计划指定的决策指标。
*建议*：补齐 AUC / NDCG，或修订 §8.4 把判据改为 HR@1 / MRR 并说明理由。

**N3**：已由 `f3ab2f4` 解决，默认策略现为 `FailClosed`。

### 9.5 验收项复测（代码基线 = `bc5f1d4`）

| 验收项 | 计划 §13 记录 | 本次实测 | 结论 |
| --- | --- | --- | --- |
| `cargo test --workspace` | P1 记录 **382** | **426 passed / 0 failed** | ⚠️ 历史记录滞后 |
| 明细 | — | home-mixer lib 314 + `p2_pipeline_assembly` 8 + `p4_final_feed` 25 + `user_topic_reader_contract` 2 + thunder 5 + proto 4 + vm-ranker 12 + candidate-pipeline 21 | — |
| `cargo test -p home-mixer --no-default-features` | P1 记录 **304** | **373 passed** | ⚠️ 历史记录滞后 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 | **0 warning / 0 error** | ✅ |
| `cd phoenix && uv run pytest -q` | 115 | **115 passed（23.6s）** | ✅ |
| `./scripts/run_demo.sh` | 通过，35 条 | **通过（10s）：35 条（网内 5 + 网外 30）** | ✅ |

**§13 的 P0 记录表同样滞后**（P0 表 vs 实测）：home-mixer 320 → **349**（= 314+8+25+2）；vm-ranker 10 → **12**；proto 2 → **4**；candidate-pipeline 21 / thunder 5 不变。

*建议*：§13 的计数改为每次提交时自动生成，或标注"截至 `<commit>`"，否则会持续失真。

### 9.6 复审后仍成立的原结论

以下条目在落库后逐条复核，**结论不变**：

| 编号 | 结论 | 复核证据 |
| --- | --- | --- |
| 未实现 #1 | U1 业务适配器 0/8 | `Mrpyq` 仅出现在 `mrpyq_recommendation_data_client.rs` 自身；`clients/` 内实现仍为 Demo / Disabled / Prod / Grpc 四类 |
| 未实现 #2 | feedback RPC 缺失 | `feedback` 在 `home-mixer/` 零命中 |
| 未实现 #3 | Bearer + `X-Viewer-Account-Id` 双校验缺失 | `Bearer` / `X-Viewer-Account-Id` / `interceptor` 均零命中 |
| 未实现 #4 | 请求级 deadline 下传缺失 | 仍只有适配器内固定超时 |
| 未实现 #5 | `ImpressedPostsQueryHydrator` 未装配 | `build_with_clients` 的 `query_hydrators` 列表（`:181–202`）不含它 |
| 未实现 #6 | 真实 `ServedPersistence` 适配器缺失 | 仅 `InMemoryServedPersistence`（端口已改非可选，见 9.2） |
| D3 | `from_u64_be_padded` / `to_u64_be_padded` 生产可见 | `ids.rs:112,120` 仍为 `pub fn`；被 `vm_ranker_client` / `in_network_posts_client` / `uas_fetcher` / `gizmoduck_client` / `tweet_entity_service_client` 等 11 处调用 |
| D4 | `snowflake_id` 未移除 | `proto/src/demo.rs:41`；`thunder/demo_seed.rs:13,33` 消费 |
| D5 | `CachedPostsSource` 无条件装配 | `phoenix_candidate_pipeline.rs:226` 无条件 `push` |
| D6 | `ResponseStatsSideEffect` 只在 ForYou | `for_you_candidate_pipeline.rs:62`；Phoenix 侧 `side_effects` 只有 `PhoenixRequestCacheSideEffect`（`:326`） |
| D7 | `recsys_compat` 聚合仍 no-op | 窗口常量已在位（`config.rs:61 UAS_WINDOW_TIME_MS = 7d`、`:65 UAS_MAX_SEQUENCE_LENGTH = 300`），但聚合器仍直通 |
| R2 | `FirstStageEligibleFilter` fail-open | `first_stage_eligible_filter.rs:19` 判据 `!= Some(false)`；文档注释显式承认该取舍 |
| R3 | `AgeFilter` 对非时间编码 ID 的回退不确定 | 未变 |
| R4 | `RuleFallbackScorer` 部分缺失即整批降级 | 未变 |
| R5 | `legacy-int-ids` 默认开启 | `default = ["legacy-int-ids"]` |
| R8 | demo ID 跨语言双实现 | `proto/src/demo.rs` 与 `phoenix/services/grpc_gateway.py` 各一份 |
| R9 | `FallbackSource` 复用 `ForYouPhoenixRetrieval` | `fallback_source.rs:23`；实测 demo 输出第 33 行 `6aa5876a0000000000895440` 为 `from_parts(ts, 9_000_000+idx)` 编码却标注"Phoenix 网外" |

### 9.7 复审后的优先级

- **已消项**：D1（提交）、R1（served 落库 no-op）、R6/D8（U5 物理删除）。
- **P0（接入前必做）**：U1 业务适配器（至少 `MrpyqInNetworkPostsClient` + `MrpyqTESClient` + `MrpyqUasFetcher`）；真实 `ServedPersistence` 适配器；feedback RPC；`production_ready` 增加"VF 策略必须 `in_network_only`"闸门。
- **P1（纪律）**：`FirstStageEligibleFilter` 非 Demo fail-closed；`legacy-int-ids` 默认关闭或启动即 fail；Bearer / viewer 双校验 + 请求级 deadline 下传。
- **P2（一致性）**：训练/服务帖龄窗口对齐（N1）；`eval_ranker.py` 补 AUC / NDCG（N2）；`from_u64_be_padded` 收敛到 feature 门禁；§13 计数自动生成。
- **P3（文档）**：`demo_object_id` 纳入共享黄金向量；`ServedType` 增加兜底专用枚举值。

---

## 10. 边界补审：P3 上游吸收与训练侧契约

> 关闭 §8 遗留的两处边界：① P3 四份能力清单的吸收声明只按自我声明采信；② `phoenix/` 训练侧契约未逐项验证。
> 上游四个提交（`49815da` / `75d93d9` / `fee1d0f` / `6bb4594`）在本地仓库均 `git cat-file -t` 可达，因此本次**逐项比对实际代码与上游 diff**，不再采信清单自述。

### 10.1 P3 吸收声明逐项复核（全部属实）

| 清单 | 声明 | 复核结果 | 证据 |
| --- | --- | --- | --- |
| `49815da` | FA4 CuTeDSL kernel 已移植 | ✅ | `phoenix/xrex/cutedsl/ranker_attention_fa4.py`、`ranker_attention_varlen_fa4.py`、`ranker_fa4/{ampere_helpers,flash_fwd,flash_fwd_sm90,flash_bwd,flash_bwd_preprocess,flash_bwd_sm90,block_sparse_utils,flash_bwd_postprocess,mask}.py` 均在位 |
| `49815da` | `InputBuffer: Default` | ✅ | `phoenix/crates/common/xai-recsys/src/util.rs:282–283` `#[derive(Default)]` + `pub struct InputBuffer` |
| `49815da` | copy-port 多前缀 checkpoint / bundle manifest | ✅ | `xai-recsys-engine/src/copy_port_client.rs`：`new_prefix`/`prev_prefix` 单调校验、`checkpoint_prefix_from_path`、`elapsed_samples_` 解析、`newest_full_prefix` |
| `49815da` | 两份 proto 的 `ContentFeatures` | ✅ | `phoenix/crates/serving/xai-recsys-proto/proto/recsys.proto` 含 `ContentFeatures` |
| `49815da` | configlib class reference 反序列化 | ✅ | `xai_configlib/__init__.py`：`_config_class_key`、`to_dict` 写 `result["__class"]`、`from_dict` 反查 |
| `49815da` | `recsys_bundle_export.py` 保持 U3 删除 | ✅ | 文件不存在 |
| `75d93d9` | `xrecsys.py` 删过时配置 | ✅ | 上游 `75d93d9` 删除 `"multimodal_embedding_type": "v5"`；本地该行已不存在 |
| `75d93d9` | `model_runner.py` NUMA `schedulable_cpus` | ✅ | `phoenix/xrex/inference/model_runner.py:80` import、`:2763` `os.sched_setaffinity(0, schedulable_cpus(*nodes))` |
| `75d93d9` | `recsys_model.py` `enable_day_of_week` | ✅ | `:287` 字段、`:1982` 使用点 |
| `fee1d0f` | two-tower `PADDING_SEGMENT_ID` + `padding_mask` | ✅ | `recsys_two_tower_model.py:60` import、`:81` `jnp.where(padding_mask, HISTORY_SEGMENT_ID, PADDING_SEGMENT_ID)` |
| `6bb4594` | Kafka `KafkaAuth` / mTLS / SASL | ✅ | `data/streaming/kafkaconsumer.py:37` `class KafkaAuth`、`:64` `sasl()`、`:75` mTLS cert/key 校验；`data/rust_kafka_recsys.py` SASL 参数透传 |
| `6bb4594` | vm-ranker DPP / metrics 纯 Rust 算法 | ✅ | `vm-ranker/dpp.rs`、`vm-ranker/metrics.rs`、`vm-ranker/scoring/dpp_model.rs` 均在位 |
| `6bb4594` | `seed_tweet_id` 不存在，DPP 保持 `None` 语义 | ✅ | 生产调用点 `scoring/dpp_model.rs:86` 传 `None`；`dpp.rs` 本身支持 `seed: Option<&DppInput>`（`:37,97,135`）并有两条例测试（`:509,532`） |
| `6bb4594` | `brazil_2026_election_filter.rs` U5 不移植 | ✅ | 文件不存在 |
| `6bb4594` | `phoenix_experiments_side_effect.rs` U3 不装配 | ✅ | 文件不存在 |

**一处清单记述不精确（低）**：`6bb4594` 清单把 `rust_kafka_recsys.py` 与 `data/streaming/{kafkaconsumer,kafkaloader}.py` 并列书写，读起来像同目录；实际路径是 `phoenix/xrex/data/rust_kafka_recsys.py`（不在 `streaming/` 下）。文件确实存在且已吸收，仅路径表述有歧义。

### 10.2 训练侧"三条不倒退纪律"（计划 §8.3）逐条复核

| 纪律 | 结论 | 证据 |
| --- | --- | --- |
| 历史序列长度不在流水线写死（7 天 / 300 条） | ✅ | `home-mixer/params/config.rs:61 UAS_WINDOW_TIME_MS = 7 * 24 * 60 * 60 * 1000`、`:65 UAS_MAX_SEQUENCE_LENGTH = 300` |
| 行为词表只认 `ActionName` 枚举 | ✅ | Rust：`phoenix_scorer.rs:181–200` 全部以 `ActionName::*` / `ContinuousActionName::DwellTime` 为键；Python：`model_contract.py:10–14 ACTION_IDX_TO_ENUM` / `NONZERO_WEIGHT_ACTION_ENUMS`，`supported_actions_header()` 输出 proto 枚举值。**且该下标表有独立第二份表达做交叉校验**（`phoenix/tests/test_grpc_gateway_contract.py:38–75`），不是单向自证 |
| 流水线内 Phoenix 逻辑只看 `PostId` | ✅ | `models/ids.rs` 为唯一身份类型；`phoenix_scorer.rs` / `phoenix_prediction_client.rs` 均以 `PostId`/`ObjectId` 出入，无字符串-整数分支 |

计划 §8.2 四项：① 派生函数 + 黄金向量双端测试 ✅（见 §2.2）；② `PHOENIX_ENGINE` 装配层选择 ✅（见 §2.8）；③ 元数据/响应校验只在适配器内 ✅（见 §2.7）；④ **原始 served + feedback 事件归档 —— ⚠️ 仅部分满足，见 N5**。

### 10.3 补审新增发现

**N4（低）`vm_ranker_dpp_seed_context_total` 是永不触发的悬挂指标。**
- `vm-ranker/metrics.rs:52–59` 注册了 `DPP_SEED_CONTEXT` counter，描述为 `"Rank requests with a seed_tweet_id, by whether the seed embedding was found"`。
- 但全仓库（排除 `target/`）**除声明处外零引用**——`dpp.rs:9` 只 import 了 `DPP_RESCALING`，`:198` 也只写 `DPP_RESCALING`。
- 根因：本地 `RankRequest` 无 `seed_tweet_id`，`scoring/dpp_model.rs:86` 恒传 `None`，故该 counter 永远不会被 `inc()`。
- 影响：运维看板会出现一个恒为 0、描述指向不存在字段的指标，误导"种子嵌入未命中"的判断。
- *建议*：删除该注册，或改为在 `rescore` 内按 `seed.is_none()` 显式埋点并改写描述。

**N5（中）计划 §8.2 第 4 项"唯一原始数据源"契约未落地。**
计划要求："归档原始 served + feedback 事件（**保留位置、时间戳、动作类型**）作为唯一原始数据源；`data_preprocessor.py` 与将来的 xrex dump 转换器都是它的导出器。"

实测当前端口：

```rust
// home-mixer/clients/served_persistence.rs:12
pub trait ServedPersistence: Send + Sync {
    fn persist(&self, viewer_id: UserId, served_post_ids: &[PostId], request_time_ms: i64) -> Result<(), String>;
}
// home-mixer/feed_state.rs:19
fn record(&self, viewer_id, served_post_ids: Vec<PostId>, request_timestamp_ms: i64) -> ...
```

- **位置**：只有 `Vec<PostId>` 的隐式顺序，没有显式 `(post_id, position)`；一旦落库时拆成单事件即丢失。
- **动作类型**：**完全没有**——端口没有事件类型判别位，served 与 feedback 无法共用一条原始事件流。
- **feedback**：无端口、无实现（与未实现项 #2 同源，此处给出更精确的契约缺口）。
- 影响：计划把该归档定义为"唯一原始数据源"，而 `data_preprocessor.py` 与未来 xrex dump 转换器都要从它导出。当前形状不足以承载该角色，训练侧数据管线届时需回头改端口。
- *建议*：把端口升格为事件流（`ServedEvent { viewer_id, post_id, position, event_time_ms, event_type }`，`event_type ∈ {Served, Feedback...}`），或在 §10 待业务确认清单中显式降级该目标。

### 10.4 补审结论

- **P3 四份清单的吸收声明全部属实**，未发现"声明已吸收但代码缺失"的情况；唯一问题是 `6bb4594` 清单的一处路径表述歧义。
- **计划 §8.3 三条纪律全部成立**，其中"行为词表只认 `ActionName`"还带了独立交叉校验，质量高于最低要求。
- **计划 §8.2 第 4 项是本节唯一实质缺口**（N5），且它是"训练数据管线的上游契约"，比 §9 的 N1（帖龄窗口）更靠前，建议一并纳入 §10 待业务确认。
- §8 遗留的两处边界至此**全部关闭**；本报告不再有"按自我声明采信"的未验证区域。

## 11. mrpyq 业务适配器专项审查

> **审查对象**：工作树未提交改动 —— 新增 `home-mixer/clients/mrpyq_adapters.rs`（794 行，未跟踪）、`home-mixer/clients/mrpyq_recommendation_data_client.rs`（+129）、`home-mixer/candidate_pipeline/phoenix_candidate_pipeline.rs`（+36）、`proto/definitions/recommendation_data.proto`
> **审查日期**：2026-09-14
> **基线**：计划 §2.3 / §2.6 / §3 / §5 / §7 / §11
> ⚠️ **审查期间代码被并发修改两次**（`mrpyq_adapters.rs` 629 → 703 → 794 行）。本节结论已按 794 行版本全量重核；两条原发现因此闭环，记在 §11.4。
>
> ⚠️ **本节是 794 行版本的快照，原样保留以存审查轨迹**：其中的行号、常量值（如 `THUNDER_MAX_RESULTS = 1200`）和"串行"一类的描述都以审查当时为准，不回写。**M1–M8 的现状以 §12 为准。**

这是计划 §5「U1 适配器清单」9 项里第一次出现真实后端接入，把 TES / in-network / fallback / VF 四个端口挂到了 mrpyq `RecommendationDataService`。**U1 进度从 1/9 推进到 4/9**（含 §2.7 已有的 `SlimPhoenixPredictionClient`）。

### 11.1 安全面：VF 层名存实亡

**M1（严重）VF 端口用一级过滤位冒充 viewer 级鉴权，viewer 级鉴权实际缺席。**

`mrpyq_adapters.rs:379–395 visibility_reason()` 的全部判据只有一个布尔位：

```rust
fn visibility_reason(content: &RecommendationContent) -> Option<FilteredReason> {
    if content.recommendation_eligible { return None; }   // 放行
    ...
}
```

而这个字段的语义由契约自己写死了 —— `proto/definitions/recommendation_data.proto`：

- `:10`："its eligibility result is not a final exposure or safety decision. **A later viewer/eligibility adapter must apply those rules.**"
- `:78–81`："This first-stage check only covers deletion, business visibility, text audit visibility, and video audit visibility. **It is not a final safety or viewer-specific eligibility decision.**"

即 proto 明确要求"后面还要有一个 viewer 级适配器"，而当前实现把这个一级位直接当成了那个适配器。两个后果：

1. **鉴权面为空**：拉黑、屏蔽、私密、地域、viewer 举报等 viewer 维度规则**一条都没跑**。`get_result()` 的 `_safety_level` / `_for_user_id` / `_context` 三个入参全部带下划线丢弃（`:329–331`），签名上就能看出它不看 viewer。
2. **该层是纯空转**：同一个 `recommendation_eligible` 已经被前置的 `first_stage_eligible_filter.rs:19` 消费掉（`recommendation_eligible != Some(false)` 留存）。候选走到 post-selection VF 时，凡是活下来的必然 `eligible == true`，`visibility_reason()` 对每一条都返回 `None`。**这一层的输出恒为"全放行"。**

违反计划 §2.6「fail-closed 的 viewer 级鉴权」、§3「VF ⇐ 业务 eligibility（fail-closed）[U1]」、§11「不得……绕过 eligibility 适配器来假装接入完成」。

*建议*：在真实 viewer 级 eligibility RPC 出现前，VF 端口维持 `Disabled`（fail-closed 路径），不要用一级位占位 —— 占位的代价是把 §5 清单里的 VF 标成"已完成"，掩盖了鉴权缺口。若要保留该实现，须在 §5 清单里降级标注为"first-stage only，非 viewer 级"。

**M2（严重）VF 失败策略默认放行，且代码里根本不存在 fail-closed 选项。**

本条是 §9.3 / N3 的**升级**：复审时定级为"机制新增但默认宽松（低）"，重核后问题比当时判断的严重。

| 计划 §7 要求 | 实测 |
| --- | --- |
| 缺项 → Drop | `vf_candidate_hydrator.rs decision_for()` 把缺项映射为 `Unavailable`，不是 Drop |
| 超时 → Unavailable | ✅ 一致 |
| **默认沿用全量拒绝** | `feature_policy.rs:3–4` `#[default] AllowAll` —— 默认全放行 |

关键点：**`InNetworkOnly` 也不是"全量拒绝"**。`vf_filter.rs:44–46` 只在"策略 = `InNetworkOnly` 且 `in_network != Some(true)`"时丢弃，即网内候选在 VF 不可用时照样放行。也就是说两个可选值**都不满足** §7 的"全量拒绝"，fail-closed 在代码里没有对应实现，不是配置问题。

叠加效应：mrpyq 未返回的帖子在 `contents_by_id` 里被 `continue` 跳过（`mrpyq_adapters.rs:336–338`），到 `VFFilter` 是 `Unavailable`，默认策略下**放行**。于是"mrpyq 查不到这条帖子"的结果是把它推出去。

另外 `runtime_config.rs:69` 的 `production_ready` 门禁条件里**不含 VF 策略**，所以即便将来门禁放开，也不会拦住 `allow_all`。

还有一处流程问题值得记录：工作树版 `plan.md` §7 已被改写成描述 `allow_all` 默认的现状，而不是走"计划变更"把纪律显式降级。**文档向实现对齐**会让后续审查失去基线。

*建议*：① 补一个真正的 fail-closed 取值（VF 不可用 → 全丢）并设为默认；② 把"VF 策略未设为 fail-closed"加入 `production_ready` 拒绝条件；③ `plan.md` §7 的改写要么回退，要么标注为经批准的计划变更并说明理由。

### 11.2 可用性面：一上量就是空 feed

**M3（高）内容水合的超时预算倒挂，大候选集必然拿不到数据。**

三个数字对不上：

| 位置 | 值 |
| --- | --- |
| `mrpyq_adapters.rs:127` | `for chunk in missing.chunks(MAX_FEED_IDS)` —— **串行**，每批 200 |
| `params/config.rs:53` | 每批 RPC 超时 `MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS = 500` |
| `params/config.rs:46` | 外层 `TES_REQUEST_TIMEOUT_MS = 500` 包住**整个** `get_tweet_core_datas()` 调用 |

`params/param.rs:19 THUNDER_MAX_RESULTS = 1200`、`:17 PHOENIX_MAX_RESULTS = 1000`，一次请求 5–6 个批次串行。只要后端单批均值超过约 85ms，外层 500ms 预算就被击穿。**内层单批的超时预算等于外层整体预算**，这个配置下多批场景没有能通过的路径。

超时不是降级而是**全损**：外层 `timeout` 返回 `Err` → 整批 core data 缺失 → `CoreDataHydrationFilter` 丢弃全部候选 → 空 feed。

各批次之间无依赖，可以并发。

*建议*：批次并发发出（`join_all` / `buffer_unordered`），或把外层预算改为"批数 × 单批超时 + 余量"。同时确认这两个常量的关系应该在代码里显式表达，而不是靠两个独立的 500 兜住。

**M4（高）`ContentCache` 的锁跨 RPC 持有，把内容水合变成全进程串行。**

这是本次并发修改新引入的。`contents_by_id()` 在 `:109` 拿锁，然后在**持锁状态下**跑完 `:127–139` 的全部 RPC 批次。注释（`:81–83`）说明这是刻意设计：

> The lock is deliberately held across the RPC: concurrent callers queue behind the first fetch and then read its result instead of issuing their own, which also bounds how much concurrency reaches mrpyq.

同一请求内的多端口去重（M8 的修复）确实靠这把锁实现了，但代价被低估了：`ContentCache` 在 `pipeline_adapters()`（`:63–70`）里**每进程只建一个**，不是请求级的。这把 `tokio::sync::Mutex` 因此是**全服务唯一**的。后果：

- 任意时刻全服务只有一个请求能水合内容，其余全部阻塞在 `:109`。
- 单请求持锁时长 = M3 里的 5–6 批串行 ≈ 数百毫秒 → 吞吐上限约个位数 QPS。
- 排队时间**计入** M3 那个 500ms 外层预算 —— 第二个并发请求还没开始发 RPC 就已经超时了。两条问题互相放大。

"bound how much concurrency reaches mrpyq" 这个目标成立，但实现把并发上限设成了 1，而且是通过全局互斥而不是显式的并发度控制。

*建议*：改为"锁只保护 map，RPC 在锁外"，用 per-key `Weak<OnceCell<..>>`（与 `tes_hydration_provider.rs` 已有的请求级合并同构）做同批合流；若确实需要限制打到 mrpyq 的并发，用 `Semaphore` 显式设一个 > 1 的值。

**M5（中）召回分页最坏 5 秒，且来源层没有超时。**

`list_posts()` 最多跑 `MAX_LIST_PAGES = 10` 页（`:28`、`:167`），串行，每页 500ms（`config.rs:53`），最坏 **10 × 500ms = 5s**。`thunder_source.rs` / `fallback_source.rs` 都没有来源级超时，上层也没有请求级 deadline（§5 已记录），所以这 ~3s 会原样透传到用户。

实际触发条件：`max_results` 大 + 每页返回条数少（mrpyq 按 ZSET 分数游标翻页，重复项会被 `:225` 的 `seen` 去重后不计入预算，页数因此可能高于理论值）。

*建议*：给召回加来源级超时（复用已有的"部分失败降级"分支，`:180–186` 已经能带着已拿到的结果继续）。

**M6（中）后端地址配错时静默降级成空 feed，且无启动期信号。**

`pipeline_adapters_from_env()` 在配置解析失败（`:56–59`）和连接创建失败（`:49–52`）时都只 `log::error!` 然后返回 `None`。返回 `None` 之后，`phoenix_candidate_pipeline.rs:425/446/480` 的三处 `if let Some(adapters)` 全部落空 → TES、in-network、VF 一起变成 Disabled → 服务正常启动、健康检查正常、feed 恒空。

区别在于**意图**：没配 `MRPYQ_RECOMMENDATION_DATA_ADDR` 是"不启用"，配了但错是"要启用却失败了"。当前代码把这两种情况合并处理。

*建议*：`Ok(config) if config.address.is_some()` 分支内的失败应当上抛，让进程启动即失败。

### 11.3 测试与可维护性

**M7（低）`FakeMrpyq.list_error` 没有 setter，召回的两条错误分支零覆盖。**

字段声明在 `:407`、初始化在 `:417`、读取在 `:455`，但 `impl FakeMrpyq`（`:412–444`）只提供了 `with_content_error` / `with_page` / `with_contents`，**没有 `with_list_error`**。结果是 `list_posts()` 的两条错误路径都没有测试：

- `:179` `Err(error) if posts.is_empty() => return Err(...)` —— 首页失败上抛
- `:180–186` 非首页失败 → warn + 带着已有结果 break —— 部分降级

第二条正是 M5 建议要复用的分支，目前没有任何测试保证它的行为。

*建议*：补 `with_list_error`，覆盖"首页失败"与"第二页失败"两种情形。

**M8（低）装配层没有 mrpyq 分支的测试，且既有测试对环境变量产生了隐式依赖。**

`home-mixer/tests/` 全目录对 `mrpyq` / `MRPYQ` **零引用**。`p2_pipeline_assembly.rs` 的 8 个用例只覆盖 Demo 与 Degraded，没有一条断言"配置了 mrpyq 地址时 TES/in-network/VF 挂的是 mrpyq 适配器"。

附带风险：`degraded_assembly_does_not_use_demo_fallback_data` 现在的通过依赖 `MRPYQ_RECOMMENDATION_DATA_ADDR` 未设置 —— 在设了该变量的环境里跑测试，装配结果会变而用例并不知情。

*建议*：`pipeline_adapters()` 已经是可注入的（接受 `Arc<dyn MrpyqRecommendationDataClient>`），补一条注入 `FakeMrpyq` 的装配测试；同时让 Degraded 用例显式声明它期望的环境。

### 11.4 审查期间已闭环的两项

审查窗口内代码被并发修改，以下两条在重核时已不成立，记录以保留审查轨迹：

| 原发现 | 状态 | 依据 |
| --- | --- | --- |
| 同一批内容被重复拉取约 3 次（TES core / TES media / VF 各发一次 `BatchGetRecommendationContents`，而 core 与 media 字段本就在同一条消息里） | **已修复** | 新增 `ContentCache`（`:84–146`），TES 与 VF 共享同一份水合结果；测试 `tes_and_vf_hydrate_a_feed_once_whether_or_not_mrpyq_has_it`（`:657–685`）断言三次调用只打一次 RPC。代价见 M4 |
| `DEFAULT_PAGE_SIZE` 死代码（`remaining.min(MAX_FEED_IDS).max(DEFAULT_PAGE_SIZE.min(remaining))` 恒等于前半段） | **已修复** | 常量已删除，`:172` 简化为 `remaining.min(MAX_FEED_IDS)` |

另有一处经确认**不构成问题**：`ContentCache` 是进程级、TTL 2 秒（`:29`）的跨请求缓存，意味着删帖/下架后最多 2 秒内仍可能被推出。对推荐 feed 的一级过滤位而言，这是常规的缓存新鲜度权衡，窗口可接受，不单独定级。但需要意识到它与 M1 叠加后的含义：**当前这条链路上，唯一的内容准入判据是一个可以陈旧 2 秒的一级位**。

### 11.5 本节结论

- **U1 适配器进度 4/9**：TES、in-network、fallback、VF 四个端口有了真实后端；仍缺 `MrpyqStratoClient`、`MrpyqUasFetcher`、`MrpyqGizmoduckClient`、`MrpyqImpressedPostsClient`、served/feedback 客户端。其中 `MrpyqStratoClient` 缺位意味着 **`AuthorSocialgraphFilter` 在 mrpyq 链路上拿不到任何数据，拉黑/屏蔽不生效** —— 这与 M1 指向同一个缺口：**viewer 维度的准入规则整体没有数据源**。
- **接入未达"可上线"**：M1/M2 是安全面（鉴权缺席 + 失败放行），M3/M4 是可用性面（预算倒挂 + 全局串行），四条中任意一条都足以阻断生产接入。
- **与计划的关系**：计划 §11 明确"不得……绕过 eligibility 适配器来假装接入完成"。当前实现不是绕过，但用一级位顶替 viewer 级判定，效果等价，且因为端口"看起来已实现"而更难被发现。建议在 §5 清单中把 VF 标为"部分（first-stage only）"而非完成。
- 质量基线未破：`cargo test --workspace` **400 passed / 0 failed**，`cargo clippy --workspace --all-targets -- -D warnings` **0 warning**。

---

## 12. M1–M8 修复执行记录

> **执行日期**：2026-09-14
> **范围**：§11 的 M1–M8 全部八条
> **验收**：`cargo test --workspace` **419 passed / 0 failed**，`cargo clippy --workspace --all-targets -- -D warnings` **0 warning**，`cargo fmt --all -- --check` 干净
> §11 是审查快照，原样保留；本节记录实际改法与残留缺口，冲突处以本节为准。

### 12.1 逐条落点

| 编号 | 结论 | 改法 |
| --- | --- | --- |
| M3 水合预算倒挂 | 已闭环 | ① `ContentCache::contents_by_id` 用 `futures::try_join_all` 并发发各 chunk，500ms 外层预算覆盖整次调用而不是单个 chunk；② 同时把批数本身压下来：`THUNDER_MAX_RESULTS` 1200 → 400（召回源是 mrpyq 关注 inbox，单账号硬顶 2000 条，`AgeFilter` 只留 48h、出口只有 `RESULT_SIZE=35`，1200 既取不满也用不上），并在 NETWORK 召回上按 `MAX_POST_AGE` 早停 —— inbox 按发帖时间倒序，第一条超龄候选之后不会再有新帖，继续翻页只是把 `AgeFilter` 待会儿要丢的东西拉来水合一遍。FALLBACK 池按入池时间排序，老帖可能排在顶部，因此只逐条过滤、不早停 |
| M4 全局串行锁 | 已闭环 | 锁只用于读写 map，RPC 移到锁外；并发同集合的调用者通过 `Weak<OnceCell<..>>` in-flight cell 合流（沿用 `tes_hydration_provider` 的既有写法），失败的 cell 随最后一个等待者析构，不落缓存 |
| M5 召回无总预算 | 已闭环 | 新增 `MRPYQ_RECALL_BUDGET_MS = 1500`，翻页循环每轮检查 deadline；超时保留已读页而不是整体失败 |
| M6 配置错误静默降级 | 已闭环 | `pipeline_adapters_from_env` 改返回 `anyhow::Result<Option<..>>`：**没配地址** = `Ok(None)`（端口保持 Disabled），**配了但不可用** = `Err`，装配处 `expect` 直接让进程起不来 |
| M7 召回失败分支无测试 | 已闭环 | `FakeMrpyq.list_error` 改为按 `page_token` 索引，补首页失败与后续页失败两个用例 |
| M8 装配层无 mrpyq 测试 | 已闭环 | 新增 `home-mixer/tests/p2_mrpyq_assembly.rs`（独立测试二进制，因为要写进程级环境变量）；`degraded_assembly_does_not_use_demo_fallback_data` 显式声明它依赖地址未配置 |
| M2 VF 失败策略没有 fail-closed 取值 | 已闭环 | 新增 `VfFailurePolicy::FailClosed` 并设为 `#[default]`，`allow_all` 降级为需显式配置的逃生口；非法值也回落 `fail_closed`；非 demo 模式配 `allow_all` 时启动告警 |
| M1 viewer 维度准入无数据源 | 部分闭环，见 12.2 | 见下 |

### 12.2 M1：方案 C 的实际交付与残留

按方案 C 做了两件事：

**① VF 正名。** `MrpyqVisibilityFilteringClient` → `MrpyqFirstStageEligibilityClient`，并在类型文档里写清它只承载 mrpyq 的一阶段 `recommendation_eligible`：这是帖子属性、对所有 viewer 相同，且 `FirstStageEligibleFilter` 上游已消费同一标志，所以挂上它**不构成第二道 viewer 感知的准入**。计划 §7 对应行同步改写。

**② viewer 关系端口。** 新增 `proto/definitions/viewer_relation.proto` 定义 `ViewerRelationService.GetViewerRelations`（拉黑 / 被拉黑 / 静音 / 屏蔽词四项），刻意**不动** `RecommendationDataService` —— 那个 service 的注释明确写了自己不聚合 Relation，改写它等于制造新的文档漂移。Rust 侧新增 `clients/mrpyq_viewer_relation_client.rs` 与 `MrpyqStratoClient`，复用同一个 `MRPYQ_RECOMMENDATION_DATA_ADDR`（同一部署、同一 proto 包），装配后 `AuthorSocialgraphFilter` 与 `ViewerMutedKeywordFilter` 拿到真实数据。

**残留缺口（未修，需要单独决策）：**

1. **后端还没实现这个 RPC。** 契约和适配器都就位了，mrpyq 侧实现后配上地址即生效；在那之前 Strato 端口仍是 Disabled。
2. **"关系源不可用"仍然是放行的，而且根子不在适配器。** `candidate-pipeline/candidate_pipeline.rs:262-272` 对 query hydrator 的失败只 `error!` 记日志、不中断请求，`query.user_features` 保持全空默认值 —— 于是"读不到拉黑名单"和"该用户没拉黑任何人"在过滤器眼里完全一样。把 `DisabledStratoClient` 从"返回空列表"改成"返回错误"**不会改变这个结果**，只多一行日志。真要堵，需要在 query 上加一个"关系已成功水合"的标志位并让 `AuthorSocialgraphFilter` 检查它 —— 这会改动框架层语义，超出本轮范围，未动。
3. `MrpyqStratoClient` 对无法解析的 account ID 采取**丢弃单条 + 告警**而非整体失败：因为在上面第 2 条的框架行为下，整体失败会让**全部**拉黑关系落空，反而更宽松。

### 12.3 与 §11.5 的差异

- U1 适配器进度 **4/9 → 5/9**（新增 Strato）。仍缺 `MrpyqUasFetcher`、`MrpyqGizmoduckClient`、`MrpyqImpressedPostsClient`、served/feedback 客户端。
- §11.5 说"四条中任意一条都足以阻断生产接入"：M2/M3/M4 已解除，M1 从"顶替"变成"契约就位、等后端 + 一个框架层缺口"。
