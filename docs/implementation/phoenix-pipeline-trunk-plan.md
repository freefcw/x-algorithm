# 以 PhoenixCandidatePipeline 为主干的推荐链路收敛方案

> **状态**：`decision`（已定方向，待 P0 执行）
> **日期**：2026-09-12
> **基线**：`mp` = `0300a09`；`mp-slim` = `ffd17eb`（二者同父 `aac24f6`）；上游 `origin/main` = `6bb4594`；`mp` 已吸收的上游锚点 = `902a06f`
> **读者**：推荐服务开发、Phoenix 训练、业务后端接口负责人
> **事实边界**：本文只记录决策、依据与执行边界；所有数字来自对上述三个分支的源码与 diff 实测，不代表业务已上线。本文随 P0 一并落到主干分支；在 `mp-slim` 上仅作为决策记录。

---

## 1. 决策摘要

1. **主干**：以 `mp` 分支的 `home-mixer::PhoenixCandidatePipeline`（含 `candidate-pipeline` 框架）为唯一编排主干，放弃 `mp-slim` 的 `recommendation-service` 硬编码流程。理由是持续吸收上游：`mp` 已有 U0–U3 差异分类与逐快照能力清单制度，业务接入在该制度下等于"实现端口 trait + 装配点注入"。
2. **业务差异只允许四种进入方式**：U1 适配器（外部依赖替换）、U2 增量（本地扩展）、**U4 身份类型替换**（新增）、**U5 产品不适用**（新增）。装配点仍是 `PhoenixCandidatePipeline::build_with_clients()`。
3. **ID 方案选 A**：流水线内以 Copy newtype `ObjectId([u8; 12])`（别名 `PostId` / `UserId`）为唯一身份；proto 与所有外部边界用 24 位小写 hex 字符串；不保留任何 u64 身份，也不引入哈希映射表。
4. **删除范围收窄到 U5**：只物理删除引用 / 转推 / 订阅三类的 6 个专用组件（另吸收 `business_feed/` 与 `recommendation-service/`）；其余不需要的能力（ForYou / Blender、广告槽位、话题、MoE、CachedPosts、Kafka 端口等）保留上游形态、不装配。
5. **模型引擎**：先用本地移植的小型 Transformer（`phoenix/services` 演示网关 + `data_preprocessor.py`）接真实数据；同时以四项前置动作保留随时切换 xrex 生产引擎的能力，切换时流水线零改动。
6. **mp-slim 的契约层成果全部保留并落回主干**：string 业务 ID、Phoenix 元数据与响应校验、fail-closed 的 viewer 级鉴权、served 落库成功才响应、deadline 传递、`phoenix/` 上的训练侧改造。

---

## 2. 背景事实

### 2.1 三个分支

| 项 | `mp` | `mp-slim` | 上游 `origin/main` |
| --- | --- | --- | --- |
| Rust 编排 | `home-mixer` 18,605 行 + `candidate-pipeline` 2,087 行 + `thunder` 30,860 行（其中 28,548 行为未编译的 Thrift schema）+ `vm-ranker` 1,375 行 | `recommendation-service` 1,561 行 | `home-mixer` 224 个文件，自分叉点净增约 4.6 万行 |
| Phoenix 流水线组件 | 44 个（41 个默认装配） | 无流水线，流程硬编码在 `lib.rs` 约 120 行 | 75 个（86 个文件依赖未开源的 `component_library`，为 U3） |
| Rust 单测 | 318 + 21 + 5 + 10 = 354 | 16 | — |
| ID 类型 | u64 Snowflake（69 个文件、356 处 u64） | string ObjectId | u64 |
| 上游同步 | `docs/upstream-sync/` 13 份能力清单，锚点 `902a06f` | 目录已删除 | 分叉点 `aaa167b` 后 26 个快照，最新 `6bb4594`（09-12） |

`mp → mp-slim` 的 diff：356 个文件，+4,795 / −72,046。

### 2.2 mp 上的两条互不相通链路

`mp` 的 `PhoenixCandidatePipeline` 使用 u64 与 Demo / Disabled 客户端，从未接过真实业务数据；真正接业务 gRPC（`RecommendationDataService`，string ObjectId，`go_package` 指向业务 Go 后端）的是独立的 `business_feed/` 规则 Feed，它不使用流水线框架。`mp-slim` 演化了后者、删除了前者。本方案把两条合并：让业务数据流过流水线框架。

### 2.3 mp-slim 做对与做过头的地方

做对（保留）：string 业务 ID 端到端（训练 / 网关 / 服务对同一字符串做 md5）；网关 `feature-schema` / `model-version` / `random-weights` / `supported-actions` 四项元数据校验与 NaN / 重复 / 缺失响应校验；eligibility 缺项或超时一律拒绝；served 持久化成功才 2xx、feedback 幂等；请求级 deadline 下传；`phoenix/` 上可训练嵌入表、观测头掩码、bundle 产物、负样本帖龄窗口、`eval_ranker.py` 基线对照。

做过头（恢复）：编排框架与逐阶段观测消失；只有一路候选（≤200 条），Phoenix Retrieve 网关在跑但无人调用；候选没有内容特征（业务内容接口已返回 text / tag_ids / room_id / has_video / gift_value / recommendation_eligible）；精排从 20 头加权 + 作者衰减 + OON × 0.75 退化为 5 头求和 + 每页作者硬上限 2，`in_network` 不进 Phoenix 路径；单测 354 → 16；删除了唯一真实业务客户端 `mrpyq_recommendation_data_client` 并另立一套 HTTP JSON 契约。

---

## 3. 目标链路

```
gRPC ScoredPostsService.GetScoredPosts (viewer_id: string, seen_ids, served_ids, is_bottom_request)   [U4]
  │  可选：薄 HTTP /v1/feed 适配层（鉴权 + 字段转换）
  ▼ QueryBuilder → ScoredPostsQuery { user_id: UserId, … }
  ├─ Query 水合（并行，组件不动）
  │     ScoringSeq / RetrievalSeq ← UserActionSequenceOps  ⇐ MrpyqUasFetcher         [U1]
  │     Followed / Blocked / Muted / SafetyFeatures ← StratoClient ⇐ MrpyqStratoClient [U1]
  │     ImpressedPosts ← ImpressedPostsClient ⇐ 业务已看列表                            [U1]
  ├─ 召回（并行）
  │     ThunderSource ← InNetworkPostsClient ⇐ 业务 NETWORK inbox   [U2 抽 trait + U1]（Thunder 进程：可选）
  │     PhoenixSource ← PhoenixRetrievalClient                                            [开关]
  │     FallbackSource ⇐ 业务 FALLBACK 池                                                 [U2 新增]
  │     Topics / MoE / CachedPosts：保留形态，不生效
  ├─ 候选水合（并行，组件不动）
  │     InNetwork · CoreData · VideoDuration · HasMedia · Language ← TESClient ⇐ MrpyqTESClient [U1]
  │     ✕ Quote · Subscription（U5）
  ├─ 过滤（串行）
  │     DropDup → CoreDataMissing → FirstStageEligible[U2] → Age[改读 created_at_ms] → Self
  │     → Seen → SeenBackup → Served → MutedKeyword → AuthorSocialgraph → Video → Topic*
  │     ✕ RetweetDedup · IneligibleSubscription（U5）
  ├─ 精排（串行）
  │     PhoenixScorer ← PhoenixPredictionClient（元数据 + 响应校验在适配器内）              [U1]
  │     → RankingScorer（retweet / quote 权重 = 0，其余按埋点）                            [参数]
  │     → RuleFallbackScorer（Phoenix 失败时整批替代，写 degraded_reason）                 [U2]
  │     → VMRanker[开关] → AuthorColdStart[开关]
  ├─ 选择        TopK(50)
  ├─ 选后水合    Gizmoduck ⇐ 业务作者状态 [U1]；VF ← VisibilityFilteringClient ⇐ 业务 eligibility（fail-closed）[U1]
  ├─ 选后过滤    VFFilter → DedupConversation（无回复时无操作）      ✕ AncillaryVF（U5）
  ├─ 服务层      ServedPersist 成功才响应 [U2]；InMemoryFeedStateStore 记 served
  └─ 副作用      ResponseStats；RequestCache / Kafka 端口保留不装配
外层 ForYouFeedService → BlenderSelector：模块槽位全关，等价直出帖子（保留上游形态）
```

---

## 4. 差异分类扩展（拟并入 `docs/upstream-sync/upstream-first-maintenance.md`）

| 类 | 含义 | 规则 |
| --- | --- | --- |
| `U4` 身份类型替换 | 业务身份是 96 bit ObjectId，上游 u64 无法承载 | 流水线内身份统一为 `PostId` / `UserId`（Copy newtype）；移植上游代码时做机械替换：ID 位置 `u64` → `PostId` / `UserId`，proto3 哨兵 `0` → `Option` / 空串，`to_be_bytes()` → `as_bytes()`，`wrapping_mul` 分桶 → `to_u64_hash()`，Snowflake 推时间 → 水合的 `created_at_ms`；时间戳 / 计数 / 阈值 / 本地相关 ID / 哈希算术中的 u64 原样保留 |
| `U5` 产品不适用 | 引用、转推、订阅等产品不存在的概念 | 只删除**专用**组件文件并从装配移除；`PostCandidate` / `PhoenixScores` / `UserFeatures` 中对应字段保留为 `None` / 空，共享过滤器中的相关分支保持为无操作；对应行为头权重设 0；上游对这些专用文件的后续改动在能力清单中记为"跳过" |

上游同步摩擦实测（`c65aa17 → 902a06f` 共 18 个快照）：`home-mixer` 每快照改 0–19 文件、0–4k 行，含 `u64` 的增删行合计 67 行、单快照 0–16 行，占总改动约 0.7%；待处理的 `49815da → 6bb4594` 四个快照含 `u64` 行共 21 行，全部为 `Vec<u64>` / `HashSet<u64>` / `HashMap<u64, …>` / `user_id: u64` 参数形态。U4 的持续成本即此。

---

## 5. 组件取舍

判定含义：**保留** = 上游形态组件不动，外部依赖只换适配器；**改写** = 组件本身需改（U4 或小 U2）；**可选** = 留在磁盘、不装配或开关默认关；**删除** = U5 或已被吸收；**新增** = 需补的 U1 / U2。

| 阶段 | 保留 | 改写 | 可选 | 删除 | 新增 |
| --- | --- | --- | --- | --- | --- |
| 框架 / 治理 | `candidate-pipeline`、`PhoenixCandidatePipeline`、`docs/upstream-sync`、`runtime_config` + `feature_policy` + `debug_access`、`mrpyq_recommendation_data_client`、`phoenix_recsys.proto` | ID 类型（U4） | — | `recommendation-service/` | 本文、U4 / U5 规则 |
| Query 水合 | UAS 三件、Followed / Blocked / Muted、UserSafetyFeatures、ImpressedPosts、ServedHistory + PastRequestTimestamps + `InMemoryFeedStateStore` | — | UserTopics、Bloom 端口 | SubscribedUserIds | — |
| 召回 | PhoenixSource | ThunderSource（`ThunderClient` 抽为 `InNetworkPostsClient` trait） | PhoenixTopics、MoE、CachedPosts、TweetMixer、ScoredPostsSource + ForYou、Ads / WTF / Prompts / PushToHome 槽位 | — | FallbackSource |
| 候选水合 | TES 五件、InNetwork、FilteredTopics、Gizmoduck、VF | — | BlockedBy 端口 | Quote、Subscription | — |
| 过滤 | DropDup / CoreDataHydration / Self、Seen / SeenBackup / Served、MutedKeyword、AuthorSocialgraph、Video / TopicIds / NewUserTopicIds、VFFilter、DedupConversation | AgeFilter（改读 `created_at_ms`） | — | RetweetDedup、IneligibleSubscription、AncillaryVF | FirstStageEligible |
| 精排 | PhoenixScorer、RankingScorer（权重表调整，不改码） | — | VMRanker + `vm-ranker` crate、AuthorColdStart | — | RuleFallbackScorer |
| 选择 | TopK、Blender | — | — | — | — |
| 落库 / 副作用 | ResponseStats | — | RequestCache、Kafka seen / served 端口 | — | ServedPersist（服务层同步步骤）、feedback RPC |
| 外壳 / 协议 | gRPC ScoredPosts / ForYou 服务 | `home_mixer.proto`（ID 改 string）、QueryBuilder | HTTP `/v1/feed` 薄层、cursor 会话、`in_network.proto` / `vm_ranker.proto` | `BusinessFeedService` + `business_feed/` | 引擎选择配置 |
| 独立 crate | — | — | `thunder`（留 workspace、不部署）、`vm-ranker` | — | — |

U1 适配器清单（P2 实现）：`MrpyqTESClient`、`MrpyqStratoClient`、`MrpyqUasFetcher`、`MrpyqVisibilityFilteringClient`、`MrpyqGizmoduckClient`、`MrpyqInNetworkPostsClient`、`MrpyqImpressedPostsClient`、served / feedback 后端客户端、`SlimPhoenixPredictionClient`（含 mp-slim 校验逻辑）。

---

## 6. ID 方案

### 6.1 为什么是 A

三个候选：A Copy newtype、B 内部 u64 哈希 + 请求级反查注册表、C `type PostId = String`。

- 逐个核对所有 ID 消费方，**没有任何业务方需要 u64**：业务后端、客户端、现用 Phoenix 网关与训练全部要 string；Thunder / VM Ranker 的 i64 / u64 是我们自己的实现选择；Bloom 是 X 客户端契约；AgeFilter 的 Snowflake 假设三方案都要改。唯一需要整数的是未启用的 xrex 引擎，它需要的是"训练与线上共用一个固定的 ObjectId → int64 派生函数"，不需要流水线内部是 u64。
- B 的注册表因此只服务上游类型签名一件事，却要以七条运行时纪律（固定带版本的哈希、单一 intern 入口、单一 resolve 出口、碰撞即拒绝、网关形状校验、日志助手、CI grep）维持四种一致性；漏一条即回到 mp-slim 记录过的"训练哈希 ObjectId、线上哈希整数十进制串"故障。
- C 与 A 的移植位置完全相同，只多付 clone 噪音与热路径分配（每请求约 25 次全量候选 clone × ≤2,200 条）。
- A 的一次性成本最高（69 个文件、约 161 处测试字面量），之后每快照约 1% 的机械改动由编译器列清单；身份精确、零运行时开销、日志即业务 ID。

### 6.2 `home-mixer/models/ids.rs` 规范

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ObjectId([u8; 12]);      // 表示为私有字段
pub type PostId = ObjectId;
pub type UserId = ObjectId;         // 上游同样以一个 u64 表达两者，别名不引入互转

impl ObjectId {
    pub const NIL: Self;                                          // Default，对应上游 0
    pub fn parse(s: &str) -> Result<Self, IdError>;               // 仅接受 24 位小写 hex
    pub fn parse_optional(s: &str) -> Result<Option<Self>, IdError>; // "" → None
    pub fn is_nil(self) -> bool;
    pub fn as_bytes(&self) -> &[u8; 12];                          // 替代 to_be_bytes()
    pub fn timestamp_secs(self) -> u32;                           // 仅诊断 / AgeFilter 回退
    pub fn to_u64_hash(self) -> u64;                              // 见 6.3
    pub fn from_parts(ts_secs: u32, seq: u64) -> Self;            // 演示数据生成
}
// Display / Debug：固定小写 24-hex；FromStr；serde 为字符串
#[cfg(test)] impl From<u64> for ObjectId { /* 末 8 字节大端零填充 */ }   // 仅测试，生产禁止从整数构造
```

`Display` 输出与 `data_preprocessor.hash_id_to_ints` 的输入形态一致（需核对行为日志存储为小写 hex）。`[u8; 12]` 的字典序即 ObjectId 时间序，与上游"ID 序 ≈ 时间序"的隐含假设一致。

### 6.3 `ObjectId → int64` 派生函数（xrex 与分桶共用）

```
to_u64_hash(oid) = from_be_bytes(md5(oid 的 12 个原始字节)[0..8]) & 0x7FFF_FFFF_FFFF_FFFF；结果为 0 则置 1
```

- 63 位非负，兼容 xrex 的 `int64 user_id` 与 `uint64 tweet_id`；避开 0（xrex padding）。
- md5 在 Rust（上游 `ranking_scorer` 已依赖 `md5` crate）与 Python（stdlib）两侧现成。
- **不要取末 8 字节**：ObjectId 末 3 字节是进程内计数器，2²⁴ 次后回绕，同一进程的两篇帖子会碰撞。
- Rust `ids.rs` 与 `phoenix/services/model_contract.py` 各一份实现，共享黄金向量文件（若干 ObjectId → int64），`cargo test` 与 `pytest` 同时校验。
- 现用演示网关路径不调用它；它只在 xrex 适配器、xrex 训练转换器和上游 `wrapping_mul` 分桶处使用。

### 6.4 356 处 u64（+17 处 i64 身份）的去向

| 类别 | 规模 | 处理 |
| --- | --- | --- |
| 帖子 / 用户 / 会话身份（字段、`HashMap<u64, …>` 键、客户端返回值、feed state 键） | ≈296 u64 + 17 i64 | → `PostId` / `UserId`；集合只换键类型 |
| proto3 哨兵 `0`（`scored_posts_server` 的 `unwrap_or(0)`、`valid_id(id) != 0`） | 5 | 出 → `map(to_string).unwrap_or_default()`；入 → `parse_optional("")` = `None`，非法串 → `None` 并计数 |
| Snowflake 推时间 | AgeFilter 1 处（+ 未装配 TweetMixerSource 测试 2 处） | `PostCandidate` / `PureCoreData` 增加 `created_at_ms: Option<u64>`（U2），TES 适配器填；AgeFilter 改读，缺失时回退 `timestamp_secs()`；删 `util/snowflake.rs` |
| 时间 / 超时 / 窗口（`*_TIMEOUT_MS`、`MAX_POST_AGE`、`impressed_time_ms`、`last_scored_at_ms`…） | 36 | 保留 u64 |
| 计数 / 阈值 / 上限（`view_count`、`COLD_START_IMPRESSION_THRESHOLD`、`LOCAL_*_LIMIT`…） | 10 | 保留 u64 |
| 本地相关 ID（`prediction_id`、`prediction_request_id`、`generate_request_id()`） | 4 | 保留 u64；非业务 ID |
| Bloom 内部算术（`util/bloom_filter.rs`） | 5 | `murmur_hash` 签名改 `&[u8]`，输入 `as_bytes()`；位算术不动 |
| 演示契约（`proto/src/demo.rs`、Demo* 适配器） | ≈10 | `DEMO_AUTHOR_IDS` 与 `snowflake_id` 改为 `demo_object_id(seq)` / `from_parts(ts, seq)`，与 phoenix 网关合成语料同一编码 |
| 测试字面量 | ≈161 + `tests/p4` 的 `[30_u64, 20_u64]` | `pid(n)` / `uid(n)` 助手（基于 `#[cfg(test)] From<u64>`） |
| `GrpcThunderClient` / `GrpcVMRankerClient` | 2 个适配器 | 无法无损转 u64；先置于 cargo feature 之后，crate 留 workspace 不部署；P3 把 `in_network.proto` / `vm_ranker.proto` 改 string 并改 PostStore / embedding_store 键 |

### 6.5 迁移顺序与验收

先改 `models/{candidate, query, user_features, candidate_features}.rs` 与 `home_mixer.proto`，由编译器按 clients → hydrators / sources → filters → scorers → servers → tests 列清单。整个 P1 只改类型不改行为：同一份演示数据（旧 Snowflake ID 与新零填充 ObjectId 一一对应，Demo TES 填入与原 Snowflake 时间戳一致的 `created_at_ms`）下，新旧两个构建的排序输出应一致。

---

## 7. mp-slim 成果的落点

| mp-slim 的东西 | 落到主干哪里 | 差异类 |
| --- | --- | --- |
| `phoenix_recsys.proto`（string ID）+ `phoenix/` 提交 | cherry-pick（见 P0）；proto 文件名沿用以避开 Python descriptor 撞名 | U0 / U1 |
| PhoenixRanker 的 metadata + NaN / 重复 / 缺失校验 | `SlimPhoenixPredictionClient` 内部，校验失败返回 `Err` | U1 |
| 整批规则回退 `ranking::fallback()` | `RuleFallbackScorer`，装配在 `RankingScorer` 之后，读 PhoenixScorer 失败标记 | U2 |
| `/recommendation/eligibility` fail-closed | `MrpyqVisibilityFilteringClient`：缺项 → Drop、超时 → Unavailable；`VFFilter` 是否把 fail-closed 扩到网内做成本地参数，默认沿用全量拒绝 | U1 / U2 |
| `/recommendation/input`（history / seen / candidates 一次返回） | 拆为三个端口实现：`UserActionSequenceOps` / `ImpressedPostsClient` / `InNetworkPostsClient` | U1 |
| served 成功才 2xx | `scored_posts_server.rs` 中 `execute()` 之后、响应之前的同步步骤（框架 SideEffect 是 fire-and-forget，不满足训练归因） | U2 |
| `/v1/feedback` 幂等落库 | 独立 RPC（或 HTTP 薄层）→ 业务适配器 | U2 |
| Bearer + `X-Viewer-Account-Id` 双校验 | tonic interceptor 或 HTTP 薄层 | U1 |
| cursor 会话分页 | 不搬；上游语义为客户端回传 `seen_ids` / `served_ids` / `is_bottom_request` 重跑；有性能指标需要再作为 U2 加回 | — |
| `fixture_backend.py` + `run_recommendation_demo.sh` | 演化为 Demo* 适配器的数据源（Demo 端口改生成 ObjectId 形状 ID），或保留为外部 fixture | Demo |
| `recommendation-service/` crate | 吸收完毕后删除 | — |

---

## 8. 模型引擎策略

### 8.1 现阶段：本地移植的小型 Transformer

演示网关（`phoenix/services/grpc_gateway.py`）+ `data_preprocessor.py` + mp-slim 的训练侧改造，接真实行为日志，先证明"模型 > 规则"。效果的主要杠杆是数据与标签（以曝光为单位的样本、只训练可观测行为头、负采样窗口、同候选集规则基线对照），两套引擎都依赖这一层。

### 8.2 保留切换 xrex 的四项前置动作（合计约 1–2 人日）

1. 定死 §6.3 的派生函数并落黄金向量双端测试。
2. 引擎选择放在装配层：`PHOENIX_ENGINE=slim|xrex` 决定注入哪个 `PhoenixPredictionClient` / `PhoenixRetrievalClient` 实现；现在只有 `slim`。
3. Phoenix 元数据 / 响应校验只存在于适配器内部，`PhoenixScorer` 只看到 `Result`。
4. 归档原始 served + feedback 事件（保留位置、时间戳、动作类型）作为唯一原始数据源；`data_preprocessor.py` 与将来的 xrex dump 转换器都是它的导出器。

### 8.3 三条不倒退纪律

- 历史序列长度不在流水线写死：`UserActionSeqQueryHydrator` 按上游 7 天 / 300 条聚合，适配器再截到引擎长度（演示 32，xrex 1022）。
- 行为词表只认 `ActionName` 枚举：权重表、训练 `supported-actions`、`top_log_probs` 下标到 `PhoenixScores` 的映射均以枚举为键。
- 流水线内任何 Phoenix 相关逻辑只看 `PostId`，不依赖 ID 是字符串或整数。

### 8.4 切换时的工作（有 Linux + NVIDIA GPU 后，约 1–1.5 周，流水线零改动）

复现 `phoenix/QUICKSTART.md` 合成流程（1–2 天）；`XrexPhoenixPredictionClient` / `XrexPhoenixRetrievalClient`（编译 `xai.recsys.v1` proto，适配器内 `to_u64_hash()` + 请求局部反查，等价版本校验；1–2 天）；原始事件 → Kafka dump parquet 转换器，关闭 SID / 多模态 / 画像列（1–2 天）；`eval_ranker.py` 同一评估集对比 AUC / NDCG 后决定切流（1 天）。不提前编入 `xai.recsys.v1` proto，不做双引擎兼容层，不提前引入 SID / 多模态 / 画像字段；两套引擎不共用 checkpoint，"随时切换"指两个适配器、两个端点、各自训练、并行对比后切流。

xrex 未验证点：`use_post_sid=False` 后召回塔回到纯 item hash 的路径，上游没有专门验证；内核为 Hopper / Blackwell 调优，其他 GPU 需调 `attn_impl`。本机（macOS）只验证过 `phoenix/` Rust workspace 116 项测试、`xai-recsys-engine` pyo3 绑定构建与 `xrex` import 链，未跑过训练与服务。

---

## 9. 实施批次

| 批次 | 内容 | 验收 |
| --- | --- | --- |
| **P0 底座** | 从 `mp`（`0300a09`）拉主干分支；cherry-pick mp-slim 的 phoenix-only 提交 `6395950 7f008f8 1f14e48 6a9ad3d 8f03bae 4452fa2 266ae3b a7ebe0e 2e3e1fc 370a03f 9568373 880ebd4`；`4ff17d1`（proto 改名）与 `25f4f7c`（ID 改 string）涉及 `proto/build.rs`，需手工合并并保留 `mp` 的其余 4 个 proto 文件；在 `upstream-first-maintenance.md` 增加 U4 / U5；本文落库；ID 方案定为 A | `cargo test --workspace` 354 项通过；`cd phoenix && uv run pytest` 通过；`./scripts/run_demo.sh` 可跑 |
| **P1 ID 替换（U4）** | 新建 `ids.rs`；`model_contract.py` 加派生函数与黄金向量；改 models 与 `home_mixer.proto`；`created_at_ms` 字段 + AgeFilter；`demo.rs` 改 ObjectId；Bloom 改字节输入；thunder / vm-ranker 两个 gRPC 适配器加 feature gate；测试字面量改 `pid(n)` / `uid(n)`。只改类型不改行为 | 318 单测迁移通过；demo-client 端到端 ObjectId 进出；与 P0 相同演示数据下排序一致 |
| **P2 业务适配器 + 契约落位 + U5 删除** | §5 的 U1 适配器清单；`InNetworkPostsClient` trait 抽取；`FallbackSource`、`FirstStageEligibleFilter`、`RuleFallbackScorer`、ServedPersist、feedback RPC、引擎选择配置；删 Quote / Subscription Hydrator、SubscribedUserIds QH、RetweetDedup / IneligibleSubscription / AncillaryVF Filter、`business_feed/`、`recommendation-service/`；`params/param.rs` 中 retweet / quote / quoted_* 权重设 0；`production_ready` 拒绝条件改为"业务适配器契约未验证" | 真实 fixture 端到端；fail-closed、整批回退、served 阻塞落库各有测试；`cargo clippy --workspace --all-targets -- -D warnings` |
| **P3 追上游与可选部件** | 按 sync procedure 处理 `49815da → 75d93d9 → fee1d0f → 6bb4594`，实测 U4 移植摩擦；按需 `in_network.proto` / `vm_ranker.proto` 改 string 并部署 Thunder / VM Ranker；话题（tag_ids）；AuthorColdStart（需业务曝光数）；多副本共享 served / session 存储 | 四份新的 capability inventory，锚点前移到 `6bb4594`；由容量 / 延迟 / 重复曝光指标触发部件启用 |

---

## 10. 待业务侧确认

1. 业务后端接口形态：gRPC `RecommendationDataService`（`mp` 已有客户端）还是 mp-slim 的 HTTP JSON 契约？eligibility / history / served / feedback 四个能力以哪种方式提供？只保留一套。
2. 评论是否会作为 Feed 条目出现？否 → 回复相关字段与 `DedupConversation` 永远无操作。
3. 产品是否有静音关键词功能？（现有 tokenizer 面向英文推文，中文需另评估）
4. 线上真有埋点的行为头有哪些（点赞 / 评论 / 点击 / 分享 / 关注 / 停留 / 不感兴趣 / 拉黑 / 静音 / 举报）？决定 `params` 权重表与训练 `supported-actions`；打赏不是 Phoenix 头，只能进规则分。
5. 客户端能否按上游语义回传 `seen_ids` / `served_ids` / `is_bottom_request`？决定是否需要把 cursor 会话作为 U2 加回。
6. 帖龄窗口：`mp` 的 48h 还是 mp-slim 的 7d？AgeFilter 与负样本采样窗口必须一致。
7. xrex 是否在路线图上、何时有 Linux + GPU 机器？决定 §8.2 第 1 项的优先级。

---

## 11. 代价与风险

- ID 替换是一次性大工程（69 个文件、约 161 处测试字面量），建议单独一个 PR，只改类型不改行为。
- 此后每次上游移植多一步机械替换（实测每快照 0–16 行，约 1%），编译器兜底。
- P2 之前主干仍跑在 Demo 适配器上，是"能跑的骨架"而非可上线系统；`production_ready` 的拒绝启动规则保留到适配器验收通过。
- 分页语义变化：客户端需回传 `seen_ids` / `served_ids` / `is_bottom_request`。
- 为了可同步，磁盘上保留一批不会打开的槽位（ads 约 1.7k 行、topics、MoE、CachedPosts、Kafka 端口）。
- 不得用 `--allow-random-model`、跳过元数据校验或绕过 eligibility 适配器来假装接入完成（沿用 mp-slim 的停止条件）。

---

## 12. 参考

- 上游维护制度：`docs/upstream-sync/upstream-first-maintenance.md`（`mp` 分支）
- mp-slim 执行计划：`docs/implementation/slim-phoenix-recommendation.md`
- MVP 范围评估：`docs/recommendation-service-mvp-assessment.md`（`mp` 分支）
- 主干装配点：`home-mixer/candidate_pipeline/phoenix_candidate_pipeline.rs`（`mp` 分支）
- 业务数据协议：`proto/definitions/recommendation_data.proto`（`mp` 分支）
- Phoenix 演示网关与训练契约：`phoenix/services/grpc_gateway.py`、`phoenix/services/model_contract.py`、`phoenix/data_preprocessor.py`
- xrex 生产栈：`phoenix/README.md`、`phoenix/QUICKSTART.md`、`phoenix/xrex/configs/xrecsys.py`
