# PhoenixCandidatePipeline 当前实现剖析

本篇聚焦 `home-mixer/` 对 Candidate Pipeline 的实际使用。框架执行语义见 [02-execution-semantics](./02-execution-semantics.md)，生产风险见 [Home Mixer 风险与路线](../home-mixer/06-current-behavior-risks-roadmap.md)。

## 1. 请求入口和对象

gRPC trait 实现在 `home-mixer/server.rs`，公共 proto 到 domain query 的校验与映射在 `home-mixer/query_builder.rs`：

1. 校验 `viewer_id` 是非空、非 NIL 的 24 位小写 hex ObjectId，并把 `seen_ids` / `served_ids` / `impressed_post_ids` 解析为 `PostId`（非法串丢弃并计数）。
2. 在 200 ms 内读取 viewer policy；只有明确 Allow 才开放网外推荐（非 demo 的 `DisabledGizmoduckClient` 恒返回 Unknown，因此当前非 demo 请求全部仅网内）。
3. 生成 request ID、prediction ID 和 request time。
4. 构造 `ScoredPostsQuery` 并调用 `PhoenixCandidatePipeline::execute()`。
5. Application server 将 `selected_candidates` 映射回响应。

`ScoredPostsQuery` 同时承载请求字段、查询补全字段、feature policy、缓存 fixture 和请求身份。`PostCandidate` 是阶段间共享状态，主要包含：

- 身份与关系：`tweet_id`（`PostId`）、`author_id`（`UserId`）、reply/retweet 关系、`ancestors`
- 内容与派生：文本、`created_at_ms`、`recommendation_eligible`、媒体、语言、screen name、`in_network`
- 排序：`phoenix_scores`、`weighted_score`、`score`、`degraded_reason`
- 安全：`visibility_decision`、`visibility_action`
- 来源与追踪：`served_type`、prediction ID、last scored time

## 2. 当前装配入口

`HomeMixerServer::build(config)` 把 `HomeMixerMode` 和 typed features 直接传给 `PhoenixCandidatePipeline::assemble_for_mode()`。Demo/Disabled adapter 不再由 pipeline 自己读 `HOME_MIXER_MODE`；装配层读 `MRPYQ_RECOMMENDATION_DATA_ADDR`（非 demo 必填，缺失或不可用则启动失败）、`PHOENIX_*_GRPC_ADDR`，旁路读 `VM_RANKER_GRPC_ADDR` / `PHOENIX_MOE_GRPC_ADDR`。

`prod()`、`prod_with_features()`、`prod_with_topic_clients()` 仅保留为上游兼容 facade；新的 application 代码应使用显式 mode 装配。

`PhoenixDependencies` 以具名字段持有 UAS、Phoenix 预测 / 召回、`InNetworkPostsClient`（网内）、兜底客户端、Strato、TES、Gizmoduck、VF、Topic/MoE 和 feature policy，避免位置参数错配。`ScoredPostsServer::with_state` 随后把 `ServedHistoryQueryHydrator` / `PastRequestTimestampsQueryHydrator` 插到 query hydrator 列表首位，并持有同一 `FeedStateStore` 供响应前的 served 落库使用。

## 3. 真实阶段顺序

### 3.1 Query Hydrators

1. `ServedHistoryQueryHydrator`（由 `ScoredPostsServer::with_state` 插入首位）
2. `PastRequestTimestampsQueryHydrator`（同上，第二位）
3. `ScoringSequenceQueryHydrator`
4. `RetrievalSequenceQueryHydrator`
5. `BlockedUserIdsQueryHydrator`
6. `MutedUserIdsQueryHydrator`
7. `FollowedUserIdsQueryHydrator`
8. `UserSafetyFeaturesQueryHydrator`
9. `UserTopicsQueryHydrator`（显式 Topic clients 或 Demo）

UAS 与 Strato 各使用一个 request-scoped provider；多个字段 owner 共享同一次读取，但不同用户/请求绝不复用结果。Query hydrator 失败只记日志、不中断请求。

### 3.2 Sources

1. `ThunderSource`（依赖 `InNetworkPostsClient`：非 demo 为 mrpyq NETWORK 收件箱，demo 为整数 Thunder；两者都不可用时不装配）
2. `PhoenixSource`
3. `FallbackSource`（非 demo 为 mrpyq FALLBACK 池，demo 为 `DemoFallbackPostsClient`）
4. `PhoenixTopicsSource`（可选）
5. `PhoenixMoeSource`（可选）
6. `CachedPostsSource`

`PhoenixSource` 与 `FallbackSource` 都要求 `!in_network_only`；非 demo 下 viewer 资格未知会让二者都不启用。`CachedPostsSource` 只接受 QueryBuilder 已批准的显式 Demo unsigned fixture。普通请求默认拒绝携带完整缓存候选。

### 3.3 Pre-selection Hydrators

1. `InNetworkCandidateHydrator`（来源已标 `in_network` 时原样保留）
2. `CoreDataCandidateHydrator`
3. `VideoDurationCandidateHydrator`
4. `HasMediaHydrator`
5. `FilteredTopicsHydrator`
6. `LanguageCodeHydrator`
7. 仅 demo 且 `HOME_MIXER_ENABLE_AUTHOR_COLD_START` 时再加 `GizmoduckCandidateHydrator`

TES 相关 hydrator 里 CoreData / VideoDuration / HasMedia / FilteredTopics / LanguageCode 共享一个 request-scoped `TesHydrationProvider`；非 demo 的 `MrpyqTESClient` 又与 VF 端口共享一份内容缓存，同一批候选只打一次 mrpyq RPC。`QuoteHydrator` / `SubscriptionHydrator` 已按 U5 删除。

### 3.4 Pre-selection Filters

1. `DropDuplicatesFilter`
2. `CoreDataHydrationFilter`
3. `FirstStageEligibleFilter`（只丢 `recommendation_eligible == Some(false)`）
4. `AgeFilter`（读 `created_at_ms`，缺失回退 ObjectId 时间戳）
5. `SelfTweetFilter`
6. `PreviouslySeenPostsFilter`
7. `PreviouslySeenPostsBackupFilter`
8. `PreviouslyServedPostsFilter`
9. `ViewerMutedKeywordFilter`
10. `AuthorSocialgraphFilter`
11. `VideoFilter`
12. `TopicIdsFilter`
13. `NewUserTopicIdsFilter`

`RetweetDeduplicationFilter` / `IneligibleSubscriptionFilter` 已按 U5 删除。

### 3.5 Ranking and selection

1. `PhoenixScorer`：读取 Phoenix 行为概率；5 s timeout。无行为序列、超时、失败或适配器内契约校验不通过时保留候选并整批写 `degraded_reason`。
2. `RankingScorer`：在上游命名边界内执行 Weighted、Author Diversity 和 OON 行为。
3. `RuleFallbackScorer`：批内任一候选缺可用 Phoenix 头时，用“新鲜度 + 网内 + 互动数 × 作者多样性”的规则分覆盖整批。
4. 可选 `VMRanker`、`AuthorColdStartScorer`（仅 demo，且要显式开开关）。
5. `TopKScoreSelector`：保留 post-selection 前 Top 50。

### 3.6 Post-selection

- Hydrators：`GizmoduckCandidateHydrator`、`VFCandidateHydrator`，两者并行且互不依赖。
- Filters：`VFFilter`（`Unchecked / Unavailable` 按 `HOME_MIXER_VF_FAILURE_POLICY`，默认 `fail_closed` 删除）、`DedupConversationFilter`。`AncillaryVFFilter` 已按 U5 删除。
- 结果：最多返回 35 条（上游 `RESULT_SIZE`）；当前不会在 post-selection 删除后从未选候选回补。
- 服务层：响应前同步调用 `ServedPersistence::persist`，失败返回 `Unavailable`；当前实现为进程内存。

### 3.7 Side Effect

`PhoenixRequestCacheSideEffect` 默认关闭。启用需要真实持久化合同；执行为 fire-and-forget，但成功、失败和耗时都有 request-scoped 日志。

## 4. 运行模式和依赖成熟度

| 依赖 | Demo | Degraded（需 `MRPYQ_RECOMMENDATION_DATA_ADDR`） | 关键行为 |
| --- | --- | --- | --- |
| UAS | `DemoUserActionSequenceFetcher` | `DisabledUserActionSequenceFetcher` | 空序列让 `PhoenixSource` 不可用、`PhoenixScorer` 整批 `phoenix_missing_sequence`，所有请求由 `RuleFallbackScorer` 排序 |
| Strato | `DemoStratoClient` | `MrpyqStratoClient`（mrpyq `ViewerRelationService`） | 后端尚未实现，调用失败只记日志，`user_features` 全空；两者都拒绝持久化写入 |
| TES | `DemoTESClient` | `MrpyqTESClient`（mrpyq `BatchGetRecommendationContents`） | 补作者 / 正文 / `created_at_ms` / 互动数 / 一级 eligibility；`creator_member_id` 为空的帖子被 `CoreDataHydrationFilter` 丢弃 |
| Gizmoduck（QueryBuilder viewer） | `DemoGizmoduckClient`（Allow 网外） | `DisabledGizmoduckClient`（未知 → 仅网内） | 非 demo 因此永不启用 `PhoenixSource` / `FallbackSource`；作者资料 hydrator 也用 Disabled，`screen_names` 为空 |
| VF | `DemoVisibilityFilteringClient`（Allow） | `MrpyqFirstStageEligibilityClient` | 非 demo 只承载一级 `recommendation_eligible`，无 viewer 级判定；`Unchecked / Unavailable`（含成功响应缺帖）按 `HOME_MIXER_VF_FAILURE_POLICY`，默认 `fail_closed` 删除 |
| 网内 / 兜底召回 | `ThunderClient`（整数 Thunder，`legacy-int-ids`）+ `DemoFallbackPostsClient` | `MrpyqInNetworkPostsClient`（NETWORK / FALLBACK） | mrpyq 单次 RPC 500 ms，一次召回总预算 1500 ms；以皮 `member_id` 作为 `account_id` 查询，皮维度对齐待 mrpyq 落地 |
| Phoenix retrieval | 配置地址后真实 gRPC | 同左，且拒绝随机权重 | 标准/MoE 调用上限 3 s |
| Phoenix prediction | 配置地址后真实 gRPC | 同左，且拒绝随机权重 | 调用上限 5 s，失败或校验不通过走 `RuleFallbackScorer` |
| served 落库 | `InMemoryServedPersistence` | 同左 | 进程内存，重启即丢 |
| Topic | Demo adapter | 需显式注入 | Topic retrieval 上限 500 ms |

`production_ready` 当前拒绝启动，直到调用方身份、TES、UAS、Strato、VF、网内 / 兜底、Phoenix 元数据、served 落库等生产合同验收闭合。

## 5. 关键执行约束

同一 stage 的 Hydrator 并行读取同一份旧候选快照，不能看到本轮其他 Hydrator 的写入。当前装配把 `GizmoduckCandidateHydrator` 放在 post-selection，因此它能看到 pre-selection CoreData 已补出的 `retweeted_user_id`，同时只查询 Top 50 候选；VF 与 Gizmoduck 在 post-selection 内彼此独立。需要新增字段依赖时，应使用后续 stage 或合并 provider，而不是依赖装配顺序。

Source 并行、Filter 串行、Scorer 串行、SideEffect 异步。每个阶段的逐候选错误被隔离并记录，长度不匹配由框架保护，避免部分返回错误地对应到其他候选。

## 6. 当前定位

当前 Pipeline 是一条可运行、可测试、边界接近上游的 portable 编排实现：Demo 可完整演示，Degraded 明确保守退化，但不是生产完成声明。生产接入必须以真实合同、认证、deadline、失败策略和真实 artifact 验收为依据，不能通过改名或打开 feature switch 宣布完成。
