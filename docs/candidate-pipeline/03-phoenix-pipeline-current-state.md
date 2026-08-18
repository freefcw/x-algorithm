# PhoenixCandidatePipeline 当前实现剖析

本篇聚焦 `home-mixer/` 对 Candidate Pipeline 的实际使用。框架执行语义见 [02-execution-semantics](./02-execution-semantics.md)，生产风险见 [Home Mixer 风险与路线](../home-mixer/06-current-behavior-risks-roadmap.md)。

## 1. 请求入口和对象

gRPC trait 实现在 `home-mixer/server.rs`，公共 proto 到 domain query 的校验与映射在 `home-mixer/query_builder.rs`：

1. 校验 `viewer_id > 0` 并做 checked ID conversion。
2. 在 200 ms 内读取 viewer policy；只有明确 Allow 才开放网外推荐。
3. 生成 request ID、prediction ID 和 request time。
4. 构造 `ScoredPostsQuery` 并调用 `PhoenixCandidatePipeline::execute()`。
5. Application server 将 `selected_candidates` 映射回响应。

`ScoredPostsQuery` 同时承载请求字段、查询补全字段、feature policy、缓存 fixture 和请求身份。`PostCandidate` 是阶段间共享状态，主要包含：

- 身份与关系：`tweet_id`、`author_id`、reply/retweet 关系、`ancestors`
- 内容与派生：文本、媒体、语言、screen name、`in_network`
- 排序：`phoenix_scores`、`weighted_score`、`score`
- 安全：`visibility_decision`、`drop_ancillary_posts`
- 来源与追踪：`served_type`、prediction ID、last scored time

## 2. 当前装配入口

`HomeMixerServer::build(config)` 把 `HomeMixerMode` 和 typed features 直接传给 `PhoenixCandidatePipeline::assemble_for_mode()`。Pipeline 不再重新读取环境变量选择依赖。

`prod()`、`prod_with_features()`、`prod_with_topic_clients()` 仅保留为上游兼容 facade；新的 application 代码应使用显式 mode 装配。

`PhoenixDependencies` 以具名字段持有 UAS、Phoenix、Thunder、Strato、TES、Gizmoduck、VF、Topic/MoE 和 feature policy，避免位置参数错配。

## 3. 真实阶段顺序

### 3.1 Query Hydrators

1. `ScoringSequenceQueryHydrator`
2. `RetrievalSequenceQueryHydrator`
3. `BlockedUserIdsQueryHydrator`
4. `MutedUserIdsQueryHydrator`
5. `FollowedUserIdsQueryHydrator`
6. `SubscribedUserIdsQueryHydrator`
7. `UserSafetyFeaturesQueryHydrator`
8. `UserTopicsQueryHydrator`（显式 Topic clients 或 Demo）

UAS 与 Strato 各使用一个 request-scoped provider；多个字段 owner 共享同一次读取，但不同用户/请求绝不复用结果。

### 3.2 Sources

1. `ThunderSource`
2. `PhoenixSource`
3. `PhoenixTopicsSource`（可选）
4. `PhoenixMoeSource`（可选）
5. `CachedPostsSource`

`CachedPostsSource` 只接受 QueryBuilder 已批准的显式 Demo unsigned fixture。普通请求默认拒绝携带完整缓存候选。

### 3.3 Pre-selection Hydrators

1. `InNetworkCandidateHydrator`
2. `CoreDataCandidateHydrator`
3. `QuoteHydrator`
4. `VideoDurationCandidateHydrator`
5. `HasMediaHydrator`
6. `SubscriptionHydrator`
7. `FilteredTopicsHydrator`
8. `LanguageCodeHydrator`

TES 相关 hydrator 共享一个 request-scoped `TesHydrationProvider`，避免重复 core/media batch。

### 3.4 Pre-selection Filters

1. `DropDuplicatesFilter`
2. `CoreDataHydrationFilter`
3. `AgeFilter`
4. `SelfTweetFilter`
5. `RetweetDeduplicationFilter`
6. `IneligibleSubscriptionFilter`
7. `PreviouslySeenPostsFilter`
8. `PreviouslySeenPostsBackupFilter`
9. `PreviouslyServedPostsFilter`
10. `MutedKeywordFilter`
11. `AuthorSocialgraphFilter`
12. `VideoFilter`
13. `TopicIdsFilter`
14. `NewUserTopicIdsFilter`

### 3.5 Ranking and selection

1. `PhoenixScorer`：读取 Phoenix 行为概率；5 s timeout，失败保留候选。
2. `RankingScorer`：在上游命名边界内执行 Weighted、Author Diversity 和 OON 行为。
3. 可选 `VMRanker`、`AuthorColdStartScorer`（都要显式开开关）。
4. `TopKScoreSelector`：保留 post-selection 前 Top 50。

### 3.6 Post-selection

- Hydrators：`GizmoduckCandidateHydrator`、`VFCandidateHydrator`，两者并行且互不依赖。
- Filters：`VFFilter`、`AncillaryVFFilter`、`DedupConversationFilter`。
- 结果：最多返回 35 条（上游 `RESULT_SIZE`）；当前不会在 post-selection 删除后从未选候选回补。

### 3.7 Side Effect

`PhoenixRequestCacheSideEffect` 默认关闭。启用需要真实持久化合同；执行为 fire-and-forget，但成功、失败和耗时都有 request-scoped 日志。

## 4. 运行模式和依赖成熟度

| 依赖 | Demo | Degraded | 关键行为 |
| --- | --- | --- | --- |
| UAS | `DemoUserActionSequenceFetcher` | `DisabledUserActionSequenceFetcher` | 空序列会关闭 Phoenix 个性化输入 |
| Strato | `DemoStratoClient` | `DisabledStratoClient` | Degraded 返回空特征；两者都拒绝未配置的持久化写入 |
| TES | `DemoTESClient` | `DisabledTESClient` | Degraded 缺少 core data，候选可能被过滤 |
| Gizmoduck | `DemoGizmoduckClient` | `DisabledGizmoduckClient` | 未知 viewer policy 强制仅网内 |
| VF | `DemoVisibilityFilteringClient` | `DisabledVisibilityFilteringClient` | Unavailable 时删除网外、保留网内；附属内容保守删除 |
| Thunder | 真实简化 gRPC client | 同左 | 500 ms timeout，seen IDs 下推 |
| Phoenix retrieval | 配置地址后真实 gRPC | 同左 | 标准/MoE 调用上限 3 s |
| Phoenix prediction | 配置地址后真实 gRPC | 同左 | 调用上限 5 s，失败走 fallback ranking |
| Topic | Demo adapter | 需显式注入 | Topic retrieval 上限 500 ms |

`production_ready` 当前拒绝启动，直到 Viewer、TES、Gizmoduck、VF、调用方身份和持久化责任等生产合同闭合。

## 5. 关键执行约束

同一 stage 的 Hydrator 并行读取同一份旧候选快照，不能看到本轮其他 Hydrator 的写入。当前装配把 `GizmoduckCandidateHydrator` 放在 post-selection，因此它能看到 pre-selection CoreData 已补出的 `retweeted_user_id`，同时只查询 Top 50 候选；VF 与 Gizmoduck 在 post-selection 内彼此独立。需要新增字段依赖时，应使用后续 stage 或合并 provider，而不是依赖装配顺序。

Source 并行、Filter 串行、Scorer 串行、SideEffect 异步。每个阶段的逐候选错误被隔离并记录，长度不匹配由框架保护，避免部分返回错误地对应到其他候选。

## 6. 当前定位

当前 Pipeline 是一条可运行、可测试、边界接近上游的 portable 编排实现：Demo 可完整演示，Degraded 明确保守退化，但不是生产完成声明。生产接入必须以真实合同、认证、deadline、失败策略和真实 artifact 验收为依据，不能通过改名或打开 feature switch 宣布完成。
