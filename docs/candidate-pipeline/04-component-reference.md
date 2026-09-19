# 组件清单与职责对照

> 本篇只保留框架视角下最重要的结论：组件之间的依赖链。逐组件字段和 enable 条件以当前 Rust 装配代码为准，不再维护一份容易过期的平行索引。

## 组件依赖链

`PhoenixCandidatePipeline` 里存在几条关键依赖链。由于同一 stage 的 hydrator 并行执行、彼此看不到本轮新写入的字段（语义见 [02-execution-semantics](./02-execution-semantics.md)），跨 stage 的先后关系是调整装配顺序时必须核对的约束：

1. `RetrievalSequenceQueryHydrator`（底层共享 `UserActionSeqQueryHydrator` provider） -> `PhoenixSource`
2. `ScoringSequenceQueryHydrator`（同一 provider） -> `PhoenixScorer`
3. `FollowedUserIdsQueryHydrator`（底层共享 `UserFeaturesQueryHydrator` provider） -> `InNetworkCandidateHydrator`（仅对来源未标 `in_network` 的候选；`ThunderSource` / `FallbackSource` 在来源处已标定）
4. `CoreDataCandidateHydrator` -> `CoreDataHydrationFilter` / `FirstStageEligibleFilter` / `AgeFilter`（`created_at_ms`） / `ViewerMutedKeywordFilter`（正文）
5. `VideoDurationCandidateHydrator` -> `RankingScorer`（内部 VQV 时长权重） / `VideoFilter`
6. `InNetworkCandidateHydrator` -> `RankingScorer`（内部 OON 调整） / `RuleFallbackScorer` / `VFCandidateHydrator`
7. `PhoenixScorer`（写 `phoenix_scores` 或 `degraded_reason`） -> `RankingScorer` -> `RuleFallbackScorer`（批内有候选缺 Phoenix 头时整批覆盖）
8. `VFCandidateHydrator` -> `VFFilter`
9. `ServedHistoryQueryHydrator`（读 `FeedStateStore`） -> `PreviouslyServedPostsFilter`；响应前 `ServedPersistence::persist` 再写回同一 store

引用 / 订阅相关的 `QuoteHydrator`、`RetweetDeduplicationFilter` 等已按 U5 删除，不再出现在依赖链里。

## 两个容易踩的实现事实

- `CoreDataCandidateHydrator` 的 `update()` 只在 `candidate.author_id.is_nil()`（source 未填作者，例如 mrpyq 候选只带 `feed_id`）且 TES 返回了作者时才写回 `candidate.author_id`；已填的作者 ID 不会被覆盖。同理 `created_at_ms` / `recommendation_eligible` 也只在候选尚无值时写回。
- 同 stage Hydrator 仍不能依赖彼此写回。当前 `GizmoduckCandidateHydrator` 已移到 post-selection，所以能读取 pre-selection CoreData 写入的 `retweeted_user_id`；后续字段依赖也必须用跨 stage 或共享 provider 表达。注意 demo 且开启 `author_cold_start` 时，装配层会额外向 pre-selection hydrators 注入一个 `GizmoduckCandidateHydrator` 实例（用于探索前补作者粉丝数）。
