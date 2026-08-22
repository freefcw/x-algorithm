# 组件清单与职责对照

> 本篇原来维护了一份与 [home-mixer 组件索引](../home-mixer/08-component-index.md) 几乎相同的组件读写字段表，两处并行维护容易漂移。现在**逐组件的权威对照表（含文件路径、enable 条件、读写字段、外部依赖）统一以 [../home-mixer/08-component-index.md](../home-mixer/08-component-index.md) 为准**，本篇只保留框架视角下最重要的结论：组件之间的依赖链。

## 组件依赖链

`PhoenixCandidatePipeline` 里存在几条关键依赖链。由于同一 stage 的 hydrator 并行执行、彼此看不到本轮新写入的字段（语义见 [02-execution-semantics](./02-execution-semantics.md)），跨 stage 的先后关系是调整装配顺序时必须核对的约束：

1. `RetrievalSequenceQueryHydrator`（底层共享 `UserActionSeqQueryHydrator` provider） -> `PhoenixSource`
2. `ScoringSequenceQueryHydrator`（同一 provider） -> `PhoenixScorer`
3. `FollowedUserIdsQueryHydrator`（底层共享 `UserFeaturesQueryHydrator` provider） -> `ThunderSource` / `InNetworkCandidateHydrator`
4. `CoreDataCandidateHydrator` -> `CoreDataHydrationFilter` / `RetweetDeduplicationFilter`
5. `QuoteHydrator` -> `ViewerMutedKeywordFilter`（引用文）
6. `VideoDurationCandidateHydrator` -> `RankingScorer`（内部 VQV 时长权重） / `VideoFilter`
7. `InNetworkCandidateHydrator` -> `RankingScorer`（内部 OON 调整） / `VFCandidateHydrator`
8. `VFCandidateHydrator` -> `VFFilter`

## 两个容易踩的实现事实

- `CoreDataCandidateHydrator` 的 `update()` 只在 `candidate.author_id == 0`（source 未填作者）且 TES 返回了作者时才写回 `candidate.author_id`；已填的作者 ID 不会被覆盖。
- 同 stage Hydrator 仍不能依赖彼此写回。当前 `GizmoduckCandidateHydrator` 已移到 post-selection，所以能读取 pre-selection CoreData 写入的 `retweeted_user_id`；后续字段依赖也必须用跨 stage 或共享 provider 表达。注意 demo 且开启 `author_cold_start` 时，装配层会额外向 pre-selection hydrators 注入一个 `GizmoduckCandidateHydrator` 实例（用于探索前补作者粉丝数）。
