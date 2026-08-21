# 提交 11a71f8 能力清点与 U0–U3 分类（2026-08-18 上游快照）

> 文档状态：清点完成；本提交无可落地项，全部为既有 U3 线路的语义迭代
> 上游提交：`11a71f87d6a7fc4c1e8159dad8f3c5ff90a0f7ed`（2026-08-18）
> 上游父提交（当前已吸收锚点）：`b089ce64891f9c50fab73aa00dbe65acb82f198f`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)
> 上一清点：[`b089ce6-capability-inventory.md`](./b089ce6-capability-inventory.md)

## 1. 提交概览

14 个文件，+208/-70。三组内容：

1. **Following feed 可见性过滤接入**（home-mixer + visibility-filtering）：新 hydrator 替换 reverse-chron 管线的 post-selection VF。
2. **既有 U3 线路的语义迭代**：品牌安全 verdict V2 修正回落语义；cold-start tracked-ids 计数口径修正；vm_ranker 映射 SID slate 字段。
3. **grox 迭代**：reply_spam 生成器收敛为单一 v3 topic 生成器 + 粉丝阈值上调；ptos 交叉验证 metric 口径细化。

## 2. 能力清单与分类

### V. visibility-filtering 线路（U3，既定不采用决策覆盖）

| 编号 | 能力 | 说明 |
|---|---|---|
| V1 | 新增 `vf_following_candidate_hydrator.rs`（95 行）：Following feed 的 post-selection VF，取 `GetTwitterContextViewer` 上下文、`TimelineHome` 安全级，复用 `should_drop_ancillary` 丢弃附属帖 | **U3**。既定决策（`e414c17` 台账 P1.4，2026-08-14）：不引入 visibility filtering，阻塞点是数据源合同而非规则代码——58 条规则的标签补水层完全建在私有存储上，只移植规则会把今天的 fail-closed 桩变成 fail-open。本项同属该阻塞 |
| V2 | `reverse_chron_posts_pipeline` 将 post-selection hydrator 从 `VFCandidateHydrator` 换为 `VFFollowingCandidateHydrator` | **U3**。依附 V1 |
| V3 | `vf_candidate_hydrator::should_drop_ancillary` 改 `pub(crate)` | **U3**。仅为 V1 复用服务；本地 `vf_candidate_hydrator.rs` 是 fail-closed 重写版，无此调用方 |
| V4 | `visibility-filtering/server_deps.rs`：Gizmoduck strato 客户端增加 80ms 请求超时 | **U3**。本地无该服务；语义记录：VF 服务对 Gizmoduck 的依赖加了硬超时 |
| V5 | `visibility-filtering` 两个 strato processor（postCreationEvent 处理/转发） | **U3**。同上 |

### I. 既有 U3 线路的语义迭代（更新记录，不落地）

| 编号 | 能力 | 说明 |
|---|---|---|
| I1 | `compute_verdict_v2` 修正：**无 V2-written 标签（`GROK_SFA_V2`/`GROK_NSFA_V2`/`GROK_NSFA_LIMITED_V2`/`GROK_NSFA_EXPANDED_V2`）时整体回落 v1 判定**；`NSFA_HIGH_PRECISION` 移出 MEDIUM_V2、`NSFA_LIMITED_INVENTORY` 移出 LOW_V2；删除 b089ce6 引入的 `strip_v1_grok_written` | **U3**（b089ce6 A1 线路）。**重入时以本版本为最终语义**：v2 不再剥离 v1 双写标签，而是"V2 未裁决则 v1 说了算"；v1/v2 分歧（v1 SFA + NSFA_V2）判 MediumRisk |
| I2 | `author_cold_start::count_tracked_ids`：只对 `tweet_id` 计数，不再把 `author_id` 混入同一 tracked 集合 | **U3**。依附 20260814 §1.2.1 已暂缓的 `ColdStartTrackedIds` observability 线路（本地无该函数）。重入时采用修正后语义：tracked 集合语义为帖 ID，作者 ID 不重复计数 |
| I3 | `vm_ranker` 请求映射新增 `sid_known/sid_k1..3/sid_gap1..3` slate 字段 | **U3**。依附 b089ce6 A5 线路（`SlateContext` sid 字段 + 候选 `semantic_ids` 数据源）；本地候选无 `slate_context`，无落点 |

### G. grox（U3，P6-B deferred）

| 编号 | 能力 | 说明 |
|---|---|---|
| G1 | reply_spam 收敛终态：删除 `PostStreamTaskGenerator`，`ReplyRankingTaskGenerator`（v3 topic）注入全部三个 plan（reply_ranking + spam_comment + coordinated_spam）；`TaskSpamFilter`/`TaskReplyRankingFilter` 粉丝阈值 40000 → 60000 | **U3**。**取代 b089ce6 C4 的中间形态**，重入时以本版本为准 |
| G2 | ptos 交叉验证任务 metric 属性从 hard/soft 二分改为全量 `policyType.value` | **U3**（b089ce6 C1 线路迭代） |

## 3. 结论与锚点

本提交 9 项能力全部落在既有 U3 线路上，**无代码落地**。两条记录价值：

1. I1/G1 修正了 b089ce6 清点中 A1/C4 的中间语义，重入时应以本文件版本为准，避免按中间态实现后再返工。
2. V1–V5 确认上游 Following 链路的 VF 接入点位置（post-selection hydrator 替换），重入 visibility filtering 时直接对照。

验证：无代码改动，无需重跑测试。锚点随 `docs/update/20260818.md` 前移至 `11a71f8`，下一清点对象 `aad7179`（2026-08-19，phoenix 引擎 checkpoint/SID/async embedding 基础设施 + home-mixer 引用帖文本/静音关键词过滤，预计含可落地项）。
