# 提交 b089ce6 能力清点与 U0–U3 分类（2026-08-17 上游快照）

> 文档状态：清点完成，待按 §4 顺序落地
> 上游提交：`b089ce64891f9c50fab73aa00dbe65acb82f198f`（2026-08-17，CI 批量导出，提交信息无区分度，以哈希+日期标识）
> 上游父提交（当前已吸收锚点）：`c65aa179db7bdd61e2c2821eac87f208a105c053`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)；不整体 cherry-pick，按 U0–U3 语义迁移
> 后续提交：`11a71f8`（08-18）、`aad7179`（08-19）、`d0cef2f`（08-20）依次清点

## 1. 提交概览

34 个文件，+751/-162。跨 6 个子系统，按主题归为四组：

1. **品牌安全 verdict V2**（home-mixer）：`compute_verdict_v2` 与 V2 安全标签体系，开关参数化。
2. **Phoenix slate 特征扩展**（home-mixer）：`SlateContext` 新增三级 semantic-ID 多样性统计；候选侧补作者 NSFW mask 生产接线与转发粉丝数修正。
3. **Grox 内容安全链路**（grox）：ptos 成人内容交叉验证 judge、Kafka 加载限流、GrokShare 元数据渲染、reply_spam 生成器拆分。
4. **杂项运维修正**：mm-embedding 缓存 TTL 去环境变量化、bdsm 执法门槛参数化、abuse-enforcement Kafka 手动提交、Strato rankall 数据流门控口径变化。

## 2. 分支对照事实（清点证据）

以下事实来自对当前分支工作树的直接检查，不依赖提交标题推测：

| 事实 | 证据 |
|---|---|
| 本地无 `visibility-filtering/`、`phoenix-rankall-strato/`、`bdsm/`、`abuse-enforcement-service/` 目录 | 根目录列表 |
| 本地 `home-mixer/models/brand_safety.rs` 仅 21 行：verdict 枚举 + proto 映射，无 `compute_verdict` | `wc -l`；文件头 |
| 本地无 `xai_x_thrift` 依赖，`SafetyLabelType` V2 变体（`GROK_NSFA_V2`、`NSFW_TEXT`、`EGREGIOUS_NSFW` 等）全仓库无定义 | 全仓 grep |
| 本地无 ads 管线：无 `ads_brand_safety_vf_hydrator.rs`，`candidate_hydrators/` 共 13 个文件无 ads 前缀 | 目录列表 |
| 本地 `PostCandidate` 无 `nsfw_author_phoenix`、`semantic_ids`、`SlateContext`；`home-mixer` 全仓无 `safety_label_mask`/`semantic_ids` 引用 | grep |
| 本地 `ranking_scorer.rs:382` 明确注释：SlateContext 持久化尚未引入，只计算位次 | 文件注释 |
| phoenix 侧合同已就绪：`recsys.proto` 有 `safetyLabelMask`（Candidate 字段 27、History 字段 33），`SAFETY_BIT_AUTHOR_NSFW` 已统一导出并被 `xai-recsys/util.rs:530/848` 消费，测试在 `util.rs:1642/1673` | grep；即 `b7d0c6c` 已落地的一半 |
| 本地 `gizmoduck_hydrator.rs` 已重写（+344 行），无 `nsfw_author`/`LabelValue` 作者安全标签数据路径 | grep |
| 本地无 `FOLLOWING_MAX_RESULT_SIZE` 常量、无 `client_events_kafka_side_effect.rs`、无 `util/urt/` 模块 | grep/目录列表 |
| 本地 `mm_embedding_client.rs` 仍含 `MM_EMBEDDING_TTL_SECS` 环境变量覆盖（`83c9f04` 导入的 `47c1bcd` 版本） | grep |
| 本地 grox 为 P6-A 恢复骨架（`src/` 布局），未采用上游 `flows/`，见 [`p6-grox-recovery-audit.md`](./p6-grox-recovery-audit.md) | 目录列表 |

## 3. 能力清单与分类

### A. home-mixer（13 个文件）

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| A1 | `compute_verdict_v2`：V2 中/低风险标签集、`strip_v1_grok_written`（剥离 prompt 侧双写的 v1 标签，规则 ID 1400–1700）、`GROK_SFA_V2` 门控、`PTOS_REVIEWED` + tweet ID cutoff 门控、v1/v2 等价性矩阵测试 | **U3** | 依赖未公开合同：`xai_x_thrift` 的 V2 `SafetyLabelType` 变体与 botmaker 规则 ID 语义；且依附本地不存在的 ads 管线。重入条件：V2 标签类型进入公开 proto/thrift 合同 **且** ads 管线落地。本节语义已记录，重入时无需重读上游 diff |
| A2 | `EnableAdsBrandSafetyVerdictV2` 参数 + `ads_brand_safety_vf_hydrator` 内 v1/v2 切换 | **U3** | 依附 A1；参数宏体系本地已重写，无对应挂载点 |
| A3 | `PostCandidate.nsfw_author_phoenix` 字段 + 候选→phoenix 请求时置 `safety_label_mask = SAFETY_BIT_AUTHOR_NSFW`（仅原创帖，转帖不置位）；gizmoduck 侧从 `user.safety.nsfw_user/nsfw_admin` 与 `NSFW_HIGH_PRECISION/POSSIBLY_NSFW_ACCOUNT` 标签推导 | **拆分**：字段+mask 映射 = **U0 合同补齐**；gizmoduck 推导 = **U3** | 字段与映射现在落地：`None` → mask 0，无行为变化，补齐 `b7d0c6c` 缺失的生产者半侧。gizmoduck 填充依赖作者安全标签数据源（本地 TES/demo 适配器无此字段），列 U3，重入条件：作者安全标签进入 TES core-data 或等价合同 |
| A4 | 候选→phoenix 请求映射中 `followers` 仅原创帖设置（转帖置 `None`） | **U3**（落地中重分类） | 上游修的是引擎请求映射中转发作者粉丝数的串扰；本地网关 `TweetInfo` 无 followers/authorInfo 字段、无任何 followers 转发链路，无此 bug 的载体。重入条件：引入 followers 转发时直接采用原创帖限定语义 |
| A5 | `SlateContext` 新增 `sid_known/sid_k_l1..3/sid_gap_l1..3`；`ranking_scorer` 在最终排序后对三级 semantic-ID 前缀（每级 20 bit 打包）做滑窗频次与位距统计 | **U3** | 算法本身可移植，但依附两条本地未引入链路：`SlateContext`（`ranking_scorer.rs:382` 已记录暂缓）与候选 `semantic_ids`（上游由检索侧填充，`aad7179` 的 `sid_client` 相关）。重入条件：semantic_ids 数据源落地 + SlateContext 重评估。注意：该统计在请求内即时计算即可供给 phoenix slate 特征，**不强制依赖** SlateContext 的请求缓存持久化，重入时按此重新评估拆分 |
| A6 | `FOLLOWING_MAX_RESULT_SIZE` 100 → 110 | **U3** | 本地无该常量，following 管线参数体系不同；随 following 链路重入一并评估 |
| A7 | `client_events_kafka_side_effect`：`post_count` 改为按会话计数（root/parent 祖先各 +1，排除 tombstone） | **U3** | 本地无客户端事件 Kafka side effect。语义已记录：计数口径从"帖数"变为"会话展开后的可见帖数" |
| A8 | URT `new_tweets_pill` 新增 `require_full_facepile` 参数：ForYou 传 `true`（头像不足 NUM_AVATARS 则不出 pill），Following 传 `false` | **U3** | 本地无 `util/urt/` 模块（URT 迁移组件未采用） |

### B. phoenix / 部署侧（2 项）

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| B1 | `mm_embedding_client` 移除 `MM_EMBEDDING_TTL_SECS` 环境变量，统一回常量 `EMBEDDING_TTL`（2 天） | **U0**（采用上游移除） | 本地无文档化的 env 覆盖需求；writer/reader 分片共享 `/dev/shm` 缓存，TTL 必须一致，env 覆盖会破坏该不变量。按 upstream-first 采用移除 |
| B2 | `phoenixRankAllCandidateProcessor.strato`：`1fav` 索引事件改为零 fav 也发送（新增 `1fav_uec_count_race` 计数器）；mm-emb metadata dump 去掉 fav≥1 门控 | **U3** | Strato 属 X 内部部署环境，本地无 `phoenix-rankall-strato/`，不导入。仅记录语义：离线 1fav/mm-emb 数据流产出口径变化（零 fav 帖也产出）；本地离线训练数据走 parquet 生成器，不消费该流 |

### C. grox（13 个文件，整体落在 P6-B deferred 范围）

| 编号 | 能力 | 分类 | 处置 |
|---|---|---|---|
| C1 | ptos 成人内容交叉验证：`SafetyPtosAdultContentCrossValidationJudge`（Grok 4.5 x-algo + 独立熔断器）、新任务（safemodel/ptos 双方不一致时以 judge 裁决）、plan DAG 接线、`SafemodelResult.scored` 字段 | **U3** | flows 未采用；词汇表已记录，重入条件见 P6 审计 §5 |
| C2 | `GrokShare` 元数据类型与渲染（`grokShareMetadatas` 独立于卡片级 share）；`LegacyCard.to_convo` 空 body 时返回空（不再产出裸 `[Card]` 头） | **U3** | 同上。`LegacyCard` 空 body 修正是内容渲染正确性语义，重入时优先吸收 |
| C3 | `KafkaLoader` 按分区限流（`max_qps_per_partition` × 已分配分区数，固定窗口，`limits` 库） | **U3** | 同上 |
| C4 | reply_spam：`ReplyRankingTaskGenerator` 拆分为独立生成器（消费 `unified-posts-v3` topic） | **U3** | 注意 `11a71f8`（08-18）又改动了同组文件，重入时以更晚快照为准 |

### D. 本地无对应模块（不迁移，仅记录）

| 编号 | 能力 | 说明 |
|---|---|---|
| D1 | bdsm：`min_actions_for_enforcement` 参数化并从 30 改为 `999999` 红线 sentinel；冷却阈值默认跟随该门槛；测试锁定 sentinel 不可达 | 本地无 `bdsm/` 目录，不在 workspace。语义：执法动作数门槛本身也列入不公开操作点 |
| D2 | abuse-enforcement-service：Kafka 消费者关 `enable_auto_offset_store`/`enable_auto_commit`（改手动提交） | 本地无该目录。语义：消费位点手动管理，属可靠性修正 |

## 4. 处置汇总与落地顺序

**已落地（2 个提交，见 [`../update/20260817.md`](../update/20260817.md)）：**

1. `home-mixer: wire author NSFW safety mask producer side`（A3 合同补齐；A4 在落地时重分类为 U3，见 §3 表）
   - `PostCandidate` 新增 `nsfw_author_phoenix: Option<bool>`；网关 proto `TweetInfo` 新增 `safety_label_mask`（字段 4）；`phoenix_scorer` 置位规则为原创帖且 `Some(true)`；常量由 `x-algorithm-proto` 导出。
   - 默认路径行为不变（字段缺省 `None` → mask 0）。
2. `phoenix: drop env override for mm embedding TTL`（B1）
   - 删除 `embedding_ttl()` 与 `MM_EMBEDDING_TTL_SECS`，统一使用 `EMBEDDING_TTL`。

**记录为 U3（本文 §3 已含语义与重入条件，不落地代码）：** A1、A2、A5、A6、A7、A8、B2、C1–C4。

**不迁移（本地无模块）：** D1、D2。

**记录提交：** `docs: record b089ce6 inventory and adoption outcome`（本文 + `docs/update/20260817.md` 落地结果）。

## 5. 验证

- `cargo test --workspace`（A3/A4/B1 至少保证编译链接与既有测试通过；A3 的 mask 置位逻辑补单测：原创+`Some(true)` 置位、转帖不置位、`None` 不置位）。
- `./scripts/run_demo.sh` 端到端不回归。

## 6. 锚点推进

已完成：验证通过后 `upstream-first-maintenance.md` 锚点已由 `c65aa17` 前移至 `b089ce6`。
