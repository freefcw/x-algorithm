# Goal Document: `e414c17` / `0bfc279` 到 `mp` 的能力差异与迁移台账

> 文档状态：本地可运行迁移已完成（P0-P2、P3-A/P4-A/P5-A/P6-A）；生产集成统一进入 Backlog
> 上游共同基线：`aaa167b3de8a674587c53545a43c90eaad360010`
> 上游功能提交：`e414c171ed68266341193330bc4864bf3f3534e3`
> 上游模型产物提交：`0bfc2795d308f90032544322747caacd535f75ae`
> 当前同步锚点：`0bfc279`（功能与模型产物均已吸收；工作树 LFS 指针即 `0bfc279` 版本，见 PHX-11）
> 本地目标分支：`mp`（`3e492095613b2a008de5d9f8295d5b6e0c07c777`）
> 后续同步规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)
> 入口执行顺序：[`entrypoint-migration-map.md`](./entrypoint-migration-map.md)

## Go / No-Go

- **判断**：Go，本地可运行迁移已验收，继续执行统一 Integration Backlog；No-Go，仍禁止直接完整 rebase 或整提交覆盖。
- **原因**：`mp` 已经吸收可独立验证的模型、流水线、最终 Feed、本地状态和 Grox 执行骨架。剩余工作主要依赖真实服务、模型、产品和数据治理决策；直接 rebase 既不能补齐这些依赖，还会破坏当前可运行链路。
- **基本原则**：所有上游业务能力默认进入“保留并评估”清单。暂缓不等于删除；只有通过本文定义的删除门槛，才能判定某项能力不迁移。

## Target Outcome

在不破坏 `mp` 现有本地演示、训练和 gRPC 接入能力的前提下，逐步吸收上游新增的 Phoenix 模型能力、推荐流水线能力、最终 Feed 混排能力、反馈闭环和 Grox 内容理解能力。

迁移完成后，应能逐项回答：

1. 上游每项能力是否已迁移、由 `mp` 现有能力覆盖、明确暂缓，或有证据支持删除。
2. 每项迁移给最终用户、算法开发者或运维人员带来了什么实际收益。
3. 每项能力由哪个模块负责，依赖哪些外部服务，如何独立测试和关闭。
4. `./scripts/run_demo.sh` 是否始终保持可运行，且迁移后的模型和 Feed 行为可验证。

## Goal Definition

- **类型**：技术迁移、产品能力补齐、质量改进。
- **范围**：`e414c17` 的 187 个变更文件及 `0bfc279` 的模型产物替换。
- **不在范围内**：
  - 不直接复刻 X 的内部服务部署环境。
  - 不把 `xai_feature_switches`、`xai_decider`、Strato、Manhattan 等内部实现写死到 `mp` 核心业务代码中。
  - 不在评估阶段一次性重写 `mp` 的公共 gRPC 合约。
- **暂缓工作**：
  - 没有本地替代依赖前，Grox 不进入默认运行链路。
  - 没有产品场景前，广告、Who to Follow、Prompts、Push-to-Home 默认关闭，但能力继续保留在迁移台账中。
- **验证规则**：每个迁移项必须有行为测试、接口测试、端到端结果或可检查的运行记录，不能只以“文件已经复制”为完成依据。
- **证据来源**：Rust/Python 测试、gRPC 合约测试、Phoenix 推理结果、`run_demo.sh`、组件启用日志、候选数量与 Feed 结构。
- **通过标准**：所有能力编号都有明确状态和证据；所有已迁移能力通过对应验收；完整 Demo 不回归。
- **可信度说明**：差异台账来自 Git 共同基线的真实 diff，并结合 `mp` 当前提交树检查现有能力，不依赖提交标题推测。
- **判断归属**：测试和端到端命令判断技术完成；是否启用广告、Grox 等产品能力由项目目标决定。

## Current State

### 分支事实

- `main` 与 `mp` 从 `aaa167b` 分叉。
- 上游 `e414c17`：187 个文件，`+18,263/-926`。
- 上游变更分布：

| 区域 | 文件数 | 新增 | 删除 |
|---|---:|---:|---:|
| Candidate Pipeline | 9 | 649 | 189 |
| Grox | 59 | 6,500 | 0 |
| Home Mixer | 108 | 10,052 | 671 |
| Phoenix | 9 | 1,039 | 66 |
| 根目录发布配置/说明 | 2 | 23 | 0 |
| **合计** | **187** | **18,263** | **926** |

- `mp` 已实现完整 Rust workspace、公共 proto、Phoenix HTTP/gRPC 服务、训练脚本、Demo 客户端和一键端到端脚本。
- `mp` 与上游共同修改 27 个文件；三方模拟合并有 24 个内容冲突。
- `mp` 已将 Candidate Pipeline 的 portable contract 重新锚定到 `e414c17`：Source/QueryHydrator/Selector/SideEffect 保留上游实现点与 `run` 包装，Hydrator/Scorer 恢复逐候选 `Vec<Result<...>>` 和长度保护，同步 Filter 恢复 `filter -> FilterResult`；私有 metrics/config 由本地接口替代，Filter 失败恢复作为 additive `try_run` 扩展保留。
- `mp` 已恢复 Home Mixer 的上游 service composition、`QueryBuilder`、`crate::models::*` canonical path、`u64` domain ID 合同，以及 `HM-E4/HM-E5` 内外层 portable assembly；`HM-E6` 以 additive ForYou wrapper/V2 和 typed DebugScoredPosts 扩展公共 RPC。Debug 默认关闭并要求 token，未签名 cached posts 默认拒绝且只允许显式 Demo；URT/trace 仍因合同缺失 deferred。
- `mp` 已将右对齐位置、帖子年龄 embedding、连续行为输入/预测头接入可选模型 forward；旧模型配置默认关闭，发布模型配置按 checkpoint shape 启用。
- `mp` 已新增统一 NPZ loader、离线 `run_pipeline.py`、发布 artifact gRPC 适配器、独立 `ScoredPostsServer`，以及 additive-compatible 的 `ForYouFeedService`。当前 offline/gRPC published 模式通过 `PublishedArtifact -> PublishedPipeline -> shared engines` 使用同一 loader、hash/preprocessing、model runner 和 output mapping；随机/本地 checkpoint 模式保持显式分支。
- P4-A 已完成独立 `FeedItem`、ScoredPosts bridge、disabled-first Ads port，并将 `SafeGap`、`PartitionOrganic` 的间距、分组、BSR/账号/关键词规避重新锚定到上游 `e414c17`；本地仅保留缺失 verdict fail-closed、真实 `non_selected` 和公开协议适配。真实广告、Who to Follow、Prompt、Push-to-Home 来源仍属于 P4-B。
- P5-A 已完成有界内存 served history、request timestamps 和最终 Feed 统计；Kafka、Redis、外部事件与训练数据出口仍属于 P5-B。
- 上游 Grox 引用多个未提交模块，例如 `grox.config`、`grox.lm`、`grox.prompts`、`monitor`，不能原样运行。P6-A 只恢复了独立任务 DAG、结果信封和 Source/Sink port，不代表模型内容理解能力已经恢复。

### 已执行迁移记录（批次当时证据）

以下数字保留迁移批次完成时的验收快照；当前累计测试与运行证据统一查看后文 `EV-*` 和 Final Validation。

| 批次 | 内容 | 状态 | 证据 | 默认行为 |
|---|---|---|---|---|
| LR-01 | Phoenix 新增纯函数单元测试 | 已完成 | `test_recsys_model.py` 27 项通过；Phoenix 全套 72 项通过 | 不变 |
| LR-02 | `right_anchored_rope_positions` | 已迁移、条件启用 | 位置经 Transformer 全链路传递；旧配置关闭，专项位置/padding 测试通过 | 旧配置不变 |
| LR-03 | `compute_post_age_bucket` | 已迁移并用于发布 ranker | 候选时间戳进入年龄 embedding；真实 checkpoint gRPC 推理通过 | 旧配置不变 |
| LR-04 | `NormConfig`、`ContinuousActionConfig`、`normalize_continuous_value` | 已迁移并用于发布 ranker | 连续历史输入和 8 维预测头接线；gRPC 返回真实 dwell 连续值 | 旧配置不变 |

P0 基线回归：`cargo test --workspace` 通过（11 个套件、24 项测试）；Phoenix 全套 72 项测试通过；`./scripts/run_demo.sh` 返回 50 条 Feed（网内 12、网外 38），三个服务正常退出并清理。

### 当前执行状态（2026-08-08）

| 阶段 | 状态 | 已交付能力 | 验收证据 |
|---|---|---|---|
| P0 | **完成** | Rust/Python/gRPC/Demo 行为基线 | 基线测试与 12 网内 + 38 网外 Feed 已记录 |
| P1 | **完成** | 正式 ranker/retrieval 语义、统一 NPZ loader、离线 pipeline、发布 artifact gRPC、Git LFS 合同 | 最新 2,903,518,802 字节对象 SHA-256 为 `fbc6017d...a83dac`；真实离线 retrieval→ranking 成功；真实 gRPC Retrieve + Predict 成功并返回连续 dwell |
| P2 | **完成** | Query/Candidate 最小合同、dependent hydration、同步 Filter、缓存 Hydrator、selected/non-selected、失败隔离、逐组件耗时/数量、空结果短路、能力开关接口 | Candidate Pipeline 与 Home Mixer 框架/业务测试通过；Source/Filter 失败、缓存命中、过滤清空、截断均有测试 |
| P3 | **P3-A 完成 / P3-B 暂缓** | ScoredPosts 边界、扩展 Query/Candidate、缓存/Phoenix/Phoenix MoE/话题/Thunder 来源、主要 Hydrator/Filter、组合 Ranking | 默认 Demo 恢复 12 网内 + 38 网外；话题 Demo 返回 50 条 `Phoenix 话题`；缓存 Demo 返回 8 条 `请求缓存`；生产数据面见独立 Integration Backlog |
| P4 | **P4-A 完成 / P4-B 暂缓** | 独立 FeedItem/ForYou RPC、ScoredPosts bridge、模块混排、Safe-gap/Partition-organic 规则、disabled Ads port | P4/P5 定向 25 项通过；最终 Feed Demo 返回 12 网内 + 38 网外；没有安全 verdict 时不插广告 |
| P5 | **P5-A 完成 / P5-B 暂缓** | 全局/单用户双重有界内存状态、响应前本地一致性提交、构成/位置统计、非阻塞外部 SideEffect | 无等待连续请求、用户淘汰、状态截断、统计记录和 sink 失败隔离测试通过；Kafka/Redis 保留待集成 |
| P6 | **P6-A 完成 / P6-B 暂缓** | 独立 eligibility-gated DAG、skip/failure 信封、环检测、Source/Sink port、本地 JSON Demo | Grox 10 项通过；CLI 输出文本元数据，不生成模型/安全/embedding 结论 |

P3 已完成的默认/可运行范围：`HM-01..02`、`HM-05..07`、`SRC-01..04`、`SRC-06`、`QH-01..13` 的本地合同、`CH-01..06`、`CH-09..11`、`CH-13..14`、`CH-16`、`FLT-01..17`、`RANK-01..02`、`SEL-01`。其中 Phoenix MoE 只有在同时设置请求开关与 `PHOENIX_MOE_GRPC_ADDR` 时装配；话题页采用严格召回，公开 `new_user_topic_ids` 保持冷启动限定语义，补充话题只有在显式注入 Topic Adapter 时采用混合召回；生产默认不装配 `UserTopicReader` 和 `TopicRetrievalClient`。

P3 仍未完成且不能伪造的条件能力：

- `SRC-05` TweetMixer：仓库没有公开服务合同或可运行服务。
- `QH-12..13` 的主动关注/推断话题：本地仅保留“外部 Adapter 返回最终补充话题”的窄端口和 Demo Adapter；关注、推断、年龄及资格策略不在 Home Mixer 内猜测，生产仍需明确数据来源、时效和隐私决策。
- `QH-14..18` 中的 starter packs、共同关注 minhash、IP 位置、人口统计和推断性别：需要明确数据来源、隐私和公平性决策；请求 IP 字段仅作为默认关闭的边界输入。
- `CH-12` following-replied users、`CH-15` mutual-follow Jaccard、`CH-17` tweet type metrics：前两项缺社交图数据端口的真实实现，后一项尚未接统计出口。
- `RANK-03` VM Ranker：没有公开 RPC/模型合同；不把未知外部分数写入核心 Ranking。

因此 P3 采用分层验收：**P3-A 本地可运行迁移已完成，P3-B 生产外部集成暂缓**。条件能力继续保留编号和恢复条件，但不再阻塞 P4-A/P5-A/P6-A 的纯迁移；统一集成 TODO 见 `p3b-p6-migration-goal.md`。

### 进度口径

本台账不再用单一百分比表示整体完成度。原因是一个文档更新、一个过滤器和一套 Grox 模型能力不能按同一权重计算。当前进度必须同时回答三件事：

1. **本地迁移**：代码、接口或纯规则是否已经在本仓库可运行并有测试。
2. **默认状态**：该能力是否进入默认 Demo/服务路径，还是条件启用、独立运行或保持关闭。
3. **生产接入**：真实数据、服务、安全、模型、认证和运维合同是否已经闭合。

由此得到两个不混淆的结论：

- 对已批准的本地范围，P0-P2 与 P3-A/P4-A/P5-A/P6-A 已完成。
- 对上游完整生产能力，迁移尚未完成；所有未闭合项继续进入 Integration Backlog，不能因为已有 trait、字段或 Demo 就标记为生产可用。

下面的“交付状态索引”是当前进度的唯一权威入口；后文 Capability Inventory 用于说明上游能力、业务价值和最初处理决策，不再承担实时进度统计职责。

### 证据索引

| 证据 | 可复核内容 |
|---|---|
| `EV-CP` | `cargo test -p xai_candidate_pipeline`：18 项通过；覆盖上游执行包装、逐候选 Hydrator/Scorer 失败隔离、长度保护、缓存只写成功结果、同步 Filter、selected/non-selected、post-selection underfill 不绕过过滤、单 Source 失败保留其他来源候选和 SideEffect 输入。 |
| `EV-PHX` | Phoenix 88 项测试通过；offline JSON/proto UAS tensor parity、固定 impression-time age parity、共享 orchestration、transport-neutral inference values、O(1) topic lookup、preloaded params 和 action mapping 均有回归。真实 artifact SHA/shape、离线 retrieval→ranking 和真实 gRPC Retrieve/Predict 的历史验收见 Final Validation。 |
| `EV-P3` | `cargo test -p home-mixer --all-targets`：146 项通过；`cargo test --workspace`：13 个套件、168 项通过。ScoredPosts/ForYou Demo 均返回 50 条（10 网内 + 40 网外），显式缓存 Demo 返回 8 条。Debug 默认 `Unavailable`，错误 token 为 `PermissionDenied`，授权 wire 验收为 600/4/50 stage counts。Viewer/VF fail-safe、所有关键外部调用 deadline、Phoenix endpoint fallback、跨用户 UAS/Strato/TES 隔离、非持久 adapter 写入拒绝、post-selection profile 装配、underfill 不绕过安全、unsigned cache 拒绝、运行模式、全 ID checked conversion 和 portable assembly 均有测试。 |
| `EV-RANK` | Phoenix 预留离散槽位 19/20、发布 profile 缺槽位兼容、引用帖 VQV 时长门槛和 `not_dwelled` 负权重均有 Rust 回归测试；`click_dwell_time` 因协议尚无对应连续动作而保持 `None`。 |
| `EV-P4` | `cargo test -p home-mixer --test p4_final_feed`：25 项通过；覆盖独立 FeedItem、ScoredPosts bridge、两种上游广告规则、默认关闭和 ForYou RPC。 |
| `EV-P5` | 同一 P4/P5 集成测试覆盖连续请求、单用户/全局状态截断、构成统计和 sink 失败隔离。 |
| `EV-P6` | `uv run --project grox --group dev pytest -q grox/tests`：10 项通过；`p6-grox-recovery-audit.md` 明确模型能力未恢复。 |
| `EV-REL` | `.gitattributes` LFS 规则、`phoenix/README.md` 三种运行路径和本文件 Final Validation。 |
| `EV-PORT` | 接口先行批次（2026-08-13）：`VMRanker`、`TweetMixerSource`、`BlockedByHydrator`、曝光存储双 Hydrator、seen-ids/served-candidates SideEffect 均以领域端口 + 内存 fake 驱动的单测验收（分数合并与失败隔离、请求映射与超龄过滤、反向屏蔽标记、存储覆盖请求值、空请求跳过、影子流量门槛）；`AuthorSocialgraphFilter` 候选级信号有中立性回归。全部组件默认不装配。 |

### 交付状态索引

状态说明：`完成` 表示本地行为已经验收；`部分` 表示只完成边界、port、字段或中立骨架；`未开始` 表示尚无可验证实现。默认状态和生产接入必须单独阅读。

| 能力编号 | 本地迁移 | 默认状态 | 生产接入 | 证据 |
|---|---|---|---|---|
| `CP-01..08` | 完成 | 启用 | 无外部依赖 | `EV-CP` |
| `PHX-01..06`, `PHX-08..11` | 完成 | 发布 profile 启用；旧 profile 兼容 | 模型产物与推理合同已闭合 | `EV-PHX`, `EV-REL` |
| `PHX-07` | 未开始 | 关闭 | 待隐私、同意和数据保留决策 | Integration Backlog |
| `HM-01..02`, `HM-05..07` | 完成 | 启用 | 核心代码完成；真实字段来源见 P3-B | `EV-P3` |
| `HM-03..04` | 完成 | ForYou 自然 Feed 启用；非帖子内容关闭 | 自然 Feed 无额外依赖；非帖子来源见 P4-B | `EV-P4` |
| `SRC-01..02` | 完成 | 默认启用 | Phoenix/Thunder 生产地址、认证、容量和降级待验收 | `EV-P3` |
| `SRC-03..04` | 完成 | 条件启用 | MoE/Topic 真实服务合同待集成 | `EV-P3` |
| `SRC-05` | 部分：上游同构 Source 与 `TweetMixerClient` 端口已迁移 | 关闭（未装配） | 缺公开 TweetMixer 合同；Adapter 通过验收后经装配注入 | `EV-PORT` |
| `SRC-06` | 完成 | 默认拒绝请求携带的未签名缓存；仅显式 Demo 启用 | 生产缓存需服务端状态或签名/opaque 合同 | `EV-P3` |
| `SRC-07` | 完成 | 启用 | 无额外外部依赖 | `EV-P4` |
| `SRC-08` | 部分：Ads port 与 disabled adapter | 关闭 | 待广告合同和真实品牌安全判定 | `EV-P4`, Integration Backlog |
| `SRC-09..11` | 部分：FeedItem、注入点和测试 Source | 关闭 | 待产品入口、内容审核和服务合同 | `EV-P4`, Integration Backlog |
| `QH-01..09` | 完成：本地合同与空数据降级 | 按请求/适配器启用 | 用户关系、曝光和请求缓存服务待集成 | `EV-P3` |
| `QH-05..06`（服务端曝光存储） | 部分：`ImpressedPostsClient`/`ImpressionBloomFilterClient` 端口与上游同构 Hydrator 已迁移 | 关闭（未装配）；当前数据仍来自请求 | 待曝光存储服务合同；启用时须显式决策服务端与请求数据的优先级 | `EV-PORT` |
| `QH-10..11` | 完成 | 启用 | 生产 UAS 数据源待集成 | `EV-PHX`, `EV-P3` |
| `QH-12..13` | 完成：本地端口、选择规则与 Demo Adapter | Demo 条件启用；生产关闭 | 待关注/推断话题真实数据合同、时效和隐私验收 | `EV-P3`, Integration Backlog |
| `QH-14..15` | 未开始 | 关闭 | 待 starter pack/社交图数据合同 | Integration Backlog |
| `QH-16..18` | 未开始 | 关闭 | 待隐私、公平性、同意和保留策略；本地 Query 仅有 `ip_address` 空字符串占位，`user_demographics`/`inferred_gender` 字段尚未引入 | Integration Backlog |
| `CH-01..06`, `CH-09..11`, `CH-13..14`, `CH-16` | 完成：本地合同/实现 | 按候选数据启用；VF 未知时网外拒绝、网内保留 | TES、作者资料、关系和 VF 真实服务待集成；缺失时 `production_ready` 拒绝启动 | `EV-P3` |
| `CH-07..08` | 部分：显式安全 verdict 边界 | 关闭 | 待广告安全 Hydrator 和供应商 | `EV-P4`, Integration Backlog |
| `CH-09`（候选级反向屏蔽） | 部分：`SocialGraphClientOps` 端口与上游同构 `BlockedByHydrator` 已迁移；`AuthorSocialgraphFilter` 已按上游消费候选级信号（未补全时中立） | 关闭（未装配）；Query 级 blocked-by 列表仍是当前生效路径 | 待真实社交图 Adapter（认证、超时、批量上限） | `EV-PORT` |
| `CH-12`, `CH-15`, `CH-17` | 未开始 | 关闭 | 待社交图端口或统计出口；CH-12/15 依赖 QH-15 minhash 数据合同，先于端口定义 | Integration Backlog |
| `FLT-01..17` | 完成 | 默认或按请求条件启用 | 过滤逻辑已完成；部分输入数据随 P3-B 接入 | `EV-P3` |
| `RANK-01..02` | 完成 | 启用 | Phoenix 生产部署仍需环境验收 | `EV-PHX`, `EV-P3`, `EV-RANK` |
| `RANK-03` | 部分：上游同构 Scorer 与 `VMRankerClient` 端口已迁移 | 关闭（未装配） | 缺 VM Ranker RPC/模型合同；value model/DPP 由装配显式配置 | `EV-PORT` |
| `SEL-01` | 完成 | 启用 | 无外部依赖 | `EV-CP`, `EV-P3` |
| `SEL-02`, `ADS-01..03` | 完成：纯混排规则 | 非帖子来源默认关闭 | 真实广告和安全服务待接入 | `EV-P4` |
| `SE-01..02`, `SE-04..05`, `SE-08..10` | 未开始 | 关闭 | 待消费者、schema、保留期、幂等和运维合同 | Integration Backlog |
| `SE-07`, `SE-11` | 部分：`SeenIdsPublisher`/`ServedCandidatesSink` 领域端口与上游同构 SideEffect 已迁移 | 关闭（未装配） | 待 Kafka/存储 Adapter 承担 schema、序列化、重试与幂等 | `EV-PORT` |
| `SE-03`, `SE-12..14` | 完成：内存/日志实现 | 启用 | Redis、Kafka 和外部指标属于 P5-B | `EV-P5` |
| `SE-06` | 部分：请求缓存边界存在 | 默认关闭 | Strato/替代存储合同待接入 | `EV-P3`, Integration Backlog |
| `GRX-01` | 部分：Plan/Task DAG 与结果信封 | 独立运行 | Engine/Dispatcher 生命周期和持久确认待恢复 | `EV-P6` |
| `GRX-02` | 部分：Source port | 关闭 | Kafka/队列/Strato 合同待接入 | `EV-P6` |
| `GRX-03..10` | 未开始 | 关闭 | 缺模型、Prompt、媒体/ASR、政策和输出合同 | `EV-P6` |
| `GRX-11` | 部分：通用 Plan 与依赖/eligibility 规则 | 独立运行 | 具体 Spam/安全/Embedding/回复计划待恢复 | `EV-P6` |
| `GRX-12` | 部分：eligibility/禁用边界 | 独立运行 | 真实限流和环境配置待恢复 | `EV-P6` |
| `GRX-13` | 部分：Sink port 与内存实现 | 独立运行 | Kafka/Manhattan/annotation/embedding sink 待接入 | `EV-P6` |
| `GRX-14` | 部分：本地结果、错误和时间边界 | 独立运行 | 生产 metrics/trace backend 待接入 | `EV-P6` |
| `REL-01` | 完成 | 启用 | 无外部依赖 | `EV-REL` |
| `REL-02` | 部分：阶段文档持续同步 | 可见 | 每次能力状态变化都需更新 | `EV-REL` |

### 台账维护规则

1. 能力状态变化时，先更新“交付状态索引”，再更新阶段 Todo 和 Current State；其他段落不得另建一套实时状态。
2. 只有行为测试、接口测试、端到端结果或可检查运行记录才能把“本地迁移”改为 `完成`；仅新增文件、字段、trait 或 stub 最多标记为 `部分`。
3. “默认状态”只描述当前装配和请求路径，不用“代码存在”推断为已启用。
4. “生产接入”只有在真实服务/模型、认证、错误语义、数据治理和测试环境验收后才能关闭；Demo adapter、内存实现或 disabled adapter 不等价于生产完成。
5. 新证据统一登记为 `EV-*`，状态行只引用证据编号，避免在多个位置复制易过期的测试数字。
6. 对外汇报优先给出三个维度和业务范围；除非先定义权重并由产品负责人确认，否则不计算整体百分比。

### 删除门槛

一项能力只有同时满足以下条件，才能从“保留并评估”改为“不迁移”：

1. 在 `mp` 的目标用户路径中没有可达场景。
2. 不是其他保留能力的前置依赖。
3. 不承担安全、合规、质量、可观测性或数据闭环职责。
4. 已记录替代方案或删除后的行为差异。
5. 由项目负责人明确接受该能力缺口。

当前没有任何能力通过该删除门槛。

## Plan Rewrite Notes

| 之前的建议 | 调整 | 原因 |
|---|---|---|
| 以 `main` 为基线重建 | 改为以 `mp` 为稳定基线逐项迁移 | `mp` 已有可运行交付；`main` 不是完整可运行 workspace |
| 优先按 Git 提交迁移 | 改为按业务能力编号迁移 | 一个提交混合模型、Feed、广告、Grox 和内部基础设施，无法独立验收 |
| 广告、Grox 等直接暂缓 | 改为保留在完整台账中，条件满足后进入对应阶段 | 暂时不能运行不代表永远无价值 |
| 只描述主要模块 | 增加来源、补全、过滤、打分、反馈等逐项清单 | 防止迁移时遗漏小但关键的行为 |
| 用一个完成率汇报迁移 | 拆为本地迁移、默认状态、生产接入三维台账 | 不把纯规则、文档和外部模型能力按同一权重误算 |
| 在各阶段段落重复写当前状态 | 以“交付状态索引”为唯一权威入口 | 避免阶段说明更新后仍残留相互矛盾的旧句子 |

## Drift Diagnosis

- **目标偏移**：若把“同步上游提交”当目标，会牺牲 `mp` 的可运行性。真正目标是获得上游业务能力。
- **阶段偏移**：按目录迁移会把模型语义、外部客户端和最终 Feed 混在一起，应按可独立验证的用户路径拆分。
- **验证偏移**：冲突解决或编译通过不能证明推荐行为正确，必须保留端到端 Feed 验证。
- **进度偏移**：不能把“trait/字段已存在”“本地 Demo 可运行”和“生产依赖已闭合”混成一个完成状态。
- **兼容偏移**：不能同时长期维护两套 Candidate、Query、proto 和配置语义；每阶段必须明确唯一业务模型及边界适配器。
- **清理偏移**：评估阶段不顺手删除旧 Demo、训练工具或兼容层，除非新链路已提供等价行为并通过验证。

## 状态和批次说明

下列标记描述的是**最初处理决策**，不是当前完成状态；当前状态只看前文“交付状态索引”。

| 标记 | 含义 |
|---|---|
| `必须迁移` | 新模型或核心推荐链路的正确性依赖它 |
| `默认保留` | 有明确业务或工程价值，原则上迁移 |
| `已有，合并语义` | `mp` 已有相近能力，保留一个实现并吸收上游行为 |
| `条件启用` | 代码能力保留，但只有外部服务或产品场景明确后才默认开启 |
| `隔离恢复` | 上游代码不完整，先作为独立边界恢复可运行性，不能污染主链路 |
| `删除候选` | 必须满足删除门槛；当前没有此状态的项目 |

批次 `P0` 到 `P6` 对应文末迁移阶段，不代表功能重要性排序。

## Capability Inventory

本节保留上游能力定义、最初差异、业务价值和处理决策，用于回答“为什么迁移”。其中的“`mp` 现状”是建台账时的差异背景，不用于回答今天是否完成；实时状态、默认开关和生产依赖统一以前文“交付状态索引”为准。

### A. Candidate Pipeline

| ID | 差异及作用 | 建台账时的差异 / 实现说明 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| CP-01 | 新增 `PipelineQuery` / `PipelineCandidate`，统一流水线对请求和候选的约束 | `mp` 仍使用较宽泛的泛型约束 | 明确流水线需要的最小能力，减少组件各自猜测请求结构 | `默认保留/P2`；定义本地接口，不直接依赖 X 内部类型 |
| CP-02 | 新增 dependent query hydration，在第一轮用户信息补全后再运行依赖它的补全 | `mp` 只有单轮并行补全 | 可正确构建依赖用户画像或前序结果的请求特征 | `默认保留/P2` |
| CP-03 | Source、Hydrator、Scorer、Filter、Selector、SideEffect 增加统一 `run` 包装 | `mp` 主要由 Pipeline 直接调用组件方法 | 统一记录错误、耗时、数量和启用状态，排查空 Feed 更直接 | `默认保留/P2` |
| CP-04 | Filter 从异步改为同步分区，并记录每个过滤器移除数量 | `mp` 沿用旧接口 | 过滤本身不做 I/O，语义更清楚；可以看出候选在哪一步被删光 | `默认保留/P2` |
| CP-05 | 新增 `CachedHydrator`、`CacheStore`、命中/未命中统计 | `mp` 各客户端自行处理缓存或没有缓存 | 帖子资料、互动数、安全标签可复用统一缓存流程 | `默认保留/P2` |
| CP-06 | Selector 返回 `selected` 和 `non_selected` | `mp` 只返回入选候选 | 可记录被截断、被混排丢弃的候选，支持实验和问题分析 | `默认保留/P2` |
| CP-07 | SideEffect 输入增加未入选候选；组件失败隔离并有独立统计 | `mp` SideEffect 只关注最终结果 | 支持训练日志、重排样本和完整候选漏斗 | `默认保留/P5` |
| CP-08 | 新增组件清单、阶段枚举、`finalize`、空结果快速返回、最终结果指标 | `mp` 缺少统一自描述能力 | 启动时可打印实际链路，测试用户或降级请求可安全短路 | `默认保留/P2` |

涉及文件：`candidate_pipeline.rs`、`filter.rs`、`hydrator.rs`、`query_hydrator.rs`、`scorer.rs`、`selector.rs`、`side_effect.rs`、`source.rs`、`util.rs`。

### B. Phoenix 模型、推理和产物

| ID | 差异及作用 | 建台账时的差异 / 实现说明 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| PHX-01 | 新增统一 `run_pipeline.py`，一次完成用户编码、语料召回、Top-K 精排 | **已迁移**：对发布包 84,564 条体育语料完成真实 retrieval→ranking | 可单独验证模型，不必启动三个服务；便于定位模型与编排问题 | `已完成/P1` |
| PHX-02 | 发布检索与排序 checkpoint、拆分 embedding 表、体育语料和示例行为序列 | **已迁移**：LFS 指针更新；真实包在隔离目录完成 SHA、shape、离线和 gRPC 验证 | Demo 可以产生有意义的推荐结果，并提供对照基线 | `已完成/P1` |
| PHX-03 | `runners.py` 新增导出参数和 embedding 表加载器 | **已迁移**：slash-delimited NPZ 还原为 Haiku 参数树；离线脚本与发布 gRPC 共用 loader/build config | 让 gRPC 网关和离线脚本共享同一模型产物 | `已完成/P1` |
| PHX-04 | 新增右对齐 RoPE：不同历史长度下，最新行为落在固定位置 | **已接线**：位置参数贯穿 Transformer；配置可选，发布 config 未声明时保持关闭 | 避免训练和推理因 padding 长度不同而改变行为位置含义 | `已完成/P1` |
| PHX-05 | 新增帖子年龄分桶及候选年龄 embedding | **已接线**：候选曝光/创建时间进入年龄 embedding；发布 checkpoint 参数 shape 匹配 | 模型能区分刚发布和过时内容，改善时效性 | `已完成/P1` |
| PHX-06 | 新增连续行为输入和连续预测头，例如停留时长 | **已接线**：8 维连续输入/预测头可选；发布 gRPC 返回连续 dwell | 可利用“看了多久”而不仅是“是否点击”，也能输出连续停留预测 | `已完成/P1` |
| PHX-07 | 新增可选 IP hash embedding、上下文特征组合 | `mp` 未包含 | 在合法合规的数据前提下可增强地域/网络上下文 | `条件启用/P3`；默认关闭并单独审查隐私 |
| PHX-08 | Retrieval 默认启用线性投影，并兼容无投影的均值表示 | **已迁移**：发布 MLP 参数与均值模式均有测试 | 与发布 checkpoint 对齐，同时保留简化模型能力 | `已完成/P1` |
| PHX-09 | 扩充注意力、RoPE、年龄桶、归一化、模型输出和 retrieval 测试 | **已迁移**：旧/发布 profile、参数名/shape、真实 RPC 均验证 | 保护 checkpoint 兼容和模型输入语义 | `已完成/P1` |
| PHX-10 | README 增加 artifact 解压、运行、定制和模型参数说明 | **已迁移**：明确随机、自训练、发布三条路径及真实 config 参数 | 给用户增加“直接跑预训练模型”的最短路径 | `已完成/P1` |
| PHX-11 | `0bfc279` 只替换 LFS 对象：3,123,149,995 字节变为 2,903,518,802 字节 | **已验证**：通过 LFS batch/Range 获取对象，整包 SHA 与 OID 完全一致；包内 config 和参数 shape 已审计 | 获得最新发布产物并减少约 7.03% 下载量 | `已完成/P1` |

涉及文件：`phoenix/README.md`、`artifacts/oss-phoenix-artifacts.zip`、`grok.py`、`recsys_model.py`、`recsys_retrieval_model.py`、`run_pipeline.py`、`runners.py`、两个模型测试文件。

特别风险（已处置）：根 README 的 256 维、2 层描述与 artifact 不一致。真实 ranker/retrieval `config.json` 和参数 shape 均确认是 128 维、4 层；发布适配器只从 artifact 配置构建模型，Phoenix README 已记录该事实。

### C. Home Mixer 核心结构和业务模型

| ID | 差异及作用 | 建台账时的差异 / 实现说明 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| HM-01 | 将 Query、Candidate、CandidateFeatures、UserFeatures、BrandSafety 等整理为独立模型 | `mp` 模型仍主要位于 `candidate_pipeline/`，并有兼容模块 | 业务字段归属更清楚，后续新增来源不会继续膨胀旧结构 | `已有，合并语义/P3`；迁移行为，不平移内部类型依赖 |
| HM-02 | 新增 `ScoredPostsServer`：只负责候选召回、补全、过滤和帖子打分 | `mp` 的 `HomeMixerServer` 同时承担服务入口和打分编排 | 把“帖子算分”和“最终页面组装”分开，便于独立测试和复用 | `默认保留/P3` |
| HM-03 | 新增 `ForYouCandidatePipeline`：把帖子、广告、关注推荐、Prompt、Push 内容混排 | `mp` 只返回帖子列表 | 为完整产品 Feed 留出明确边界，不污染帖子排序逻辑 | `默认保留/P4`；非帖子来源默认关闭 |
| HM-04 | 新增 `ForYouFeedServer` 和 URT 输出路径 | `mp` 只有公共 gRPC `ScoredPostsService` | 可同时服务算法调用和最终客户端 Feed | `条件启用/P4`；保留现有公开 gRPC 合约 |
| HM-05 | Query 扩展请求设备、地域、话题、历史、缓存、预览、轮询等上下文 | `mp` Query 较小，部分信息由 Demo 客户端提供 | 能表达更多真实刷新场景，减少隐式全局状态 | `默认保留/P3`；按字段来源分组，避免巨型构造函数 |
| HM-06 | Candidate 扩展引用帖、互动数、语言、媒体、话题、安全、共同关注等特征 | `mp` Candidate 只覆盖当前排序所需字段 | 支撑新过滤、上下文排序、品牌安全和结果解释 | `默认保留/P3`；只由对应 Hydrator 写入 |
| HM-07 | `main.rs` / `server.rs` / `lib.rs` 扩展双服务装配、请求构建和调试输出 | `mp` 有自己的可运行装配和 proto | 能接入新能力，但也是冲突最集中的位置 | `已有，合并语义/P3-P4`；最后接线，不直接覆盖 |

### D. Home Mixer 候选来源

| ID | 来源 | 作用 | 建台账时的差异与收益 | 处理建议 |
|---|---|---|---|---|
| SRC-01 | `thunder_source.rs` | 获取关注用户的近期内容 | 已有；吸收新请求条件和 served type 语义 | `已有，合并语义/P3` |
| SRC-02 | `phoenix_source.rs` | 标准 Phoenix 网外召回 | 已有 gRPC 客户端；与新 retrieval sequence、cluster 参数对齐 | `已有，合并语义/P3` |
| SRC-03 | `phoenix_moe_source.rs` | 从 Phoenix MoE 集群召回多专家候选 | `mp` 无；可扩展不同兴趣专家的召回覆盖 | `条件启用/P3` |
| SRC-04 | `phoenix_topics_source.rs` | 按指定话题或新用户关注话题召回 | `mp` 无；支持话题页和新用户冷启动 | `默认保留/P3` |
| SRC-05 | `tweet_mixer_source.rs` | 接入另一套网外候选服务 | `mp` 无；可比较或补充 Phoenix 召回 | `条件启用/P3` |
| SRC-06 | `cached_posts_source.rs` | 服务失败或连续请求时复用缓存候选 | `mp` 无；降低依赖抖动造成的空 Feed | `默认保留/P3` |
| SRC-07 | `scored_posts_source.rs` | 把已打分帖子作为最终 Feed 的一个来源 | 建台账时 `mp` 无双层流水线；该来源是帖子排序与页面混排解耦的连接点 | `默认保留/P4` |
| SRC-08 | `ads_source.rs` | 拉取符合用户和页面条件的广告 | `mp` 无；使 Feed 具备商业内容入口 | `条件启用/P4` |
| SRC-09 | `who_to_follow_source.rs` | 生成关注用户推荐模块 | `mp` 无；扩展 Feed 不只推荐帖子 | `条件启用/P4` |
| SRC-10 | `prompts_source.rs` | 注入引导、提示或运营内容 | `mp` 无；支持运营和产品引导 | `条件启用/P4` |
| SRC-11 | `push_to_home_source.rs` | 将指定内容固定到 Feed 顶部 | `mp` 无；支持通知回流和定向内容 | `条件启用/P4` |

### E. Home Mixer 用户请求补全

| ID | Hydrator | 作用 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| QH-01 | Blocked user IDs | 取得用户主动屏蔽列表 | 防止出现用户已明确拒绝的作者 | `默认保留/P3` |
| QH-02 | Muted user IDs | 取得静音作者列表 | 提升用户控制一致性 | `默认保留/P3` |
| QH-03 | Followed user IDs | 取得关注列表 | Thunder 网内召回和网内标记的基础 | `已有，合并语义/P3` |
| QH-04 | Subscribed user IDs | 取得订阅作者列表 | 支持订阅内容资格和优先级 | `默认保留/P3` |
| QH-05 | Impressed posts | 取得已曝光帖子 ID | 避免短期重复推荐 | `默认保留/P3` |
| QH-06 | Impression Bloom Filter | 以低内存记录更长周期曝光 | 大规模历史下仍能控制重复 | `默认保留/P3` |
| QH-07 | Served history | 读取已下发历史及游标 | 连续刷新时结果更稳定 | `默认保留/P3` |
| QH-08 | Past request timestamps | 读取过去请求时间 | 区分轮询、顶部刷新和长时间回访 | `默认保留/P3` |
| QH-09 | Cached posts | 读取请求缓存候选 | 下游失败时可降级而不是返回空结果 | `默认保留/P3` |
| QH-10 | Retrieval sequence | 构造召回专用行为序列 | 让召回使用合适的长度和聚合方式 | `必须迁移/P1-P3` |
| QH-11 | Scoring sequence | 构造精排专用行为序列 | 召回和精排不再被迫共享同一输入 | `必须迁移/P1-P3` |
| QH-12 | Followed Grok topics | 取得用户主动关注话题 | 支持话题召回和新用户冷启动 | `默认保留/P3` |
| QH-13 | Inferred Grok topics | 取得系统推断兴趣话题 | 用户未主动关注时补足兴趣 | `条件启用/P3` |
| QH-14 | Followed starter packs | 取得用户关注的主题账号包 | 增加上下文特征和冷启动信号 | `条件启用/P3` |
| QH-15 | Mutual follow minhash | 获取用户社交图摘要 | 可计算用户与作者的共同关注相似度 | `条件启用/P3` |
| QH-16 | IP location | 根据 IP 补充位置上下文 | 可支持本地内容，但涉及隐私和合规 | `条件启用/P3`，默认关闭 |
| QH-17 | User demographics | 补充用户人口统计信息 | 可能提升上下文模型，但必须审查公平性 | `条件启用/P3`，默认关闭 |
| QH-18 | Inferred gender | 补充推断性别及置信度 | 有潜在模型收益，也有明显公平性风险 | `条件启用/P3`，默认关闭并单独决策 |

### F. Home Mixer 候选补全

| ID | Hydrator | 作用 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| CH-01 | Core data | 补正文、作者、转帖/回复/引用关系 | 过滤和展示的基础；`mp` 已有基础实现 | `已有，合并语义/P3` |
| CH-02 | Gizmoduck author data | 补作者名称、粉丝等资料并缓存 | 支持展示、作者特征和调试 | `已有，合并语义/P3` |
| CH-03 | In-network marker | 标记候选是否来自关注网络 | 网内/网外权重和结果解释所需 | `已有，合并语义/P3` |
| CH-04 | Subscription | 补订阅作者关系 | 支持订阅内容过滤和策略 | `已有，合并语义/P3` |
| CH-05 | Visibility filtering | 调用安全可见性服务 | 防止删除、违规或不可见内容进入 Feed | `已有，合并语义/P3` |
| CH-06 | Video duration | 补原帖和引用帖视频时长 | 正确计算视频观看权重 | `已有，合并语义/P3` |
| CH-07 | Ads brand safety labels | 汇总原帖/引用帖安全标签并缓存 | 广告不会贴近高风险内容 | `默认保留/P4` |
| CH-08 | Ads brand safety VF | 从可见性结果得到品牌安全判定 | 为广告安全提供另一条判定路径 | `默认保留/P4` |
| CH-09 | Blocked-by-author | 判断作者是否反向屏蔽用户 | 避免推荐用户无法互动的内容 | `默认保留/P3` |
| CH-10 | Engagement counts | 补点赞、回复、转帖、引用数量 | 为上下文模型和结果分析提供热度信息 | `默认保留/P3` |
| CH-11 | Filtered topics | 获取不同实验下的帖子话题 | 支持话题页和用户排除话题 | `默认保留/P3` |
| CH-12 | Following replied users | 找出用户关注的人中谁回复了该对话 | 支持社交证明和回复排序 | `条件启用/P3` |
| CH-13 | Has media | 判断原帖或引用帖是否包含媒体 | 支持媒体特征和过滤 | `默认保留/P3` |
| CH-14 | Language code | 补帖子语言并缓存 | 语言匹配和多语言策略基础 | `默认保留/P3` |
| CH-15 | Mutual-follow Jaccard | 计算用户与作者关注集合相似度 | 为社交相关性提供连续特征 | `条件启用/P3` |
| CH-16 | Quote hydration | 补引用帖正文、作者和媒体关系 | 引用帖不再以不完整信息参与过滤和打分 | `默认保留/P3` |
| CH-17 | Tweet type metrics | 生成帖子类型位图和统计 | 统一识别视频、回复、转帖、媒体等类型 | `默认保留/P3` |

### G. Home Mixer 过滤规则

| ID | Filter | 作用 | 建台账时的差异与收益 | 处理建议 |
|---|---|---|---|---|
| FLT-01 | Age | 删除过旧帖子 | 已有；合并参数化和边界语义 | `已有，合并语义/P3` |
| FLT-02 | Author social graph | 过滤屏蔽、静音等作者关系 | 已有；吸收更完整用户关系 | `已有，合并语义/P3` |
| FLT-03 | Core-data hydration | 删除关键资料缺失候选 | 已有；防止后续组件处理半成品 | `已有，合并语义/P3` |
| FLT-04 | Conversation dedup | 每个对话保留合适代表 | 已有；减少同一对话刷屏 | `已有，合并语义/P3` |
| FLT-05 | Exact duplicate | 删除重复 tweet ID | 已有 | `已有，合并语义/P3` |
| FLT-06 | Ineligible subscription | 删除不符合订阅规则内容 | 已有 | `已有，合并语义/P3` |
| FLT-07 | Muted keyword | 删除命中用户静音词内容 | 已有；补引用/转帖文本语义 | `已有，合并语义/P3` |
| FLT-08 | Previously seen | 删除当前请求已见内容 | 已有 | `已有，合并语义/P3` |
| FLT-09 | Previously seen backup | 主曝光数据不可用时使用备用规则 | `mp` 无；减少重复控制失效 | `默认保留/P3` |
| FLT-10 | Previously served | 删除历史已下发内容 | 已有基础；接入 served history | `已有，合并语义/P3` |
| FLT-11 | Retweet dedup | 原帖和转帖去重 | 已有 | `已有，合并语义/P3` |
| FLT-12 | Self tweet | 按策略过滤用户自己的帖子 | 已有 | `已有，合并语义/P3` |
| FLT-13 | Visibility | 删除安全服务判定不可见内容 | 已有；合并新标签 | `已有，合并语义/P3` |
| FLT-14 | Ancillary visibility | 处理引用、转帖等附属内容不可见情况 | `mp` 无；避免主帖可见但附属内容违规 | `默认保留/P3` |
| FLT-15 | Topic IDs | 按包含、排除、新用户话题和实验映射过滤 | `mp` 无；支撑完整话题推荐 | `默认保留/P3` |
| FLT-16 | New-user topic IDs | 限制新用户冷启动话题候选 | `mp` 无；提高新用户首屏相关性 | `默认保留/P3` |
| FLT-17 | Video | 在请求排除视频时删除视频候选 | `mp` 无显式过滤器 | `默认保留/P3` |

### H. 打分、选择和广告混排

| ID | 差异及作用 | 建台账时的差异 / 实现说明 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| RANK-01 | Phoenix Scorer 支持更多离散行为、连续停留和新请求格式 | 已有 Phoenix gRPC scorer | 对齐新模型输出，避免丢分数字段 | `必须迁移/P1-P3` |
| RANK-02 | `RankingScorer` 组合正负行为权重、分数归一化、作者多样性、网外权重和新用户调整 | `mp` 分散在 Weighted/OON/AuthorDiversity scorer | 行为更完整；但不应复制成一个大类 | `已有，合并语义/P3`；保留为可组合评分策略 |
| RANK-03 | `VMRanker` 接入外部二次排序 | `mp` 有自定义精排策略，但没有该客户端 | 可做模型后重排和实验对比 | `条件启用/P3`；用独立接口适配 |
| SEL-01 | Top-K Selector 返回未入选候选 | `mp` 只返回选中项 | 支持完整候选漏斗和训练日志 | `默认保留/P2-P3` |
| SEL-02 | `BlenderSelector` 按类型拆分 FeedItem，再插入广告、Prompt、关注推荐和置顶内容 | `mp` 只排序帖子 | 建立最终 Feed 组装能力 | `默认保留/P4` |
| ADS-01 | Safe-gap 策略只在品牌安全间隙插广告 | `mp` 无广告 | 降低广告紧邻敏感内容的风险 | `默认保留/P4` |
| ADS-02 | Partition-organic 策略把安全帖子成组包围广告，并检查品牌、账号和关键词 | `mp` 无广告 | 提供第二种可实验的广告混排策略 | `默认保留/P4` |
| ADS-03 | 统一广告间距、首个位置、最小自然内容数、末尾广告清理 | `mp` 无 | 防止广告过密或 Feed 末尾只剩广告 | `默认保留/P4` |

### I. 响应后处理和数据闭环

| ID | SideEffect | 作用 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| SE-01 | Ads injection logging | 记录广告请求、放置和丢弃结果 | 广告问题可追踪 | `条件启用/P5` |
| SE-02 | Client events Kafka | 记录帖子、广告、关注模块、视频和空 Feed 数量 | 建立用户侧交付指标 | `默认保留/P5`；先提供本地日志适配器 |
| SE-03 | For You response stats | 统计最终 Feed 构成和位置 | 判断混排是否符合预期 | `默认保留/P5` |
| SE-04 | Mutual-follow stats | 统计社交相似度特征覆盖 | 判断该特征是否值得长期保留 | `条件启用/P5` |
| SE-05 | Phoenix experiments | 并行调用影子模型并记录各模型分数 | 支持无用户影响的模型比较 | `默认保留/P5` |
| SE-06 | Phoenix request cache | 保存模型请求信息 | 便于复现某次打分 | `默认保留/P5` |
| SE-07 | Publish seen IDs | 发布已见帖子 ID | 构建跨请求去重闭环 | `默认保留/P5` |
| SE-08 | Redis candidate cache | 缓存候选结果 | 依赖异常时可降级 | `默认保留/P5` |
| SE-09 | Reranking Kafka | 发布重排输入/输出 | 支持训练和离线分析 | `条件启用/P5` |
| SE-10 | Scored stats | 统计各阶段分数和候选类型 | 快速发现分数漂移和来源失衡 | `默认保留/P5` |
| SE-11 | Served candidates Kafka | 发布最终下发候选 | 形成训练样本和审计记录 | `默认保留/P5` |
| SE-12 | Update served history | 写入本次已下发历史 | 连续刷新不重复 | `默认保留/P5` |
| SE-13 | Truncate served history | 控制历史大小 | 防止状态无限增长 | `默认保留/P5` |
| SE-14 | Update request timestamps | 保存刷新时间 | 支持刷新节奏判断和非轮询逻辑 | `默认保留/P5` |

### J. Grox 内容理解

| ID | 能力 | 作用 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|---|
| GRX-01 | Engine、Dispatcher、Task Generator、Schedule Context | 从消息流取任务、并发执行、回收结果和优雅退出 | 为内容理解建立独立后台处理服务 | `隔离恢复/P6` |
| GRX-02 | Kafka、消息队列、Strato Loader | 从实时流和存储加载帖子/用户数据 | 可把内容理解接入真实数据 | `隔离恢复/P6`；用本地接口替换内部实现 |
| GRX-03 | ASR 和媒体加载 | 将视频/音频转写并补齐媒体信息 | 多模态帖子不再只看文字 | `默认保留/P6` |
| GRX-04 | Initial Banger 分类 | 判断新帖子是否值得初始放大 | 支持冷启动内容筛选 | `条件启用/P6` |
| GRX-05 | Spam 分类和回复 Spam 流程 | 识别低质量、批量或垃圾内容 | 提升 Feed 和回复质量 | `默认保留/P6` |
| GRX-06 | Post Safety Deluxe | 对帖子做详细安全分类 | 在推荐前增加内容安全信号 | `默认保留/P6` |
| GRX-07 | PTOS 类别和策略分类 | 先识别风险类别，再判断违反的具体策略 | 支持可解释的政策执行 | `默认保留/P6` |
| GRX-08 | Reply Ranking | 使用内容模型给回复排序 | 可扩展对话页质量，不直接耦合主 Feed | `条件启用/P6` |
| GRX-09 | V2/V5 多模态 Embedding | 综合正文、图片、视频、摘要生成向量 | 为 Phoenix 召回提供更强内容表示 | `默认保留/P6` |
| GRX-10 | Post summarizer | 为长帖和多媒体内容生成摘要 | 改善 embedding 输入和内容解释 | `默认保留/P6` |
| GRX-11 | Plans 和 Task Filters | 为 Spam、安全、Embedding、回复排序定义可组合执行链 | 能按场景复用任务，而不是复制脚本 | `默认保留/P6` |
| GRX-12 | Rate limits 和环境禁用规则 | 控制每类任务吞吐及环境开关 | 防止模型服务过载，支持本地/测试禁用 | `默认保留/P6` |
| GRX-13 | Kafka/Manhattan/annotation/embedding sinks | 发布分类、排序、Embedding 和安全标注结果 | 让内容理解结果能被推荐系统消费 | `隔离恢复/P6`；定义本地 Sink 接口 |
| GRX-14 | 监控、trace 和失败结果 | 记录成功、失败、处理时长和上下文 | 生产排错所需 | `默认保留/P6` |

上游 Grox 文件覆盖：6 个 classifier、4 个 data loader、2 个 embedder、2 个 generator、11 个 plan、3 个 schedule、3 个 summarizer、22 个 task，以及 engine、dispatcher、main 和通用库，共 59 个文件。

### K. 发布和文档

| ID | 差异及作用 | 对 `mp` 的好处 | 处理建议 |
|---|---|---|---|
| REL-01 | `.gitattributes` 为 ZIP 配置 Git LFS | 避免 2.9 GB 模型进入普通 Git 对象 | `已完成/P1` |
| REL-02 | 根 README 增加 2026-05 更新摘要 | 用户能发现新入口和新能力 | `已有，合并语义/P1-P6`；每阶段同步，不一次宣称全部可用 |

## File Coverage Map

本节用于证明所有 187 个变更文件均已进入能力台账，而不是只挑选显眼文件。

| 路径组 | 文件数 | 对应能力编号 |
|---|---:|---|
| `.gitattributes`, `README.md` | 2 | REL-01..02 |
| `candidate-pipeline/*.rs` | 9 | CP-01..08 |
| `grox/classifiers/content/*` | 6 | GRX-04..08 |
| `grox/data_loaders/*` | 4 | GRX-02..03 |
| `grox/embedder/*` | 2 | GRX-09 |
| `grox/generators/*` | 2 | GRX-01 |
| `grox/lib/*` | 2 | GRX-01..02 |
| `grox/plans/*` | 11 | GRX-05..11 |
| `grox/schedules/*` | 3 | GRX-01 |
| `grox/summarizer/*` | 3 | GRX-10 |
| `grox/tasks/*` | 22 | GRX-03..14 |
| `grox/{__init__,dispatcher,engine,main}.py` | 4 | GRX-01, GRX-14 |
| `home-mixer/ads/*` | 4 | ADS-01..03 |
| `home-mixer/candidate_hydrators/*` | 18 | CH-01..17 |
| `home-mixer/candidate_pipeline/*` | 3 | HM-02..03 |
| `home-mixer/filters/*` | 18 | FLT-01..17 |
| `home-mixer/models/*` | 7 | HM-01, HM-05..06 |
| `home-mixer/query_hydrators/*` | 19 | QH-01..18 |
| `home-mixer/scorers/*` | 4 | RANK-01..03 |
| `home-mixer/selectors/*` | 3 | SEL-01..02 |
| `home-mixer/side_effects/*` | 15 | SE-01..14 |
| `home-mixer/sources/*` | 12 | SRC-01..11 |
| `home-mixer/{for_you_server,scored_posts_server,lib,main,server}.rs` | 5 | HM-02..07 |
| `phoenix/*`（含 artifact 和测试） | 9 | PHX-01..10 |
| **合计** | **187** | 全部能力编号 |

可用以下命令复核原始路径：

```bash
git diff --name-status \
  aaa167b3de8a674587c53545a43c90eaad360010 \
  e414c171ed68266341193330bc4864bf3f3534e3
```

`0bfc279` 没有增加新路径，只再次修改 `phoenix/artifacts/oss-phoenix-artifacts.zip` 的 LFS 指针，已归入 PHX-11。

## DDD and SOLID Migration Rules

### 业务边界

1. **Recommendation Scoring**：Phoenix 召回、帖子预测、行为加权和帖子 Top-K。
2. **Feed Composition**：帖子、广告、Who to Follow、Prompt、Push-to-Home 的最终位置安排。
3. **Content Understanding**：Grox 的 Spam、安全、Embedding、摘要和回复排序。
4. **Runtime Adapters**：gRPC、Kafka、Redis、模型文件、Demo 数据和未来真实服务客户端。

这些边界通过接口交互。模型代码不读取 Kafka，Feed 混排不直接调用 JAX，Grox 不直接修改 Home Mixer 的内部候选对象。

### 设计约束

- `mp` 的业务代码依赖小接口，Demo、内存实现和生产实现都在启动装配处注入。
- 不把 `xai_feature_switches::Params` 或 `xai_decider::Decider` 作为通用 Candidate Pipeline 的固定类型；改为 `mp` 自己的能力开关接口。
- `RankingScorer` 的业务行为全部保留，但实现上继续拆成行为加权、作者多样性、网内/网外调整等独立策略，避免一个类承担所有排名规则。
- Query 和 Candidate 字段由明确的 Hydrator 负责，禁止多个组件无约束写同一字段。
- 最终 Feed 使用独立 `FeedItem`，不要让广告、Prompt 字段进入帖子 Candidate。
- 外部服务不可用时，通过显式降级结果或关闭能力处理，不在业务逻辑中散落环境判断。
- 兼容层只能存在于边界，并必须有明确的删除条件；不能让两套业务模型长期并行传播。

## Priority Rationale

1. Phoenix 模型语义和 artifact 最先处理，因为其他上游打分行为建立在新模型输出上。
2. Candidate Pipeline 第二步处理，因为后续大量 Hydrator、Source 和 SideEffect 依赖新执行语义。
3. 先完成“纯帖子推荐”纵向链路，再增加最终 Feed 混排，能保持每一步都有用户可见结果。
4. 数据闭环在行为稳定后接入，避免先记录一套随后又变化的数据格式。
5. Grox 最后作为独立服务恢复，因为其缺失依赖最多，但不会从总目标中删除。

## Assumptions and Open Decisions

| 项目 | 状态 | 影响 | 下一步 |
|---|---|---|---|
| `mp` 的首要交付仍是本地一键完整推荐 Demo | 已确认并持续验证 | 所有后续生产集成不得破坏该路径 | 每次 Integration Backlog 交付都运行 `./scripts/run_demo.sh` |
| 最终产品是否需要广告 | 未决 | 不影响 P4-A；决定 Ads Source、广告安全、日志和审计是否进入 P4-B | 产品明确入口和安全责任方后恢复 |
| 是否需要 Who to Follow、Prompt、Push-to-Home | 未决 | 领域类型和混排规则已存在；决定哪些真实 Source 值得接入 | 产品明确用户场景后逐项恢复，不打包默认启用 |
| Grox 本地恢复范围 | **已决** | P6-A 只提供中立 DAG/port；模型、Prompt 和内容结论属于 P6-B | 满足 `p6-grox-recovery-audit.md` 的恢复条件后逐项进入 |
| Phoenix 最新 LFS 包内真实模型配置 | **已验证** | ranker/retrieval 均为 128 维、4 层、4 头、127 历史、64 候选 | SHA、config、参数 shape、离线与 gRPC 证据已记录 |
| `mp` 公共 proto 是否保持向后兼容 | **已决：保持** | 旧 `ScoredPostsService` 与新增 `ForYouFeedService` 并存 | 后续只做 additive 变更，破坏性调整需独立决策 |
| Feature Switch / Decider 的本地替代 | **已决** | 条件组件依赖 `FeatureSwitches` 或显式 Query/装配开关，不依赖 X 内部类型；`HomeMixerFeatures` 统一控制可选外部集成 | MoE 与请求缓存旁路已默认关闭；新增旁路必须提供 typed flag、人工接入条件和主链降级证据 |
| Served history 的本地实现 | **已决并完成** | P5-A 使用有界内存状态验证连续请求行为 | 外部持久化不得改变已验证语义 |
| Kafka、Redis 和外部指标 | 未决/待合同 | 影响跨进程持久化、训练数据、幂等、保留期和运维 | 统一放入 P5-B Integration Backlog |

## Phases

### P0: 固化 `mp` 当前行为

- **目的**：建立迁移前可重复的成功基线。
- **进入条件**：在 `mp` 工作树执行，不覆盖现有未提交文件。
- **阶段规则**：只补测试和记录，不迁移上游生产代码。
- **Todos**：
  - [x] 记录 Rust workspace 编译和测试结果。
    - **Surface**：Rust workspace。
    - **Proof**：`cargo build --workspace`、`cargo test --workspace`。
    - **Depends on**：无。
  - [x] 记录 Phoenix 测试结果。
    - **Surface**：Phoenix 模型和服务。
    - **Proof**：`cd phoenix && uv run pytest`。
    - **Depends on**：Python 依赖已安装。
  - [x] 保存端到端 Demo 的候选数量、来源和得分样例。
    - **Surface**：Thunder + Phoenix + Home Mixer。
    - **Proof**：`./scripts/run_demo.sh` 返回非空且同时有网内/网外帖子。
    - **Depends on**：前两项。
- **退出证据**：三条验证命令及结果已记录。
- **停止条件**：当前 `mp` 本身无法通过基线；先修复基线，禁止叠加迁移。

### P1: Phoenix 正式模型纵向迁移

- **目的**：让离线脚本和现有 gRPC 网关使用同一套发布模型语义和产物。
- **进入条件**：P0 通过；能够获取 LFS artifact。
- **阶段规则**：测试先行；artifact 配置是模型 shape 的唯一事实来源；不改 Home Mixer 公共接口。
- **Todos**：
  - [x] 先移植 PHX-04..09 对应测试并确认失败原因正确。
  - [x] 移植位置编码、帖子年龄、连续行为、retrieval 投影和 loader。
  - [x] 移植 `run_pipeline.py`，接入最新 artifact。
  - [x] 让 `mp` Phoenix gRPC gateway 复用统一 loader。
  - [x] 更新 README，区分随机权重、自训练权重和发布权重三条路径。
- **退出证据**：模型单测、离线 retrieval→ranking、gRPC 合约测试全部通过；旧 Demo 仍通过。
- **停止条件**：artifact `config.json` 与代码 shape 不一致，或新旧 checkpoint 无法明确区分。

### P2: Candidate Pipeline 执行语义

- **目的**：为新增组件提供稳定、可观察、可缓存的执行骨架。
- **进入条件**：P1 通过。
- **阶段规则**：不引用 X 内部配置类型；每项新语义先有最小组件测试；不迁移业务来源。
- **Todos**：
  - [x] 迁移 dependent hydration、统一组件运行包装和错误隔离。
  - [x] 迁移 Filter 统计、CachedHydrator 和 Selector 非入选结果。
  - [x] 增加组件清单、阶段耗时、输入输出数量和空结果测试。
  - [x] 定义 `mp` 自有的能力开关接口和内存测试实现。
- **退出证据**：Candidate Pipeline 单元测试覆盖成功、组件失败、缓存命中、过滤清空和截断场景。
- **停止条件**：为了兼容上游而需要把内部 `xai_*` 类型暴露给业务组件。

### P3-A / P3-B: 帖子推荐能力

- **目的**：P3-A 在只返回帖子 Feed 的情况下吸收可本地验证的 Query、Candidate、Source、Hydrator、Filter 和 Ranking 行为；P3-B 接入真实用户、帖子、安全、Phoenix/Thunder 和可选排序服务。
- **进入条件**：P3-A 依赖 P2；P3-B 还必须具备服务合同、认证、错误语义和测试环境。
- **阶段规则**：一次迁移一条可验证纵向能力；外部服务先使用 trait + Demo 实现；没有真实合同的能力保持关闭，不以字段或 stub 代替生产完成。
- **Todos**：
  - [x] 恢复上游服务入口边界：`HomeMixerConfig -> HomeMixerServer::build/register -> ScoredPostsServer/ForYouFeedServer`；两个 RPC trait 由各自业务 Server 持有。
  - [x] 恢复当前公共合同下的 `QueryBuilder`：统一 proto 映射和请求 ID；`GizmoduckClient::get_viewer_data` 使用 200 ms 超时，只有 `ViewerEligibility::Allowed` 开放网外，Denied/Unknown/错误/超时均限制为仅网内。
  - [x] 建立默认关闭的 `HomeMixerFeatures`，显式控制 MoE Source 和请求缓存 SideEffect；缺少地址时跳过旁路并保留主链。
  - [x] 建立 ScoredPosts 业务模型和服务边界，同时以 additive proto 字段保持旧 gRPC 客户端兼容。
  - [x] 合并 Retrieval/Scoring sequence 和 Phoenix/Thunder source。
  - [ ] 按 QH、CH、FLT、SRC 编号逐项迁移，并更新本台账状态；默认能力已完成，条件外部能力见本节前的残余清单。
  - [x] 将当前可获得 Phoenix 输出的 Ranking 行为实现为可组合策略；未知 VM/连续动作不伪造。
  - [x] 增加话题、缓存降级、曝光去重、引用帖和视频路径测试。
- **退出证据**：P3-A 以组件清单、ScoredPosts/话题/缓存 Demo 和定向测试验收；P3-B 只有在对应真实服务合同和环境测试通过后才能关闭。
- **停止条件**：某字段没有明确来源、两个组件争夺同一字段所有权，或外部服务没有可验证错误语义。

### P4-A / P4-B: 最终 Feed 混排

- **目的**：P4-A 建立独立 FeedItem、ForYou 服务和纯混排规则；P4-B 按真实产品入口接入广告、关注推荐、Prompt 和 Push-to-Home。
- **进入条件**：P4-A 依赖 P3-A；P4-B 还需要产品决定、真实 Source 合同和内容/广告安全责任方。
- **阶段规则**：帖子打分服务不认识非帖子内容；每类内容独立 Source；广告没有真实品牌安全判断时必须保持关闭。
- **Todos**：
  - [x] 引入 `FeedItem` 和独立 ForYou Feed 服务；旧 ScoredPosts RPC 保持兼容。
  - [x] 迁移 BlenderSelector 和 Safe-gap/Partition-organic 两种纯广告策略，默认关闭。
  - [ ] 迁移生产广告安全 Hydrator 和 Ads adapter；port、显式 verdict 与 disabled adapter 已完成，真实集成见 P4-B。
  - [ ] 按需求接入 Who to Follow、Prompts 和 Push-to-Home；领域类型、混排位置和测试 Source 已完成，真实服务见 P4-B。
  - [x] 为 FeedItem 增加位置、数量、降级、空来源和安全缺失测试。
- **退出证据**：P4-A 以自然帖子顺序、非帖子测试 Source、位置规则、默认关闭和安全缺失测试验收；P4-B 只有在真实 Source 与安全/审核环境验收通过后才能关闭。
- **停止条件**：非帖子内容需要修改 PostCandidate，或没有可测试的安全判定、内容审核和降级策略。

### P5-A / P5-B: 状态和反馈闭环

- **目的**：P5-A 用内存/日志实现验证连续刷新和最终 Feed 统计；P5-B 接入跨进程持久化、事件、实验、监控和训练数据。
- **进入条件**：P5-A 依赖 P3-A；记录最终 Feed 类型时依赖 P4-A。P5-B 还需要消费者、schema、幂等、保留期和隐私边界。
- **阶段规则**：本地无 I/O 状态可以在响应前提交；外部 SideEffect 不能阻塞主响应。先固定本地语义，再接真实 Kafka/Redis。
- **Todos**：
  - [x] 迁移本地 served history、request timestamps 和状态截断；外部 seen IDs/候选缓存写回见 P5-B。
  - [x] 迁移本地最终 Feed 构成/位置统计；客户端事件和外部分数指标见 P5-B。
  - [ ] 迁移 Phoenix shadow experiment 和 reranking 数据出口；等待消费者和 schema。
  - [x] 为 SideEffect 失败、连续请求和状态截断增加测试；外部重试/幂等在接 Kafka/Redis 时验收。
- **退出证据**：P5-A 以连续请求差异、单用户/全局状态截断、Feed 统计和 sink 失败隔离测试验收；P5-B 只有在真实存储/事件链路通过重试、幂等和恢复测试后才能关闭。
- **停止条件**：记录的数据没有消费者、schema、保留期限、隐私边界或失败恢复责任方。

### P6-A / P6-B: Grox 独立恢复

- **目的**：P6-A 审计上游 59 个文件并恢复不依赖私有模型的中立执行骨架；P6-B 在模型、Prompt、数据和政策合同齐备后，逐项恢复真实内容理解能力。
- **进入条件**：P6-A 依赖 P3-A 稳定；P6-B 必须满足 `p6-grox-recovery-audit.md` 的合法访问、版本化合同、fixture、Source/Sink 和隐私安全条件。
- **阶段规则**：Grox 单独构建和部署；不直接依赖 Home Mixer 内部模型；没有模型、Prompt 或政策证据时不得输出 Spam、安全、Embedding、摘要或回复排序结论。
- **Todos**：
  - [x] 列出全部缺失的 `grox.*`、`monitor.*` 和内部存储/模型依赖；不可合法替换的项进入 P6-B。
  - [x] 跑通中立 `WorkItem→eligibility DAG→WorkResult→本地 Sink` 切片；没有模型/Prompt 时不伪造 Spam 分类。
  - [ ] 再迁移安全、Embedding、摘要和回复排序计划。
  - [ ] 最后接消息流、限流、监控和生产 Sink。
  - [ ] 通过稳定接口把内容标签或 embedding 提供给 Home Mixer/Phoenix。
- **退出证据**：P6-A 以独立 DAG、skip/failure 信封、环检测、Source/Sink port、10 项测试和 JSON Demo 验收；P6-B 按 classifier/embedder/summarizer 的真实输出合同逐项验收，不能由 P6-A 代替。
- **停止条件**：模型、Prompt、政策或数据合同无法合法获得，或能力无法脱离 X 内部服务运行。

## Dry-Run Findings

- 直接从 P1 跳到广告混排会缺少新 Candidate/Query 字段和品牌安全信号，阶段顺序不可交换。
- 直接复制上游 `RankingScorer` 的私有 feature-switch 实现会重新引入不可构建依赖；本地保留 Weighted/AuthorDiversity/OON 行为，并由同名 facade 提供上游装配边界。
- 直接复制 `PipelineQuery` 会引入 X 内部实验类型，应先在 P2 定义本地开关接口。
- 最新 LFS 对象已在隔离目录完成整包 SHA、config、shape、离线和 gRPC 验证；仓库仍只保存标准 LFS 指针，不提交 2.9 GB 二进制。
- Grox 缺失的不是一个依赖，而是一组模型、配置、Prompt、数据类型和监控模块，因此必须作为独立恢复项目。
- P0-P2 与 P3-A/P4-A/P5-A/P6-A 新增行为均已有 Python/Rust 定向测试；最终数量以本次 Final Validation 的全套输出为准。

## Final Validation

完成全部已批准阶段后，至少执行：

```bash
cargo build --workspace
cargo test --workspace

cd phoenix
uv run pytest
uv run run_pipeline.py --artifacts_dir artifacts/oss-phoenix-artifacts
cd ..

./scripts/run_demo.sh
```

当前 Rust 验证结果（2026-08-08）：

- `cargo test --workspace`：13 个套件，168 项通过。
- `cargo test -p home-mixer --all-targets`：5 个套件，146 项通过。
- `cargo test -p xai_candidate_pipeline`：18 项通过。
- `cargo test -p home-mixer --test p4_final_feed`：25 项通过。
- `cargo clippy -p home-mixer --all-targets -- -W clippy::all`：0 error，0 warning。
- `./scripts/run_demo.sh`：ScoredPosts 返回 50 条（10 网内 + 40 网外），通过；标准 Phoenix 与 Topics 的同分候选按上游 Source 顺序稳定截断。
- `./scripts/run_demo.sh --final-feed`：ForYou Feed 返回 50 条（10 网内 + 40 网外），通过。
- `./scripts/run_demo.sh --cached-posts 8`：脚本只在显式 Demo 下开启 unsigned fixture，返回 8 条请求缓存候选。
- additive RPC wire 验收：Debug 默认返回 `Unavailable`；启用后错误 token 返回 `PermissionDenied`；正确 token 返回 50 posts 和 600 retrieved / 4 filtered / 50 selected。`GetForYouFeedV2` 返回 50 items。
- 运行模式验收：`production_ready` 在调用方身份、Viewer、UAS、Strato、TES、Gizmoduck、VF、Phoenix、Thunder 合同未全部闭合时退出 1；非 Demo 开启 unsigned cached posts 同样退出 1。
- 独立端口降级验收：未启动 Thunder/Phoenix 时，Home Mixer reflection 可列出两个业务服务，gzip accept header 请求可返回 50 条 Demo 话题候选。
- 本次触及的 Home Mixer/Candidate Pipeline Rust 文件通过独立 `rustfmt`，`git diff --check` 通过。
- `cargo fmt --all -- --check` 已能解析全仓，但仍报告多个既有 Thunder 文件的 rustfmt 差异；未批量格式化这些无关用户改动。
- `cargo check -p thunder --all-targets --all-features` 仍被 legacy listener 的私有 `xai_kafka`、`xai_thunder_proto` 和 `crate::schema` 依赖阻塞；受支持的默认 `cargo test -p thunder` 为 3 个套件、2 项通过。

- Phoenix 当前代码验证：`uv run pytest -q` 88 项通过；变更文件通过 Ruff；shared orchestration、length mismatch、offline JSON/proto UAS、固定 impression timestamp 和 O(1) topic lookup 均有回归。
- 当前工作树中的 2.9 GB artifact 仅为 LFS pointer（OID `fbc6017d...a83dac`），本轮未重复执行真实 artifact；下列历史验收仍对应同一 OID。

需要模型 artifact 或完整外部运行环境的最近一次历史验收（2026-07-22）：

- `cargo build --workspace`：通过（仅 3 个既有 dead-code 警告）。
- `uv run --project phoenix --group service pytest -q phoenix/tests`：82 项通过。
- `uv run --project grox --group dev pytest -q grox/tests`：10 项通过；独立 JSON Demo 成功。
- `uvx ruff check phoenix grox/src grox/tests`、Python `compileall`、`bash -n scripts/run_demo.sh`、`git diff --check`：全部通过。
- 真实发布 artifact：大小 2,903,518,802，SHA-256 与 LFS OID 一致；84,564 条语料离线召回→精排成功，Top-5 结果可重复。
- 真实发布 gRPC：Retrieve 返回 5 条，Predict 为 3 条候选返回 19 个离散槽位和 2 个公共连续槽位，dwell 值非空且有限。
- 默认 ScoredPosts Demo：50 条，12 网内 + 38 网外；P4 ForYou Feed Demo：50 条，12 网内 + 38 网外；话题 Demo：50 条 `Phoenix 话题`；缓存 Demo：8 条 `请求缓存`。
- 验证后端口 `50051`、`50052`、`50053`、`50153` 全部释放。

还需人工检查：

- 组件清单与本台账启用状态一致。
- Demo 同时包含网内和网外帖子。
- 使用发布 checkpoint 时结果可重复且不存在 shape 错误。
- 广告等非帖子能力未获批准时不会出现在默认 Feed。
- 任何暂缓能力仍保留编号、原因和重新进入迁移的条件。

## First Execution Step for Integration Backlog

Home Mixer `HM-E1..E6`、Phoenix `PHX-E1/E2` 与 Thunder portable boundary 已完成。下一步不再继续制造 production 外观：真实 UAS、用户关系、TES、VF、Phoenix/Thunder、Kafka/Redis 或 Grox model/Prompt 只有在 owner、schema、认证、超时、错误、保留期、隐私和恢复合同齐备后才进入对应纵向能力。未批准的非帖子内容继续关闭。
