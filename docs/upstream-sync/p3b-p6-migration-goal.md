# Goal Document: P3-B 到 P6 的可运行迁移

## Go / No-Go

- **Judgment**: Go
- **Reason**: P3 的默认帖子推荐链路已由本地 Demo、Rust 测试和 Phoenix 测试验证。P4、P5 的核心领域行为可在没有生产广告、Kafka 或 Redis 的情况下迁移和验证；P6 先按独立可运行切片恢复，不把缺失的内部基础设施伪装为已完成能力。

## Target Outcome

在保持 `ScoredPostsService` 兼容、帖子排序边界不被污染的前提下，完成：

1. P4 的最终 Feed 领域模型、自然帖子桥接、确定性混排、独立 gRPC 服务和本地 Demo。
2. P5 的本地可观测与连续请求状态闭环，且 SideEffect 失败不影响响应。
3. P6 的 Grox 独立恢复基线：明确可运行最小切片、依赖缺口和与 Home Mixer/Phoenix 的稳定输出边界。

## Goal Definition

- **Type**: technical / delivery
- **Boundary**: 迁移可通过本仓库模型、port、Demo adapter 或内存实现验证的行为；公共 protobuf 只做 additive 变更。
- **Non-goals**:
  - 不接入真实广告竞价、计费、预算或品牌安全供应商。
  - 不接入 Kafka、Redis、Strato、Manhattan、生产 UAS/TES/VF/Gizmoduck 或 X 私有服务。
  - 不将广告、Prompt、Who to Follow 或 Push 内容塞入 `PostCandidate`。
  - 不声称 Grox 的模型、Prompt 或生产数据流已经恢复。
- **Deferred work**: 见 [Integration Backlog](#integration-backlog)。
- **Verification rule**: 每个阶段必须有行为测试，默认 Demo 必须仍返回 P3 的自然帖子；新 Feed 场景必须能用公开 gRPC 或本地服务证明来源、顺序、降级和状态。
- **Evidence source**: Rust 单元/集成测试、Phoenix 测试、gRPC Demo、组件日志、`git diff --check`。
- **Pass criteria**: P4/P5 每项已启用功能都有通过测试和端到端路径；P6 每个被标为完成的切片可以在本地独立运行。
- **Confidence note**: 本地证据证明领域合同和降级行为，不等价于生产外部服务的可用性、安全性或容量。
- **Judgment owner**: 本仓库自动化测试和端到端 Demo；生产集成由提供相应服务合同与凭据的一方验收。

## Current State

- P0、P1、P2 已完成；P3-A 默认帖子推荐、话题和缓存降级已完成并验证。
- P3-B 的生产 UAS、用户关系、TES、VF、Gizmoduck、Phoenix/Thunder 部署仍是 stub、配置或外部服务待接入状态。
- `ScoredPostsServer` 仍是帖子推荐边界；公共协议已 additive 新增 `ForYouFeedService`，旧 `ScoredPostsService` 不变。
- P4-A 已建立独立 `FeedItem`、`ForYouFeedServer`、ScoredPosts bridge、Blender 与 disabled-first Ads port。
- P5-A 已接入全局/单用户双重有界的内存 served history/request timestamps 和最终 Feed 构成统计；本地无 I/O 状态在响应前提交，外部写回仍为非阻塞 P5-B。
- P6-A 已建立独立 `grox` Python 包，恢复 eligibility-gated DAG、失败信封、Source/Sink port 和无模型结论的本地 Demo。
- 现有 Candidate Pipeline 已支持异步 Source、同步 Filter、失败观测、selected/non-selected 与非阻塞 SideEffect。

## Plan Rewrite Notes

| Existing item | Decision | Reason |
|---|---|---|
| P3-B 外部生产数据面 | merge | 与 P4/P5 的生产广告、事件和状态存储一样都依赖服务合同、认证、超时和数据治理，应作为一个恢复工作包管理。 |
| P4 FeedItem / ForYou 服务 / 混排 | keep now | 都是纯领域和协议工作，可本地验证，不能因为广告服务未就绪而搁置。 |
| P4 Ads Source 与品牌安全供应商 | defer | 必须具备真实安全判定后才能启用广告。 |
| P5 本地状态与日志 | keep now | 可验证连续请求、失败隔离和 Feed 构成，不需要 Kafka/Redis。 |
| P5 Kafka/Redis 写回 | defer | 需要消费者、保留策略、幂等键、凭据和运维合同。 |
| P6 Grox | reorder | 先形成独立最小切片和依赖清单，再决定具体模型/Prompt/数据流接入。 |

## Drift Diagnosis

- **Goal drift**: 将“导入上游文件”作为完成标准会把私有基础设施缺失隐藏起来；本目标以可运行行为和明确恢复条件为标准。
- **Phase drift**: P4 的纯混排逻辑与广告服务接入被混在同一阶段；现在按可迁移和待集成拆分。
- **Validation drift**: 新增字段、Source 或 trait 不算启用；必须进入真实服务路径或本地 Demo。
- **Compatibility drift**: `ScoredPostsService` 是已存在公共合同，P4 必须新增服务而不是重解释旧响应。
- **Cleanup drift**: 不为迁移顺手重写 P3 或无关 crate。

## Priority Rationale

- P4-A 先建立最终 Feed 边界，P5 才能正确记录最终类型与位置；因此不先做 P5 的最终 Feed 指标。
- 广告品牌安全与广告服务没有真实输入时不应假装可用，故留在集成包。
- Grox 与 Home Mixer 内部模型隔离，能独立验证的最小工作流可在 P5 后推进，不阻塞 Feed。

## Integration Backlog

这些能力统一暂缓到“生产数据面与外部服务集成”工作包。恢复任一项前，必须提供服务所有者、接口/Schema、认证方式、超时/重试、错误语义、数据保留与测试环境。

| Work item | Origin | Purpose | Enablement and acceptance condition |
|---|---|---|---|
| 用户行为序列 UAS | P3-B | Phoenix 个性化 retrieval/ranking 历史 | 可读行为存储、字段映射、空历史策略；测试用户可验证历史改变模型输入。 |
| 用户关系与请求缓存 | P3-B/P5 | 关注、屏蔽、静音、订阅、连续请求去重 | 关系/状态服务合同；屏蔽与静音故障策略必须 fail-closed 或明确拒绝。 |
| TES 与作者资料 | P3-B | 正文、引用关系、语言、媒体、订阅、作者资料 | 批量 API 与字段所有权；缺核心数据时的候选丢弃策略有测试。 |
| Visibility Filtering | P3-B | 候选及附属内容安全 | 真实安全判定、认证、超时策略；错误时不能默认放行。 |
| Phoenix / Thunder 生产部署 | P3-B | 网内和网外候选、模型打分 | 可访问地址、模型版本、认证、容量和降级演练。 |
| Topic / MoE / TweetMixer / VM Ranker | P3-B | 可选召回和二次排序增强 | 独立服务合同与可量化增量价值；未满足时默认关闭。 |
| IP、人口统计、推断性别 | P3-B | 可选上下文特征 | 隐私、同意、公平性与保留策略审查通过；默认关闭。 |
| Ads Source / 广告安全 | P4-B | 广告候选与安全插入 | 广告合同、品牌安全判定、审计日志和无安全判定时的禁用证明。 |
| Who to Follow / Prompts / Push | P4-B | 非帖子 Feed 项 | 产品入口、服务合同、内容审核及单独降级策略。 |
| Kafka / Redis / 外部指标 | P5-B | 事件、缓存、served history、训练数据 | 消费者、schema、幂等键、保留期、PII 策略和失败恢复演练。 |
| Grox 模型 / Prompt / 流数据 | P6-B | 内容理解和输出标签/embedding | 合法可用的模型/Prompt、数据输入和 sink 合同；先独立验收再接推荐。 |

## Phases

### Phase 1: P4-A 最终 Feed 领域边界

- **Purpose**: 在不改变 P3 帖子合同的情况下建立最终 Feed 的独立类型和服务。
- **Entry condition**: P3-A 默认 Demo 通过。
- **Phase rules**:
  - `FeedItem` 只能引用 `ScoredPost` 或自己的非帖子 payload；`PostCandidate` 不得感知 P4 类型。
  - 新公共 RPC 与 message 只增加字段/服务，既有 `ScoredPostsService` 不变。
  - 非帖子 Source 先由测试和 Demo adapter 驱动，默认关闭。
- **Todos**:
  - [x] 新增 `FeedItem`、`ForYouFeedResponse` 和独立服务；请求复用 additive-compatible `ScoredPostsQuery`。
    - **Surface**: protobuf、Home Mixer 服务边界。
    - **Proof**: 旧 ScoredPosts 客户端和新 ForYou 客户端均可编译/调用。
    - **Depends on**: none.
  - [x] 实现 `ScoredPostsSource`，保留 P3 帖子的排序和来源。
    - **Surface**: P4 application source。
    - **Proof**: 单元测试断言顺序和字段保持。
    - **Depends on**: P3 `ScoredPostsServer`。
  - [x] 实现纯 `BlenderSelector`，支持自然帖子、受控 Prompt/WhoToFollow/Push 测试项和确定性位置规则。
    - **Surface**: P4 selector/domain。
    - **Proof**: 位置、数量、空来源、冲突优先级测试。
    - **Depends on**: FeedItem。
- **Exit proof**: 默认 ForYou gRPC Demo 与 ScoredPosts 返回相同自然帖子顺序；模拟非帖子项按规则插入。
- **Stop condition**: 任一非帖子类型需要修改 P3 `PostCandidate` 或重算 Phoenix 分数。

### Phase 2: P4-A 广告混排规则和关闭态

- **Purpose**: 迁移可纯函数验证的广告位置策略，但不启用真实广告。
- **Entry condition**: Phase 1 通过。
- **Phase rules**:
  - 广告默认关闭；没有品牌安全 verdict 的帖子不构成可插入广告的安全间隙。
  - 不创建虚假的广告竞价或安全标签。
- **Todos**:
  - [x] 实现首位、间距、最小自然帖子、末尾广告清理和安全间隙规则。
    - **Surface**: Feed blender。
    - **Proof**: 纯单元测试覆盖边界和拒绝插入。
    - **Depends on**: Phase 1。
  - [x] 定义 Ads Source port 和 disabled adapter；brand safety 以 additive verdict 表达，生产 Hydrator 留在 Integration Backlog。
    - **Surface**: P4 integration boundary。
    - **Proof**: 未配置时 ForYou Feed 等价于纯自然 Feed。
    - **Depends on**: Phase 1。
- **Exit proof**: 规则测试通过，未接广告服务时不会输出广告。
- **Stop condition**: 规则需要猜测真实品牌安全结果。

### Phase 3: P5-A 本地状态和观测闭环

- **Purpose**: 为最终 Feed 增加内存/日志实现的 served history、请求时间和构成统计。
- **Entry condition**: Phase 1 通过。
- **Phase rules**:
  - 外部 SideEffect 不阻塞响应；失败必须记录。本地无 I/O 一致性提交可在返回前完成，保证紧接请求读取最新 served history。
  - 本地状态同时限制单用户历史和总用户数；Kafka/Redis 保持在 Integration Backlog。
- **Todos**:
  - [x] 实现本地 served history 与请求时间 port。
    - **Surface**: state adapters / query hydrators。
    - **Proof**: 连续请求不无条件重复同一结果。
    - **Depends on**: P4 FeedItem。
  - [x] 实现 Feed 构成和位置统计；生产指标 Source 维度留待真实 adapter 注入。
    - **Surface**: side effects / metrics log。
    - **Proof**: 测试断言记录内容且故障不影响响应。
    - **Depends on**: P4 blender。
- **Exit proof**: 两次本地 ForYou 请求展示可验证状态差异，SideEffect 失败隔离测试通过。
- **Stop condition**: 状态需要外部持久性才能定义正确语义。

### Phase 4: P6-A Grox 独立最小切片

- **Purpose**: 建立不依赖 Home Mixer 内部对象的内容理解入口、输出合同和缺失依赖清单。
- **Entry condition**: P4/P5 不再需要修改 P3 核心。
- **Phase rules**:
  - 不把未获得的模型、Prompt 或安全结论伪装成结果。
  - Grox 输出只经稳定 port 提供标签或 embedding，不反向读取 Home Mixer 内部模型。
- **Todos**:
  - [x] 审计上游 59 个 Grox 文件和缺失依赖，记录可恢复/不可恢复边界。
    - **Surface**: docs / standalone crate or service boundary。
    - **Proof**: 每个顶层能力有输入、输出、依赖、运行条件和处置。
    - **Depends on**: none.
  - [x] 恢复 eligibility-gated DAG 本地工作流，不依赖私有模型且不输出安全/embedding/分类结论。
    - **Surface**: Grox standalone slice。
    - **Proof**: 本地 test/command 及输出 artifact；否则记录客观 blocker。
    - **Depends on**: 审计结论。
- **Exit proof**: 至少一个独立切片真实可运行，或一个精确、可恢复的阻塞清单取代模糊“Grox 未完成”。
- **Stop condition**: 所需模型、Prompt 或数据合同不具备合法可用来源。

## Dry-Run Findings

- P4 的协议和纯 selector 可以现在迁移；广告与品牌安全接入必须延后，否则“Demo 广告”会造成错误的安全完成声明。
- P5 最终 Feed 统计依赖 P4 `FeedItem`，所以不先做 P5 的响应构成事件。
- P6 上游依赖范围大于单个服务适配器，必须先审计、隔离并选择最小切片。
- P3-B 和 P4-B/P5-B 共享服务发现、认证、超时、数据治理和环境验收问题，合并为 Integration Backlog 能减少重复接线，但不应阻挡纯迁移工作。

## Execution Evidence

- P4/P5 定向测试：15 项通过，覆盖自然顺序、模块位置、单一 Push、两种广告策略、缺失安全判定、disabled Ads port、单用户/全局状态截断、无等待连续请求和统计失败隔离。
- Home Mixer：50 项通过；Rust workspace 共 12 个套件、64 项通过；`ForYouFeedService` 和旧 `ScoredPostsService` 均完成一键 Demo，均返回 50 条自然帖子（12 网内 + 38 网外）。
- Phoenix：82 项通过；P4/P5 未改变发布模型和 gRPC 评分合同。
- P6：10 项通过；`python -m grox.demo` 输出结构和文本元数据确定、运行时间戳不同的 JSON，不包含安全、embedding 或 Spam 结论。
- 外部生产服务、广告安全提供方、Kafka/Redis、Grox 模型与 Prompt 均未标记完成，仍在 Integration Backlog。

## Final Validation

```bash
cargo fmt -p home-mixer -p xai_candidate_pipeline -p x-algorithm-proto
cargo test --workspace
cargo build --workspace
uv run --project phoenix --group service pytest -q phoenix/tests
uvx ruff check phoenix
./scripts/run_demo.sh
git diff --check
```

另外验证：旧 `ScoredPostsService` 保持可用；默认 ForYou Feed 与 P3 自然帖子顺序相同；所有外部 Source 关闭时不产生广告或非帖子项目；Integration Backlog 的条目没有被标记为生产已完成。

## First Execution Step

为 `FeedItem` 与 `ForYouFeedServer` 写一个失败测试：自然帖子以原顺序映射成最终 Feed，而没有显式启用的非帖子 Source 不改变输出。
