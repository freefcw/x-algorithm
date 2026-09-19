# home-mixer 总览手册

这是一份面向“一次读懂 `home-mixer`”的主文档。

如果只读一篇，先读这一篇；如果需要查细节，再跳到附录索引 [appendices.md](./appendices.md)。

## 1. 一句话定义

`home-mixer` 是首页 Feed 的在线编排层：它接收客户端请求，补齐用户上下文，并行召回网内与网外候选，完成候选补全、过滤、打分、选择和安全后处理，最终返回一页已排序结果。

```mermaid
flowchart LR
    Client["客户端"] --> HM["home-mixer"]
    HM --> Q["查询补全"]
    Q --> R["多路召回<br/>网内（mrpyq / Thunder）+ Phoenix + 兜底"]
    R --> H["候选补全"]
    H --> F["过滤"]
    F --> S["打分"]
    S --> K["TopK 选择"]
    K --> P["后处理<br/>VF + 会话去重"]
    P --> Resp["ScoredPostsResponse / ForYouFeedResponse"]
```

## 2. 系统边界

### 2.1 `home-mixer` 负责什么

- 接收 `ScoredPostsService` 的 Get/Debug 请求和 `ForYouFeedService` 的 legacy/V2 请求
- 由 `QueryBuilder` 构造内部 `ScoredPostsQuery` 与请求/预测身份
- 通过上游命名 Query Hydrator owners 获取 scoring/retrieval sequence 和 user feature fields
- 从 mrpyq 网内关注收件箱、Phoenix 和真实业务兜底池取候选
- 对候选做补全、过滤、打分、选择
- 响应前等待异步记录本次下发的帖子（`ServedPersistence`），再返回 `ScoredPost`

### 2.2 `home-mixer` 不负责什么

- 帖子事件消费与网内缓存维护：`thunder/`
- 模型训练和推理实现：`phoenix/`
- 通用 pipeline 框架：`candidate-pipeline/`
- proto 生成：`proto/`

## 3. 一次请求怎么流动

请求入口的 tonic trait 实现在 `home-mixer/server.rs`（`impl ScoredPostsService for ScoredPostsServer`、`impl ForYouFeedService for ForYouFeedServer`），打分与响应映射在 `scored_posts_server.rs`，核心流程固定：

1. `QueryBuilder` 校验并映射 proto 请求
2. 调用内层 `PhoenixCandidatePipeline::execute()`
3. 将 scored candidates 映射为 `ScoredPost`
4. ForYou 请求再进入外层 `ForYouCandidatePipeline`；Debug 请求从同一次内层执行返回 typed stage data

```mermaid
sequenceDiagram
    participant C as Client
    participant S as HomeMixerServer
    participant P as PhoenixCandidatePipeline

    C->>S: GetScoredPosts(query)
    S->>S: 校验 + 构造内部 query
    S->>P: execute(query)
    P-->>S: PipelineResult
    S-->>C: ScoredPostsResponse
```

## 4. Pipeline 的真实阶段

`home-mixer` 的业务装配在 `home-mixer/candidate_pipeline/phoenix_candidate_pipeline.rs`，执行语义由 `candidate-pipeline/candidate_pipeline.rs` 提供。

### 4.1 阶段顺序

1. Query Hydrators
2. Sources
3. Candidate Hydrators
4. Pre-selection Filters
5. Scorers
6. Selector
7. Post-selection Hydrator
8. Post-selection Filters
9. Side Effect

### 4.2 并行与串行

- 并行：QueryHydrator、Source、Hydrator、SideEffect
- 串行：Filter、Scorer、Selector、Post-selection Filter

```mermaid
flowchart TD
    A["Query Hydrators"] --> B["Sources"]
    B --> C["Candidate Hydrators"]
    C --> D["Pre-selection Filters"]
    D --> E["Scorers"]
    E --> F["Selector"]
    F --> G["Post-selection Hydrator"]
    G --> H["Post-selection Filters"]
    H --> I["Side Effect"]
```

## 5. 两个核心数据对象

### 5.1 `ScoredPostsQuery`

它是请求上下文容器，包含：

- 原始请求字段：用户、语言、国家、已看过、已投递、翻页标记
- hydrated 字段：`user_action_sequence`、`user_features`
- 追踪字段：`request_id`

### 5.2 `PostCandidate`

它是候选共享状态容器，包含：

- 标识与关系：`tweet_id`、`author_id`、reply/retweet 关系、`ancestors`
- 内容与展示：`tweet_text`、`video_duration_ms`、screen name
- 排序：`phoenix_scores`、`weighted_score`、`score`
- 来源与安全：`served_type`、`in_network`、`visibility_decision`

## 6. 召回策略

`home-mixer` 默认是多路召回，演示里还会加上话题源。

### 6.1 ThunderSource

- 负责网内候选；名字沿用上游，实际依赖 `InNetworkPostsClient` 和 mrpyq NETWORK 关注收件箱（`MRPYQ_RECOMMENDATION_DATA_ADDR` 必填）
- 真实候选只带帖子 ID，作者 / 正文 / 时间靠 TES（同一 mrpyq 服务）补全
- 在来源处就标 `in_network = Some(true)`
- 请求带未签名 cached posts 时关闭

### 6.2 PhoenixSource

- 负责网外候选
- 输入依赖 retrieval sequence
- 只有在 Phoenix client contract 与服务端协议完成适配、并配置 `PHOENIX_RETRIEVAL_GRPC_ADDR` 后才可接入
- 当前 xrex serving contract 尚未完成适配；未配置或不可用时跳过这一路
- 网内限定、严格话题、或已有 cached posts 时关闭

### 6.3 FallbackSource

- 负责真实业务 fallback 池候选，在来源处标 `in_network = Some(false)`
- 网内限定或已有 cached posts 时关闭

产品默认是全网 For You，网络范围只有一个真源：请求的 `in_network_only`。只有它显式为 `true` 才切到网内专用路径并关闭 `PhoenixSource` 与 `FallbackSource`。QueryBuilder 不依赖 Gizmoduck viewer RPC；Gizmoduck 只在候选水合阶段补作者资料，当前真实 adapter 的可用性以部署验收为准。`CachedPostsSource` 只接受显式、已签名的测试 fixture。

## 7. 补全、过滤和排序

### 7.1 Candidate Hydrators

当前主要补：

- `in_network`（来源未标定时）
- `author_id`（mrpyq 候选来源留空，由 TES 补回）、`tweet_text`、`created_at_ms`
- `recommendation_eligible`（mrpyq 一级不可推荐标志）
- `retweeted_*`
- `video_duration_ms`
- `author_screen_name`（取决于作者资料 adapter 是否接入）
- `visibility_decision`（后补全，区分 Allowed / Restricted / Unchecked / Unavailable）

### 7.2 Filters

当前过滤链主要解决：

- 重复内容
- 内容不完整
- 业务一级不可推荐（删除 / 未公开 / 审核未过）
- 过旧内容（`created_at_ms`，缺失时回退 ObjectId 时间戳）
- viewer 自己的内容
- 已看过 / 已投递
- 屏蔽关键词
- 拉黑 / 静音作者
- VF 删除（未验证的候选默认也删除）
- 会话级去重

引用 / 转推 / 订阅三类产品不存在的专用过滤器已按 U5 删除。

### 7.3 Scorers

排序链为：

1. `PhoenixScorer`
2. `RankingScorer`（内部依次组合 Weighted、AuthorDiversity、OON 行为）
3. `RuleFallbackScorer`（Phoenix 头缺失时整批改用规则分）
4. 可选 `VMRanker`、`AuthorColdStartScorer`（需真实 adapter 验收并显式开关）

含义是：

- 先预测行为概率
- 再加权合成相关性
- 再做作者多样性衰减
- 最后给网外内容降权；网内回复/转发默认也乘同一因子
- 模型不可用（无行为序列、超时、契约校验失败）时整批换成“新鲜度 + 网内 + 互动数”的规则分

## 8. 网内召回源为什么重要

网内源是 mrpyq 关注收件箱，对 `home-mixer` 的价值不是“直接返回排好序的网内 Feed”，而是：

- 快速给出一批网内轻量候选
- 提供 reply / retweet / conversation 关系
- 用新鲜度做初步排序

但它不负责：

- 文本补全
- 个性化排序
- 用户级去重
- 安全过滤

所以 `home-mixer` 必须继续跑后续整条 pipeline。

## 9. 外部依赖现状

从当前仓库真实状态看：

| 依赖 | 当前状态 |
| --- | --- |
| InNetworkPostsClient（网内 / 兜底） | `MrpyqInNetworkPostsClient`；`MRPYQ_RECOMMENDATION_DATA_ADDR` 缺失时真实业务装配失败 |
| TESClient | `MrpyqTESClient`，调用 mrpyq `BatchGetRecommendationContents`；缺失真实合同时候选无法完成水合 |
| VisibilityFilteringClient | `MrpyqFirstStageEligibilityClient`；未验证候选按 `HOME_MIXER_VF_FAILURE_POLICY`，默认 `fail_closed` 删除 |
| StratoClient | `MrpyqStratoClient`，调用由 rec-bff 承载的 `ViewerRelationService`；失败时准入过滤器按 fail-closed 处理 |
| PhoenixRetrievalClient | 当前仍是旧 Phoenix client contract；xrex serving contract 尚未完成适配，不能直接把地址指向 xrex |
| PhoenixPredictionClient | 当前仍是旧 Phoenix client contract；缺失或校验失败时整批走 `RuleFallbackScorer` |
| UserActionSequenceOps | `RedisUserActionSequenceStore` 读取 `uas-worker` 投影到 Redis 的行为序列；没有投影数据时序列为空 |
| GizmoduckClient | 作者资料 adapter；当前可用性以部署注入和验收结果为准 |
| ServedPersistence | 生产环境写共享状态存储；持久化能力必须通过部署合同验收 |

这意味着：

- 框架和装配代码存在，但完整生产链路仍需外部合同、真实埋点、作者资料、viewer 关系和曝光持久化验收。

## 10. 当前默认运行行为

默认 `degraded` 配上 mrpyq 地址后，召回和排序可以先走起来，但 viewer 关系 RPC 未上线时准入过滤器会把结果清掉：

1. 没有 `user_action_sequence`，`PhoenixScorer` 整批标 `phoenix_missing_sequence`，所有请求由 `RuleFallbackScorer` 排序
2. 请求未显式限定网内，因此真实 fallback 池可启用；`PhoenixSource` 仍会因缺少行为序列或 serving contract 未适配而不可用
3. mrpyq 收件箱以皮 `member_id` 作为 `account_id` 查询，皮维度对齐落地前候选供给可能为空；`creator_member_id` 为空的帖子被 `CoreDataHydrationFilter` 清掉
4. `ViewerRelationService` 未实现或失败时，`viewer_relations_hydrated` 为 false，`AuthorSocialgraphFilter` / `ViewerMutedKeywordFilter` 整批丢弃

```mermaid
flowchart TD
    A["UAS 为空"] --> B["PhoenixSource 不可用<br/>PhoenixScorer 整批 fallback"]
    C["in_network_only=false"] --> D["网内 + 网外<br/>Fallback 源可启用"]
    E["mrpyq 皮维度未对齐 / creator_member_id 为空"] --> F["网内候选为空或被 CoreDataHydrationFilter 清掉"]
    H["ViewerRelationService 未上线"] --> I["准入过滤器 fail-closed"]
    B --> G["结果 = mrpyq 网内 / 兜底候选 + 规则排序，或为空"]
    D --> G
    F --> G
    I --> G
```

仓库不再提供绕过这些依赖的 Demo 模式；缺失依赖时应记录明确的降级原因并按生产验收清单处理。

## 11. 当前最值得关注的风险

### 11.1 结果为空、过少或“只有规则排序”

主要由「不看」仍是账号级、行为事件流未接入、作者资料未接（Gizmoduck Disabled）叠加导致；viewer 关系 RPC 本身已由 rec-bff 承载。

### 11.2 同 stage hydrator 依赖问题

因为同一 stage 的 hydrator 并行执行，彼此看不到这一轮刚写入的新字段。

### 11.3 post-selection 删除后不回补

- selector 先取 Top 50
- post-selection 再删
- 最终截断到 35（`RESULT_SIZE`）
- 不会回补第 51 名之后的候选

### 11.4 观测只到 RPC 入口一层

管理端口有 `/healthz`、`/readyz`、`/metrics`，指标覆盖每个 RPC 的状态码计数、耗时直方图和在途数；流水线内部各阶段仍只有 request_id + stage 日志，没有阶段级指标。

## 12. 配置和参数怎么影响行为

配置来源主要有三类：

- CLI 参数：端口等
- 环境变量：如 `HOME_MIXER_MODE`、`MRPYQ_RECOMMENDATION_DATA_ADDR`、`PHOENIX_*_GRPC_ADDR`、`HOME_MIXER_VF_FAILURE_POLICY`
- `params/` 常量：召回上限、权重、Age、TopK、ResultSize、mrpyq 超时与召回预算

最重要的参数组是：

- `THUNDER_MAX_RESULTS`
- `PHOENIX_MAX_RESULTS`
- `MAX_POST_AGE`
- `TOP_K_CANDIDATES_TO_SELECT`
- `RESULT_SIZE`
- 各类行为权重
- `OON_WEIGHT_FACTOR`
- `AUTHOR_DIVERSITY_*`

## 13. 怎么排障

当前最实用的排障路径：

1. 看请求入口日志
2. 看最终返回条数
3. 看 `stage=Source` 日志
4. 看 `stage=Filter kept/removed`
5. 看 post-selection 是否缩水

如果结果为空，优先怀疑：

- `PhoenixSource` 因缺少 `user_action_sequence` 不可用，或请求显式设置 `in_network_only=true` 后 `PhoenixSource` / `FallbackSource` 被关闭
- `ThunderSource` 的 mrpyq 收件箱 `source_ready=false`、皮维度未对齐或召回预算耗尽（日志 `mrpyq adapter recall ...`）
- `CoreDataHydrationFilter` 因 `creator_member_id` 为空，或无正文且未确认有媒体而清空候选
- `AuthorSocialgraphFilter` / `ViewerMutedKeywordFilter` 因 viewer 关系未水合而整批丢弃
- `VFFilter` 在默认 `fail_closed` 下删掉了所有 `Unavailable` 候选（日志 `visibility unavailable for N candidates`）

## 14. 推荐阅读方式

### 14.1 想快速理解系统

读这篇主文档即可。

### 14.2 想查实现细节

去附录索引 [appendices.md](./appendices.md)。

### 14.3 想看逐字段或逐组件说明

- 字段和组件细节以当前 Rust 源码、[请求生命周期](./02-request-lifecycle.md)及[外部依赖合同](./05-external-deps-and-contracts.md)为准。

### 14.4 想看完整请求样例

- [10-end-to-end-example.md](./10-end-to-end-example.md)

## 15. 一句话结论

`home-mixer` 当前已经是一套结构完整的首页编排系统骨架：链路、阶段、策略位点都很清楚，代码已接入 rec-bff / mrpyq 的内容、网内 / 兜底召回与账号级 viewer 关系；真正限制它可用性的，不是主流程缺失，而是行为序列事件流未接、作者资料、持久化曝光这几个适配器仍是 stub，以及「不看 / 不让看」的皮维度存储尚未在 mrpyq 落地。Gizmoduck 仅负责作者资料，网络范围由请求显式控制。
