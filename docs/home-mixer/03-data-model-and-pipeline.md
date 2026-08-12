# 03. 数据模型与 Pipeline 装配

理解 `home-mixer`，核心不是背诵组件名字，而是理解两类对象：

- 请求对象 `ScoredPostsQuery`
- 候选对象 `PostCandidate`

以及它们如何被 pipeline 逐阶段改写。

## 1. `ScoredPostsQuery`：请求上下文容器

内部请求结构定义在 `home-mixer/models/query.rs`。

### 1.1 字段分层

| 类别 | 字段 | 来源 |
| --- | --- | --- |
| 原始请求字段 | `user_id` `client_app_id` `country_code` `language_code` | 来自 gRPC 请求 |
| 去重与分页字段 | `seen_ids` `served_ids` `bloom_filter_entries` `is_bottom_request` | 来自 gRPC 请求 |
| 召回控制字段 | `in_network_only` | 来自 gRPC 请求 |
| QueryHydrator 补全字段 | `scoring_sequence` `retrieval_sequence` | `ScoringSequenceQueryHydrator` / `RetrievalSequenceQueryHydrator` |
| QueryHydrator 补全字段 | `user_features` | 四个上游 user-id owner + 本地 safety owner |
| 追踪字段 | `request_id` `prediction_id` `request_time_ms` | `QueryBuilder` |

### 1.2 查询对象的演化

```mermaid
flowchart LR
    A["proto ScoredPostsQuery"] --> B["内部 ScoredPostsQuery<br/>原始字段 + request_id"]
    B --> C["ScoringSequenceQueryHydrator<br/>补 scoring_sequence"]
    B --> D["RetrievalSequenceQueryHydrator<br/>补 retrieval_sequence"]
    B --> F["User feature owners<br/>分字段补 user_features"]
    C --> E["hydrated query"]
    D --> E
    F --> E
```

### 1.3 `request_id` 的作用

`request_id`、`prediction_id` 和 `request_time_ms` 由 `QueryBuilder` 一次生成，贯穿阶段日志和模型打分结果。因为 pipeline 大量采用“失败但不中断”的降级策略，没有稳定的请求身份就很难定位是哪一阶段退化。

## 2. `PostCandidate`：流水线共享状态对象

`PostCandidate` 定义在 `home-mixer/models/candidate.rs`。它不是一个“固定结构体”，而是一份被逐阶段补齐的共享状态。

### 2.1 字段按作用划分

| 类别 | 字段 | 主要由谁写入 |
| --- | --- | --- |
| 标识 | `tweet_id` `author_id` | Source |
| 关系 | `in_reply_to_tweet_id` `retweeted_tweet_id` `retweeted_user_id` `ancestors` | Source / CoreDataHydrator |
| 文本与内容 | `tweet_text` `video_duration_ms` | TES 相关 Hydrator |
| 用户展示 | `author_screen_name` `retweeted_screen_name` | GizmoduckHydrator |
| 用户侧派生 | `in_network` `subscription_author_id` | InNetwork / Subscription Hydrator |
| 排序相关 | `phoenix_scores` `weighted_score` `score` | 各类 Scorer |
| 追踪 | `prediction_request_id` `last_scored_at_ms` | PhoenixScorer |
| 安全 | `visibility_decision` | VFCandidateHydrator |
| 来源 | `served_type` | Source |

### 2.2 候选对象在各阶段的变化

```mermaid
flowchart TD
    S["Source 输出<br/>tweet_id / author_id / served_type / ancestors"] --> H1["InNetworkHydrator<br/>补 in_network"]
    S --> H2["CoreDataHydrator<br/>补 text / retweet / reply 关系"]
    S --> H3["VideoDurationHydrator<br/>补 video_duration_ms"]
    S --> H4["SubscriptionHydrator<br/>补 subscription_author_id"]
    S --> H5["GizmoduckHydrator<br/>补 screen_name / followers_count"]

    H1 --> F["Pre-selection Filters"]
    H2 --> F
    H3 --> F
    H4 --> F
    H5 --> F

    F --> P["PhoenixScorer<br/>补 phoenix_scores"]
    P --> R["RankingScorer<br/>组合 weighted / diversity / OON"]
    R --> SEL["TopKSelector"]
    SEL --> VFH["VFCandidateHydrator<br/>补 visibility_decision"]
    VFH --> PSEL["Post-selection Filters"]
```

## 3. Pipeline 的真实装配顺序

`home-mixer` 当前只装了一条主 pipeline：`PhoenixCandidatePipeline`。

### 3.1 Query Hydrators

1. `ScoringSequenceQueryHydrator`
2. `RetrievalSequenceQueryHydrator`
3. `BlockedUserIdsQueryHydrator`
4. `MutedUserIdsQueryHydrator`
5. `FollowedUserIdsQueryHydrator`
6. `SubscribedUserIdsQueryHydrator`
7. `UserSafetyFeaturesQueryHydrator`（本地 additive owner）
8. `UserTopicsQueryHydrator`（显式注入 Topic adapter 时）

### 3.2 Sources

1. `ThunderSource`
2. `PhoenixSource`
3. `PhoenixTopicsSource`（可选）
4. `PhoenixMoeSource`（可选）
5. `CachedPostsSource`

### 3.3 Pre-selection Hydrators

1. `InNetworkCandidateHydrator`
2. `CoreDataCandidateHydrator`
3. `QuoteHydrator`
4. `VideoDurationCandidateHydrator`
5. `HasMediaHydrator`
6. `SubscriptionHydrator`
7. `FilteredTopicsHydrator`（topic request / excluded topics）
8. `LanguageCodeHydrator`

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

### 3.5 Scorers

1. `PhoenixScorer`
2. `RankingScorer`（内部保留 Weighted / AuthorDiversity / OON 行为）

### 3.6 Selector

- `TopKScoreSelector`

### 3.7 Post-selection

- Hydrators: `GizmoduckCandidateHydrator`, `VFCandidateHydrator`
- Filters: `VFFilter`, `AncillaryVFFilter`, `DedupConversationFilter`

### 3.8 Side Effect

- `PhoenixRequestCacheSideEffect`（默认关闭）

## 4. 一个最重要的语义：同 stage 的 hydrator 彼此看不到新字段

同一 stage 的 hydrator 基于同一份旧候选快照并行执行，字段依赖必须跨 stage。当前装配已将 `GizmoduckCandidateHydrator` 从 pre-selection 移到 post-selection，因此它能看到 `CoreDataCandidateHydrator` 已写回的 `retweeted_user_id`，并且只批量查询 Selector 保留的候选。

```mermaid
graph LR
    C["Pre-selection 候选"] --> H1["CoreDataHydrator"]
    H1 --> F["Filter / Score / Selector"]
    F --> H2["Post-selection GizmoduckHydrator"]
    H2 --> M["响应 screen_names"]
```

新增 Hydrator 时仍不能依赖同 stage 的执行顺序；需要使用后续 stage 或共享 provider 明确表达字段依赖。

## 5. 关键依赖链

| 上游写入 | 下游依赖 |
| --- | --- |
| `scoring_sequence` / `retrieval_sequence` | `PhoenixScorer` / `PhoenixSource` |
| `user_features.followed_user_ids` | `ThunderSource`、`InNetworkCandidateHydrator` |
| `tweet_text` | `CoreDataHydrationFilter`、`MutedKeywordFilter` |
| `retweeted_tweet_id` | `RetweetDeduplicationFilter`、`PhoenixScorer` |
| `video_duration_ms` | `RankingScorer` 内部 VQV 权重 |
| `in_network` | `RankingScorer` 内部 OON 调整、`VFCandidateHydrator` |
| `visibility_decision` | `VFFilter`（未知时网外拒绝、网内保留） |
| `score` | `TopKScoreSelector`、`DedupConversationFilter` |

## 6. 这个数据模型的优点和代价

### 优点

- 所有阶段共享一份统一候选对象，扩展方便
- 响应映射简单
- 适合快速新增字段和策略

### 代价

- 字段是否可用依赖阶段顺序和并发语义
- 很容易出现“结构上有字段，运行时却还没补出来”的误判
- 调试时必须同时看字段定义和装配顺序
