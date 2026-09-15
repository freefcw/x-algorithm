# 04. 召回、过滤与排序策略

本篇只讲策略链，不展开协议和客户端细节。

## 1. 召回：为什么要双路

`home-mixer` 默认是多路并行，演示还会加上话题源：

- `ThunderSource`：网内内容（有 cached posts 时关闭）。名字沿用上游，实际依赖 `InNetworkPostsClient`：非 demo 是 mrpyq NETWORK 关注收件箱，demo 是整数 Thunder
- `PhoenixSource`：网外内容（网内限定、严格话题、或有 cached posts 时关闭）
- `FallbackSource`：兜底池（非 demo 是 mrpyq FALLBACK 池；网内限定或有 cached posts 时关闭）
- `PhoenixTopicsSource`：演示默认装配；非 demo 要显式注入 adapter
- `CachedPostsSource`：只接受 QueryBuilder 已批准的 demo fixture

```mermaid
flowchart LR
    Q["hydrated query"] --> T["ThunderSource<br/>mrpyq NETWORK 收件箱（demo：Thunder）"]
    Q --> P["PhoenixSource<br/>基于 retrieval_sequence"]
    Q --> FB["FallbackSource<br/>mrpyq FALLBACK 池"]
    T --> M["合并候选"]
    P --> M
    FB --> M
```

`QueryBuilder` 默认构造全网 For You 请求，网络范围完全由请求的 `in_network_only` 决定。只有显式设置为 `true` 时，`PhoenixSource` 与 `FallbackSource` 才不会启用，`ThunderSource` 则按网内专用语义继续运行。Gizmoduck 只在后续水合阶段补作者资料。

### 1.1 `ThunderSource`

作用：

- 通过 `InNetworkPostsClient` 取关注作者最近帖子
- 默认启用；请求已带 cached posts 时关闭

输入：

- `query.user_id`（非 demo 作为 `account_id` 发给 mrpyq；皮维度对齐见 `docs/implementation/mrpyq-member-dimension-requirements.md`）
- demo 的 Thunder 请求还会带 `query.user_features.followed_user_ids`

输出特点：

- `served_type`：普通请求是 `ForYouInNetwork`；`in_network_only` 时是 `RankedFollowing`
- 在来源处直接标 `in_network = Some(true)`
- 非 demo 的 mrpyq 候选只带 `tweet_id`，作者、正文、`created_at_ms` 都靠 TES 补全；适配器按 ObjectId 时间戳做粗筛，遇到第一条超过 `MAX_POST_AGE` 的候选即停止翻页
- demo 的 Thunder 候选另带 `ancestors` 与 `retweeted_tweet_id` / `retweeted_user_id`（来自 `source_post_id` / `source_user_id`）

### 1.2 `PhoenixSource`

作用：

- 从 Phoenix Retrieval 取网外候选

启用条件：

- `!query.in_network_only`
- 没有 cached posts
- 不是 strict / cold-start 话题请求

硬依赖：

- retrieval sequence

输出特点：

- `served_type = ForYouPhoenixRetrieval`
- 当前只写入轻量关系字段，更多内容依赖后续 hydrator

### 1.3 `FallbackSource`

作用：

- 从业务兜底池取网外候选（非 demo：mrpyq `ListRecommendationCandidates(source=FALLBACK)`，最多 200 条）

启用条件：

- `!query.in_network_only`
- 没有 cached posts

输出特点：

- 在来源处直接标 `in_network = Some(false)`
- `served_type` 复用 `ForYouPhoenixRetrieval`，响应里无法与真实 Phoenix 召回区分

## 2. 候选补全：召回后的“补课”阶段

召回出来的候选信息非常少，因此需要集中补数。

| Hydrator | 作用 | 下游影响 |
| --- | --- | --- |
| `InNetworkCandidateHydrator` | 对来源未标定的候选判断作者是否在关注网络内；Thunder / Fallback 已标定的原样保留 | `RankingScorer` 内部 OON 阶段、`RuleFallbackScorer`、`VFCandidateHydrator` |
| `CoreDataCandidateHydrator` | 补作者（来源留空时）、文本、`created_at_ms`、一级 `recommendation_eligible`、互动计数、转推 / 回复关系 | 多个 filter 和 scorer 依赖 |
| `VideoDurationCandidateHydrator` | 补视频时长 | `VideoFilter`（看 `video_duration_ms`）、VQV 权重 |
| `HasMediaHydrator` | 补是否有媒体 | 展示信号，当前无过滤消费 |
| `FilteredTopicsHydrator` / `LanguageCodeHydrator` | 补话题和语言 | 话题过滤 |
| `GizmoduckCandidateHydrator` | 补作者 screen_name、粉丝数 | 默认在 post-selection；冷启动打开时预选再跑一次；非 demo 为 Disabled，全部为空 |

引用与订阅相关的 `QuoteHydrator` / `SubscriptionHydrator` 已按 U5 删除，对应字段保留为空。

## 3. 过滤链：先把明显不该排的内容拿掉

过滤是串行执行的，所以顺序有业务含义。

```mermaid
flowchart TD
    A["合并并补全后的候选"] --> F1["DropDuplicates"]
    F1 --> F2["CoreDataHydration"]
    F2 --> F2B["FirstStageEligible<br/>只丢 Some(false)"]
    F2B --> F3["Age<br/>created_at_ms，缺失回退 ObjectId 时间戳"]
    F3 --> F4["SelfTweet"]
    F4 --> F7["PreviouslySeenPosts"]
    F7 --> F7B["SeenPostsBackup<br/>seen_ids 缺失时用 impressed_post_ids"]
    F7B --> F8["PreviouslyServedPosts<br/>仅 bottom request"]
    F8 --> F9["ViewerMutedKeyword"]
    F9 --> F10["AuthorSocialgraph"]
    F10 --> F11["Video / TopicIds / NewUserTopic"]
    F11 --> B["进入打分"]
```

### 3.1 过滤器按问题分类

| 问题 | 过滤器 |
| --- | --- |
| 候选重复 | `DropDuplicatesFilter` |
| 内容不完整 | `CoreDataHydrationFilter`（作者为 NIL 或正文为空） |
| 业务一级不可推荐 | `FirstStageEligibleFilter`（删除 / 未公开 / 审核未过，来自 mrpyq `recommendation_eligible=false`） |
| 内容过旧 | `AgeFilter` |
| 不该给自己看 | `SelfTweetFilter` |
| 已经看过 / 已下发过 | `PreviouslySeenPostsFilter`、`PreviouslySeenPostsBackupFilter`、`PreviouslyServedPostsFilter` |
| 用户明确不想看 | `ViewerMutedKeywordFilter`、`AuthorSocialgraphFilter` |
| 请求不要视频 / 话题约束 | `VideoFilter`、`TopicIdsFilter`、`NewUserTopicIdsFilter` |

### 3.2 为什么 `PreviouslyServedPostsFilter` 只在 bottom request 启用

它的 `enable()` 条件是 `query.is_bottom_request`，意图是：

- 首屏更强调质量，不一定强依赖 `served_ids`
- 下滑续页更强调“不要重复给刚才给过的内容”

这体现了请求类型差异化策略。

## 4. 打分链：从行为概率到最终排序分

打分也是串行的，每个 scorer 都依赖前一个阶段产物。

```mermaid
flowchart LR
    A["候选 + scoring_sequence"] --> P["PhoenixScorer<br/>预测多种互动概率"]
    P --> R["RankingScorer<br/>内部组合加权 / 多样性 / OON"]
    R --> RF["RuleFallbackScorer<br/>Phoenix 头缺失时整批规则分"]
    RF --> S["TopKScoreSelector"]
```

上游 47c1bcd 已把 Weighted、Author Diversity、OON 三段逻辑合并进唯一的 `RankingScorer`，仓库中不再存在 `WeightedScorer`、`AuthorDiversityScorer`、`OONScorer` 这三个独立类型；下面 4.2–4.4 介绍的是 `RankingScorer` 内部按序执行的三个阶段，4.5 是本地新增的整批规则回退。

### 4.1 `PhoenixScorer`

作用：

- 调 Phoenix Prediction 服务
- 取每个候选上的行为概率

主要输出：

- `phoenix_scores`
- `prediction_request_id`
- `last_scored_at_ms`

一个重要细节：

- 对于转推，它优先用原始帖子的 `tweet_id` / `author_id` 作为模型输入和结果 lookup key

### 4.2 `RankingScorer`

它把多种行为概率合成一个 `weighted_score`。当前主要权重包括：

| 行为 | 权重 |
| --- | --- |
| 点赞 | `0.5` |
| 回复 | `5.0` |
| 转发 | `0.0`（上游 1.0；产品无转推，U5 置 0） |
| 分享 | `2.0` |
| 引用 | `0.0`（上游 5.0；产品无引用转发，U5 置 0） |
| 关注作者 | `4.0` |
| 不感兴趣 | `-43.2` |
| 拉黑 | `-31.2` |
| 静音 | `-58.8` |
| 举报 | `-234.0` |

简化理解：

```text
weighted_score
  = 正向行为概率 * 正向权重
  + 负向行为概率 * 负向权重
  + 连续停留时间 * dwell_time_weight
```

这里的“行为概率”是 Phoenix 针对当前 viewer 的个性化预测，不是帖子的点赞、
举报等原始互动次数。因此不能用两个权重的比值推导“一次举报抵消多少次点赞”。
负向行为通常具有更低的基准概率，较大的权重绝对值用于让这些低概率预测仍能
影响最终排序。

VQV 还有一个额外条件：

- 只有 `video_duration_ms > MIN_VIDEO_DURATION_MS`，且 viewer `follower_count` 未达 10000（缺粉丝数时不触发该门槛）才应用 `VQV_WEIGHT`

### 4.3 作者多样性（`RankingScorer` 内部阶段）

作用：

- 防止同一作者在结果前列出现过密

实现方式：

1. 先按 `weighted_score` 排一个内部顺序
2. 统计每个作者已经出现了几次
3. 按出现次数乘衰减因子
4. 把结果写回 `score`

当前关键参数：

- `AUTHOR_DIVERSITY_DECAY = 0.5`
- `AUTHOR_DIVERSITY_FLOOR = 0.25`（第 2、3 条大约 ×0.625、×0.4375）

### 4.4 OON 降权（`RankingScorer` 内部阶段）

作用：

- 给网外内容统一降权

规则：

- `in_network == false` 时，`score *= OON_WEIGHT_FACTOR`
- 网内回复/转发默认也乘同一因子（`ENABLE_OON_RESCORE_FOR_IN_NETWORK_REPLIES_RETWEETS = true`）
- 当前普通请求 `OON_WEIGHT_FACTOR = 0.75`；话题请求用 `TOPIC_OON_WEIGHT_FACTOR = 0.5`

### 4.5 `RuleFallbackScorer`（整批规则回退）

装配在 `RankingScorer` 之后。批内所有候选都带可用 Phoenix 头时它什么都不做；只要有任一候选带 `degraded_reason`（`PhoenixScorer` 无序列、超时、失败或契约校验不通过）或缺少可用的 Phoenix 头，它就用一套规则分覆盖整批，并把 `phoenix_scores` / `weighted_score` / `prediction_request_id` 清空、`degraded_reason` 统一写成 `phoenix_unavailable`：

```text
rule_score = 1.5 × max(0, 1 − 帖龄天数 / 7)      # 新鲜度
           + 2.0 × [in_network]                  # 网内加成
           + 0.2 × ln(1 + 点赞 + 2 × 评论) × 0.5^帖龄天数   # 互动
score      = rule_score × 0.5^同作者已出现次数（下限 0.25）  # 作者多样性
```

非 demo 下 UAS 适配器仍是 Disabled，`PhoenixScorer` 拿不到序列，所以当前所有非 demo 请求最终都由这一步排序。

## 5. 选择与后处理

### 5.1 Selector

`TopKScoreSelector` 按 `score` 降序排序，并先保留 Top 50（`TOP_K_CANDIDATES_TO_SELECT`）。

### 5.2 Post-selection

选择后并没有立刻返回，还会继续做：

1. `GizmoduckCandidateHydrator`：补作者资料（非 demo 为 Disabled，全部为空）
2. `VFCandidateHydrator`：批量拿可见性结果（非 demo 只有 mrpyq 一级 `recommendation_eligible`，没有 viewer 级判定）
3. `VFFilter`：删除应被 Drop 的内容；`Unchecked / Unavailable`（含成功响应缺帖）按 `HOME_MIXER_VF_FAILURE_POLICY`，默认 `fail_closed` 一律删除
4. `DedupConversationFilter`：同一会话树只留一条最高分
5. 最终再截断到 `RESULT_SIZE = 35`

`AncillaryVFFilter` 已按 U5 删除（产品无引用 / 转推附属内容）。

```mermaid
flowchart TD
    A["排序后的 Top 50"] --> G["GizmoduckCandidateHydrator"]
    G --> B["VFCandidateHydrator"]
    B --> C["VFFilter<br/>默认 fail_closed"]
    C --> D["DedupConversationFilter"]
    D --> E["truncate 到 35"]
    E --> F["返回响应"]
```

## 6. 为什么要把可见性过滤放在后面

当前设计明显偏向这个取舍：

- 先让排序模型面对更大的候选池
- 再对高分结果做更贵的安全审核

优点：

- 节省 VF 调用成本
- 只审核真正可能返回给用户的候选

代价：

- 如果 VF 删掉很多结果，不会回补第 51 名以后的候选

这也是后面风险文档里会单独指出的问题。
