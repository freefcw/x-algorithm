# PhoenixCandidatePipeline 当前实现剖析

本篇聚焦 `home-mixer/` 中对 `candidate-pipeline` 的实际使用，而不是框架抽象。

## 1. 请求入口

当前请求入口在 `home-mixer/server.rs`：

1. gRPC 接收 `pb::ScoredPostsQuery`
2. 校验 `viewer_id != 0`
3. 转换为内部 `ScoredPostsQuery`
4. 调用 `PhoenixCandidatePipeline::execute(query)`
5. 把 `selected_candidates` 映射回 proto `ScoredPost`

也就是说，`candidate-pipeline` 是 `home-mixer` 服务内部的一条同步请求链路。

## 2. 查询对象 `ScoredPostsQuery`

当前查询对象承载三类信息：

### 2.1 请求原始字段

- `user_id`
- `client_app_id`
- `country_code`
- `language_code`
- `seen_ids`
- `served_ids`
- `in_network_only`
- `is_bottom_request`
- `bloom_filter_entries`

### 2.2 QueryHydrator 补全字段

- `user_action_sequence`
- `user_features`

### 2.3 追踪字段

- `request_id`

`request_id` 由 `generate_request_id()` 与 `user_id` 拼接得到，满足框架的 `HasRequestId` 约束。

## 3. 候选对象 `PostCandidate`

`PostCandidate` 是一份逐阶段叠加的聚合结构，字段大致可分为：

### 3.1 标识与关系

- `tweet_id`
- `author_id`
- `in_reply_to_tweet_id`
- `retweeted_tweet_id`
- `retweeted_user_id`
- `ancestors`

### 3.2 模型分数与排序结果

- `phoenix_scores`
- `prediction_request_id`
- `last_scored_at_ms`
- `weighted_score`
- `score`

### 3.3 展示与派生特征

- `served_type`
- `in_network`
- `video_duration_ms`
- `author_followers_count`
- `author_screen_name`
- `retweeted_screen_name`

### 3.4 安全与策略字段

- `visibility_reason`
- `subscription_author_id`

这说明候选对象本质上既是“候选事实对象”，也是整条流水线的“共享中间状态容器”。

## 4. 当前装配顺序

`PhoenixCandidatePipeline::build_with_clients()` 把所有组件按固定顺序装配如下。

### 4.1 Query Hydrators

1. `UserActionSeqQueryHydrator`
2. `UserFeaturesQueryHydrator`
3. `UserTopicsQueryHydrator`（条件启用：显式传入 `topic_clients` 或在 `HOME_MIXER_DEMO=1` 下自动启用）

### 4.2 Sources

1. `CachedPostsSource`
2. `PhoenixTopicsSource`（条件启用：配置 `topic_clients` 时触发）
3. `PhoenixSource`
4. `PhoenixMoeSource`（条件启用：设置 `PHOENIX_MOE_GRPC_ADDR` 时触发）
5. `ThunderSource`

### 4.3 Hydrators

1. `InNetworkCandidateHydrator`
2. `CoreDataCandidateHydrator`
3. `VideoDurationCandidateHydrator`
4. `SubscriptionHydrator`
5. `GizmoduckCandidateHydrator`

### 4.4 Filters

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
12. `TopicIdsFilter`
13. `VideoFilter`

### 4.5 Scorers

1. `PhoenixScorer`
2. `WeightedScorer`
3. `AuthorDiversityScorer`
4. `OONScorer`

### 4.6 Selector

- `TopKScoreSelector`

### 4.7 Post-selection Hydrators

1. `VFCandidateHydrator`

### 4.8 Post-selection Filters

1. `VFFilter`
2. `AncillaryVFFilter`
3. `DedupConversationFilter`

### 4.9 Side Effects

1. `CacheRequestInfoSideEffect`

## 5. 每个阶段在业务上的作用

### Query Hydrators

- UAS hydrator：为 Phoenix 召回和精排准备用户行为序列
- User features hydrator：拿关注、屏蔽、静音、订阅、关键词等用户侧特征
- User topics hydrator：拉取用户感兴趣的话题列表，支持个性化话题召回

### Sources

- `CachedPostsSource`：优先从本地/内存缓存中快速补充候选
- `PhoenixTopicsSource`：根据用户话题画像进行定向话题召回
- `PhoenixSource`：双塔模型海选全网网外候选
- `PhoenixMoeSource`：基于 MoE 架构进行多专家多目标网外候选召回
- `ThunderSource`：召回关注网络内的实时候选

### Hydrators

- `InNetworkCandidateHydrator`：根据作者是否被关注判断网内/网外
- `CoreDataCandidateHydrator`：补 tweet 文本、转推/回复关系
- `VideoDurationCandidateHydrator`：补视频时长
- `SubscriptionHydrator`：补订阅作者信息
- `GizmoduckCandidateHydrator`：补作者粉丝数和 screen_name

### Filters

- 前几层 filter 负责去重、内容合法性、年龄限制、基础策略过滤
- `PreviouslySeenPostsFilter` / `PreviouslySeenPostsBackupFilter`：结合 Bloom Filter 与备份索引过滤已看内容
- `TopicIdsFilter`：针对话题候选的合法 Topic ID 进行校验过滤
- `VideoFilter`：对非法或损坏视频格式的候选进行过滤
- `AuthorSocialgraphFilter` / `MutedKeywordFilter`：针对用户屏蔽、静音作者及关键词进行强过滤

### Scorers

- `PhoenixScorer`：拉模型原始行为概率
- `WeightedScorer`：按业务权重聚合成一个可排序分数
- `AuthorDiversityScorer`：同作者多条内容做衰减
- `OONScorer`：对网外内容降权

### Post-selection 阶段

- `VFCandidateHydrator`：调用可见性审核服务
- `VFFilter`：把需要 Drop 的内容剔除
- `AncillaryVFFilter`：辅助可见性安全审核与标记
- `DedupConversationFilter`：对同一会话树只保留一条高分候选

## 6. 关键参数

当前装配里最重要的参数在 `home-mixer/params.rs`：

| 参数 | 当前值 | 含义 |
| --- | --- | --- |
| `THUNDER_MAX_RESULTS` | 500 | 网内召回上限 |
| `PHOENIX_MAX_RESULTS` | 300 | 网外召回上限 |
| `MAX_POST_AGE` | 48 小时 | `AgeFilter` 过滤阈值 |
| `MIN_VIDEO_DURATION_MS` | 2000 ms | VQV 权重是否生效 |
| `TOP_K_CANDIDATES_TO_SELECT` | 100 | selector 保留的数量 |
| `RESULT_SIZE` | 50 | 最终返回数量上限 |
| `OON_WEIGHT_FACTOR` | 0.5 | 网外内容降权比例 |
| `AUTHOR_DIVERSITY_DECAY` | 0.5 | 同作者重复内容衰减系数 |
| `AUTHOR_DIVERSITY_FLOOR` | 0.1 | 衰减下限 |

## 7. 外部依赖图

当前 `PhoenixCandidatePipeline` 依赖的外部客户端如下（成熟度权威表见 [home-mixer 外部依赖文档](../home-mixer/05-external-deps-and-contracts.md)）：

| 依赖 | 当前用途 | 当前实现状态 |
| --- | --- | --- |
| `ThunderClient` | 调 Thunder 取网内候选 | 简化版真实客户端 |
| `PhoenixPredictionClient` | 精排模型预测 | 设 `PHOENIX_PREDICT_GRPC_ADDR` 后真连 gRPC，否则 stub |
| `PhoenixRetrievalClient` | 双塔召回 | 设 `PHOENIX_RETRIEVAL_GRPC_ADDR` 后真连 gRPC，否则 stub |
| `UserActionSequenceFetcher` | 取用户行为序列 | stub（`HOME_MIXER_DEMO=1` 时装配层注入 Demo 实现） |
| `StratoClient` | 取用户特征、缓存请求信息 | stub（演示模式注入 `DemoStratoClient`） |
| `TESClient` | 取帖子文本/媒体/订阅信息 | stub（演示模式注入 `DemoTESClient`） |
| `GizmoduckClient` | 取作者资料 | stub |
| `VisibilityFilteringClient` | 可见性审核 | stub |

## 8. 当前默认实现推导出来的运行结果

按不设任何环境变量的默认 stub 组合，流水线的实际行为不是“效果差一些”，而是“基本无法产出完整结果”（演示组合下的可跑通路径见 [getting-started 第四步](../getting-started/05-第四步-跑通完整推荐链路.md)）。

### 8.1 UAS 补全会失败

`UserActionSequenceFetcher` 返回空行为序列，而 `UserActionSeqQueryHydrator` 会把空序列视为错误：

- `hydrate_query()` 记录 error
- `query.user_action_sequence` 保持 `None`

### 8.2 PhoenixSource 基本不会产出候选

`PhoenixSource` 要求 `query.user_action_sequence` 存在。因为前一步通常失败，所以：

- `PhoenixSource` 会返回 `missing user_action_sequence`
- 整个网外召回默认失效

### 8.3 Strato 默认返回空用户特征

这会带来：

- `followed_user_ids` 为空
- `blocked_user_ids` / `muted_user_ids` 为空
- `subscribed_user_ids` 为空
- `muted_keywords` 为空

因此多数个性化过滤都退化成“无效或最弱状态”。

### 8.4 TES 默认返回空帖子元数据，导致候选会被清空

这是最关键的一点：

- `CoreDataCandidateHydrator` 无法补出 `tweet_text`
- `CoreDataHydrationFilter` 要求 `tweet_text` 非空

由于当前 `Source` 也不会直接填 `tweet_text`，所以只要候选进到这里，几乎都会被 `CoreDataHydrationFilter` 全部过滤掉。

结论：

- 即便 Thunder 真返回了候选
- 在默认 stub 状态下，后续也大概率被 core-data 过滤清空

### 8.5 Phoenix 打分默认退化

即便忽略前面的召回/补全问题，`PhoenixPredictionClient` 现在也是 stub：

- `PhoenixScorer` 会拿不到有效预测分布
- `WeightedScorer` 多数动作分数都按 `0.0` 处理

排序将退化成规则分数和默认值排序，而不是模型驱动排序。

## 9. 当前系统最准确的定位

如果严格按代码现状描述，当前 `PhoenixCandidatePipeline` 更适合被视为：

- 一条结构完整的推荐编排骨架
- 一份可替换私有依赖的迁移样板
- 一份便于后续逐步补齐外部服务的工程底盘

而不是“已经能直接给出高质量首页 Feed 的完整实现”。
