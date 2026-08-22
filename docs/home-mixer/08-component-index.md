# 08. 组件索引

本篇是“查手册式”索引，按组件列出：

- 文件位置
- enable 条件
- 读取字段
- 写回字段
- 外部依赖
- 下游依赖

> **索引范围**：§1-§10 索引内层 `PhoenixCandidatePipeline` 装配的组件；ForYou 外层 pipeline（`for_you_candidate_pipeline.rs`）的装配见 §11；存在于代码中但未装配进任何 pipeline 的骨架组件见 §12。

## 1. Query Hydrators

| 组件 | 文件 | enable | 读取 | 写回 | 外部依赖 | 下游依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| `ScoringSequenceQueryHydrator` | `query_hydrators/scoring_sequence_query_hydrator.rs` | 默认启用 | `user_id` `request_id` | `user_action_sequence` `scoring_sequence` | 共享 `UserActionSequenceOps` provider | `PhoenixScorer` |
| `RetrievalSequenceQueryHydrator` | `query_hydrators/retrieval_sequence_query_hydrator.rs` | 默认启用 | `user_id` `request_id` | `retrieval_sequence` | 同一共享 provider | `PhoenixSource` / MoE |
| `BlockedUserIdsQueryHydrator` | `query_hydrators/blocked_user_ids_query_hydrator.rs` | 默认启用 | `user_id` | `blocked_user_ids` | 共享 `StratoClient` provider | `AuthorSocialgraphFilter` |
| `MutedUserIdsQueryHydrator` | `query_hydrators/muted_user_ids_query_hydrator.rs` | 默认启用 | `user_id` | `muted_user_ids` | 同一共享 provider | `AuthorSocialgraphFilter` |
| `FollowedUserIdsQueryHydrator` | `query_hydrators/followed_user_ids_query_hydrator.rs` | 默认启用 | `user_id` | `followed_user_ids` | 同一共享 provider | `ThunderSource` / InNetwork |
| `SubscribedUserIdsQueryHydrator` | `query_hydrators/subscribed_user_ids_query_hydrator.rs` | 默认启用 | `user_id` | `subscribed_user_ids` | 同一共享 provider | subscription filter |
| `UserSafetyFeaturesQueryHydrator` | `query_hydrators/user_safety_features_query_hydrator.rs` | 默认启用 (`U2`) | `user_id` | `muted_keywords` `blocked_by_user_ids` | 同一共享 provider | safety filters |
| `UserTopicsQueryHydrator` | `query_hydrators/user_topics_query_hydrator.rs` | 显式注入 `topic_clients` 或 Demo 时启用 | `user_id` | `supplemental_topic_ids` | `UserTopicReader` | `PhoenixTopicsSource` |

## 2. Sources

| 组件 | 文件 | enable | 读取 | 产出字段 | 外部依赖 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| `ThunderSource` | `sources/thunder_source.rs` | 默认启用；`has_cached_posts` 时跳过 | `user_id`、`followed_user_ids` | `tweet_id` `author_id` `in_reply_to_tweet_id` `retweeted_tweet_id` `retweeted_user_id` `ancestors` `served_type` | `ThunderClient` | 上游顺序第一；转推原帖来自 LightPost `source_*`；`in_network_only` 时 `served_type=RankedFollowing` |
| `PhoenixSource` | `sources/phoenix_source.rs` | 非网内限定、非 strict/cold-start topic、无 cached posts | `user_id`、`retrieval_sequence`（缺时回退 `user_action_sequence`） | `tweet_id` `author_id` `in_reply_to_tweet_id` `served_type` | `PhoenixRetrievalClient` | 两个序列都缺失时直接失败 |
| `PhoenixTopicsSource` | `sources/phoenix_topics_source.rs` | 注入 topic adapter 且有 topic recall | selected topics | `tweet_id` `author_id` `served_type` | `TopicRetrievalClient` | 可选话题候选召回 |
| `PhoenixMoeSource` | `sources/phoenix_moe_source.rs` | typed switch + endpoint + 请求允许 | `user_id`、`retrieval_sequence` | `tweet_id` `author_id` `served_type` | `PhoenixRetrievalClient` (MoE) | 默认关闭 |
| `CachedPostsSource` | `sources/cached_posts_source.rs` | QueryBuilder 已接受显式 Demo unsigned fixture | 本地请求缓存 | `tweet_id` `author_id` 等 | 无 | 默认请求拒绝未签名缓存；启用时其他主要来源关闭 |

## 3. Candidate Hydrators

| 组件 | 文件 | enable | 读取 | 写回 | 外部依赖 | 下游依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| `InNetworkCandidateHydrator` | `candidate_hydrators/in_network_candidate_hydrator.rs` | 默认启用 | `query.user_id` `followed_user_ids` `candidate.author_id` | `in_network` | 无 | `RankingScorer`、`VFCandidateHydrator` |
| `CoreDataCandidateHydrator` | `candidate_hydrators/core_data_candidate_hydrator.rs` | 默认启用 | `candidate.tweet_id` | `retweeted_user_id` `retweeted_tweet_id` `in_reply_to_tweet_id` `tweet_text` `favorite_count` `view_count` 等互动计数 | `TESClient.get_tweet_core_datas` via shared provider | `CoreDataHydrationFilter`、`RetweetDeduplicationFilter`、`ViewerMutedKeywordFilter`、`PhoenixScorer`、冷启动探索 |
| `QuoteHydrator` | `candidate_hydrators/quote_hydrator.rs` | 默认启用 | shared core batch | `quoted_tweet_id` `quoted_user_id` `quoted_tweet_text` `quoted_video_duration_ms` | shared TES core/media batches | quote-aware filters / Ranking |
| `HasMediaHydrator` | `candidate_hydrators/has_media_hydrator.rs` | 默认启用 | shared media batch | `has_media` | `TESClient.get_tweet_media_entities` via shared provider | 展示信号，当前无过滤消费 |
| `VideoDurationCandidateHydrator` | `candidate_hydrators/video_duration_candidate_hydrator.rs` | 默认启用 | `candidate.tweet_id` | `video_duration_ms` | shared `TESClient` media batch | `VideoFilter`、`RankingScorer` |
| `SubscriptionHydrator` | `candidate_hydrators/subscription_hydrator.rs` | 默认启用 | `candidate.tweet_id` | `subscription_author_id` | `TESClient.get_subscription_author_ids` | `IneligibleSubscriptionFilter` |
| `GizmoduckCandidateHydrator` | `candidate_hydrators/gizmoduck_hydrator.rs` | post-selection 默认启用；demo Cold Start 显式开启时另在 pre-selection 补粉丝数 | `author_id` `retweeted_user_id` | `author_followers_count` `author_screen_name` `retweeted_screen_name` | `GizmoduckClient`（去重批量读取） | Cold Start 资格；响应映射；能读取 pre-selection CoreData 已补出的 retweet author |
| `FilteredTopicsHydrator` | `candidate_hydrators/filtered_topics_hydrator.rs` | topic recall / excluded topics | shared core batch | `filtered_topic_ids` `unfiltered_topic_ids` | shared `TESClient` core batch | `TopicIdsFilter` / `NewUserTopicIdsFilter` |
| `LanguageCodeHydrator` | `candidate_hydrators/language_code_hydrator.rs` | 默认启用 | shared core batch | `language_code` | shared `TESClient` core batch | language / response consumers |
| `VFCandidateHydrator` | `candidate_hydrators/vf_candidate_hydrator.rs` | post-selection 阶段默认启用 | `get_viewer()`（`user_id` `client_app_id` `country_code` `language_code`）`candidate.in_network` `tweet_id` | `visibility_decision` `drop_ancillary_posts` | `VisibilityFilteringClient`（500 ms） | `VFFilter` / `AncillaryVFFilter` |

## 4. Filters

### 4.1 Pre-selection Filters

| 组件 | 文件 | enable | 读取 | 移除条件 |
| --- | --- | --- | --- | --- |
| `DropDuplicatesFilter` | `filters/drop_duplicates_filter.rs` | 默认启用 | `tweet_id` | 同一 `tweet_id` 重复出现 |
| `CoreDataHydrationFilter` | `filters/core_data_hydration_filter.rs` | 默认启用 | `author_id` `tweet_text` | 作者为空或文本为空 |
| `AgeFilter` | `filters/age_filter.rs` | 默认启用 | `tweet_id` | Snowflake 推导年龄大于 `MAX_POST_AGE` |
| `SelfTweetFilter` | `filters/self_tweet_filter.rs` | 默认启用 | `query.user_id` `author_id` | 作者就是 viewer |
| `RetweetDeduplicationFilter` | `filters/retweet_deduplication_filter.rs` | 默认启用 | `tweet_id` `retweeted_tweet_id` | 原帖/转推去重域冲突 |
| `IneligibleSubscriptionFilter` | `filters/ineligible_subscription_filter.rs` | 默认启用 | `subscription_author_id` `subscribed_user_ids` | 订阅内容作者不在订阅列表 |
| `PreviouslySeenPostsFilter` | `filters/previously_seen_posts_filter.rs` | 默认启用 | `seen_ids` `bloom_filter_entries` `related_post_ids` | 见过任一相关帖子 |
| `PreviouslySeenPostsBackupFilter` | `filters/previously_seen_posts_backup_filter.rs` | `seen_ids` 为空且带了 `impressed_post_ids` | `impressed_post_ids` | 主 seen 列表缺失时，用曝光 ID 做备份去重 |
| `PreviouslyServedPostsFilter` | `filters/previously_served_posts_filter.rs` | `query.is_bottom_request` | `served_ids` `related_post_ids` | 下翻请求里命中已下发帖子 |
| `ViewerMutedKeywordFilter` | `filters/viewer_muted_keyword_filter.rs` | 默认启用 | `tweet_text` `quoted_tweet_text` + `user_features.muted_keywords` | 主文或引用文命中 viewer 屏蔽关键词 |
| `AuthorSocialgraphFilter` | `filters/author_socialgraph_filter.rs` | 默认启用 | `author_id` `blocked_user_ids` `blocked_by_user_ids` `muted_user_ids`；候选级 `author_blocks_viewer` 未装配时中立 | 作者在拉黑、被拉黑或静音列表 |
| `VideoFilter` | `filters/video_filter.rs` | `query.exclude_videos` | `video_duration_ms` | 请求不要视频时，丢掉带时长的候选 |
| `TopicIdsFilter` | `filters/topic_ids_filter.rs` | strict topic 或 excluded topics | topic fields | strict topic 不匹配或命中排除项 |
| `NewUserTopicIdsFilter` | `filters/new_user_topic_ids_filter.rs` | cold-start topics | `new_user_topic_ids` `filtered_topic_ids` `in_network` | 网外且不匹配 cold-start topic |

### 4.2 Post-selection Filters

| 组件 | 文件 | enable | 读取 | 移除条件 |
| --- | --- | --- | --- | --- |
| `VFFilter` | `filters/vf_filter.rs` | 默认启用 | `visibility_decision` `in_network` | Restricted Drop/generic；Unchecked/Unavailable 时移除网外、保留网内 |
| `AncillaryVFFilter` | `filters/ancillary_vf_filter.rs` | 默认启用 | `drop_ancillary_posts` | 引用/转发附属帖受限、漏结果或 VF 不可用 |
| `DedupConversationFilter` | `filters/dedup_conversation_filter.rs` | 默认启用 | `ancestors` `tweet_id` `score` | 同一会话树保留最高分 |

## 5. Scorers

| 组件 | 文件 | enable | 读取 | 写回 | 外部依赖 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| `PhoenixScorer` | `scorers/phoenix_scorer.rs` | 默认启用 | `scoring_sequence`（缺时回退 `user_action_sequence`）、候选 tweet/author 关系 | `phoenix_scores` `prediction_request_id` `last_scored_at_ms` | `PhoenixPredictionClient`（5 s） | 超时保留候选并进入 fallback ranking；`TweetInfo.safety_label_mask` 恒为 0 |
| `RankingScorer` | `scorers/ranking_scorer.rs` | 默认启用 | `phoenix_scores` `video_duration_ms` `author_id` `in_network` | `weighted_score` `score` | 无 | 对齐上游 47c1bcd：权重混合、作者多样性、OON 降权在同一 Scorer 内按序完成（旧 weighted/author_diversity/oon 三个文件已随上游删除） |
| `VMRanker` | `scorers/vm_ranker.rs` | `HOME_MIXER_ENABLE_VM_RANKER=1` 且提供 `VM_RANKER_GRPC_ADDR` | `phoenix_scores` `score` `author_followers_count` `video_duration_ms` | `score` | `GrpcVMRankerClient`（500 ms） | 默认关闭；调用失败时按候选数返回错误交流水线隔离，不伪造分数 |
| `AuthorColdStartScorer` | `scorers/author_cold_start.rs` | demo 且 `HOME_MIXER_ENABLE_AUTHOR_COLD_START` | `view_count` `author_followers_count` | `score` | 无 | 默认关闭；缺曝光或粉丝数的候选不参与 |

## 6. Selector

| 组件 | 文件 | 读取 | 输出规模 | 说明 |
| --- | --- | --- | --- | --- |
| `TopKScoreSelector` | `selectors/top_k_score_selector.rs` | `score` | `TOP_K_CANDIDATES_TO_SELECT` | 分数缺失时视为负无穷 |

## 7. Side Effects

| 组件 | 文件 | enable | 输入 | 外部依赖 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `PhoenixRequestCacheSideEffect` | `side_effects/phoenix_request_cache_side_effect.rs` | `HOME_MIXER_ENABLE_REQUEST_CACHE_SIDE_EFFECT=true` 且请求允许 | `user_id` + 最终候选 tweet ids | `StratoClient.store_request_info` | 默认关闭；fire-and-forget，不阻塞响应 |

## 8. 支撑性工具模块

| 模块 | 文件 | 作用 |
| --- | --- | --- |
| `runtime_config` | `runtime_config.rs` | 解析 Demo/Degraded/ProductionReady 意图并校验启动不变量 |
| `query_builder` | `query_builder.rs` | 校验公共 proto、读取 viewer policy、生成请求身份并以具名字段构造 domain query |
| `debug_access` | `debug_access.rs` | 默认关闭的 Debug RPC token 授权策略 |
| `request_util` | `util/request_util.rs` | 为 `QueryBuilder` 生成请求/预测 ID 和 request time |
| `snowflake` | `util/snowflake.rs` | 从 tweet id 推导创建时间 |
| `bloom_filter` | `util/bloom_filter.rs` | 支持已看过内容去重 |
| `candidates_util` | `util/candidates_util.rs` | 生成 related post ids |
| `post_text` | `post_text/mod.rs` | 屏蔽关键词分词与匹配 |
| `visibility/models` | `visibility/models.rs` | 安全过滤原因和动作模型 |

## 9. 一张依赖关系总图

```mermaid
flowchart TD
    Q1["ScoringSequenceQueryHydrator"] --> SC1["PhoenixScorer"]
    Q2["RetrievalSequenceQueryHydrator"] --> S1["Phoenix / MoE Sources"]
    Q3["FollowedUserIdsQueryHydrator"] --> S2["ThunderSource"]
    Q3 --> H1["InNetworkHydrator"]
    Q4["Blocked / Muted / Subscribed / Safety owners"] --> F8["Keyword / Socialgraph / Subscription filters"]

    H2["CoreDataHydrator"] --> F1["CoreDataHydrationFilter"]
    H2 --> F2["RetweetDeduplicationFilter"]
    H2 --> SC1
    HQ["QuoteHydrator"] --> F3["ViewerMutedKeywordFilter"]

    H3["VideoDurationHydrator"] --> SC2["RankingScorer"]
    H1 --> SC2
    H1 --> H6["VFCandidateHydrator"]
    H6 --> PF1["VFFilter"]
```

## 10. 当前索引最值得记住的两个事实

### 10.1 `CoreDataCandidateHydrator` 是关键枢纽

很多后续逻辑都隐含依赖它补出来的字段，尤其是：

- `tweet_text`
- `retweeted_tweet_id`
- `retweeted_user_id`
- `in_reply_to_tweet_id`

### 10.2 `InNetworkCandidateHydrator` 决定的不只是标签

`in_network` 不仅影响返回字段，还会直接影响：

- `RankingScorer` 内部 OON 阶段的降权
- `VFCandidateHydrator` 的 `SafetyLevel` 选择

所以它其实是排序和安全策略的共同分叉点。

## 11. ForYou 外层 pipeline 组件

`for_you_candidate_pipeline.rs` 在内层 `PhoenixCandidatePipeline` 之外装配了 ForYou 外层链路（query 类型同为 `ScoredPostsQuery`，候选类型为 `FeedItem`）：

| 组件 | 文件 | 说明 |
| --- | --- | --- |
| `ServedHistoryQueryHydrator` | `query_hydrators/served_history_query_hydrator.rs` | 外层 query 补全：已服务历史 |
| `PastRequestTimestampsQueryHydrator` | `query_hydrators/past_request_timestamps_query_hydrator.rs` | 外层 query 补全：历史请求时间戳 |
| `ScoredPostsSource` | `sources/scored_posts_source.rs` | 把内层 pipeline 结果作为外层候选来源 |
| `AdsSource` | `sources/ads_source.rs` | 广告来源（当前 `disabled`） |
| `WhoToFollowSource` | `sources/who_to_follow_source.rs` | 推荐关注来源 |
| `PromptsSource` | `sources/prompts_source.rs` | 提示卡片来源 |
| `PushToHomeSource` | `sources/push_to_home_source.rs` | push 转 home 来源 |
| `BlenderSelector` | `selectors/blender_selector.rs` | 外层混合选择（`BlenderConfig.max_items` 为外层 result_size） |
| `ResponseStatsSideEffect` | `side_effects/response_stats_side_effect.rs` | 外层响应统计（`FeedStatsSink`） |

## 12. 未装配的骨架组件

以下组件存在于代码中，但当前未装配进任何 pipeline（预留扩展点）：

| 组件 | 文件 |
| --- | --- |
| `ImpressedPostsQueryHydrator` | `query_hydrators/impressed_posts_query_hydrator.rs` |
| `ImpressionBloomFilterQueryHydrator` | `query_hydrators/impression_bloom_filter_query_hydrator.rs` |
| `TweetMixerSource` | `sources/tweet_mixer_source.rs` |
| `BlockedByHydrator` | `candidate_hydrators/blocked_by_hydrator.rs`（BlockedBy 仍为 U3） |
| `PublishSeenIdsToKafkaSideEffect` | `side_effects/publish_seen_ids_to_kafka_side_effect.rs` |
| `ServedCandidatesKafkaSideEffect` | `side_effects/served_candidates_kafka_side_effect.rs` |
