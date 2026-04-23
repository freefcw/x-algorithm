# 组件清单与职责对照

本篇按组件类别给出当前 `PhoenixCandidatePipeline` 的完整清单，重点说明：

- 什么时候会启用
- 读什么字段
- 写什么字段
- 依赖哪些外部系统
- 下游哪些组件依赖它

## 1. Query Hydrators

| 组件 | enable 条件 | 读取 | 写回 | 外部依赖 | 备注 |
| --- | --- | --- | --- | --- | --- |
| `UserActionSeqQueryHydrator` | 默认启用 | `query.user_id` | `query.user_action_sequence` | `UserActionSequenceFetcher` | 空行为序列会报错，不会写回 |
| `UserFeaturesQueryHydrator` | 默认启用 | `query.user_id` | `query.user_features` | `StratoClient` | 多数后续过滤和网内判定都依赖它 |

## 2. Sources

| 组件 | enable 条件 | 读取 | 产出字段 | 外部依赖 | 备注 |
| --- | --- | --- | --- | --- | --- |
| `PhoenixSource` | `!query.in_network_only` | `user_id`、`user_action_sequence` | `tweet_id`、`author_id`、`in_reply_to_tweet_id`、`served_type=ForYouPhoenixRetrieval` | `PhoenixRetrievalClient` | 缺少 `user_action_sequence` 时直接报错 |
| `ThunderSource` | 默认启用 | `user_id`、`user_features.followed_user_ids` | `tweet_id`、`author_id`、`in_reply_to_tweet_id`、`ancestors`、`served_type=ForYouInNetwork` | `ThunderClient` | 用 reply/conversation 关系预先构造 `ancestors` |

## 3. Pre-selection Hydrators

| 组件 | 读取 | 写回 | 外部依赖 | 下游依赖 |
| --- | --- | --- | --- | --- |
| `InNetworkCandidateHydrator` | `query.user_id`、`followed_user_ids`、`candidate.author_id` | `candidate.in_network` | 无 | `OONScorer`、`VFCandidateHydrator` |
| `CoreDataCandidateHydrator` | `candidate.tweet_id` | `retweeted_user_id`、`retweeted_tweet_id`、`in_reply_to_tweet_id`、`tweet_text` | `TESClient.get_tweet_core_datas` | `CoreDataHydrationFilter`、`RetweetDeduplicationFilter`、`MutedKeywordFilter` |
| `VideoDurationCandidateHydrator` | `candidate.tweet_id` | `video_duration_ms` | `TESClient.get_tweet_media_entities` | `WeightedScorer` 的 VQV 权重判断 |
| `SubscriptionHydrator` | `candidate.tweet_id` | `subscription_author_id` | `TESClient.get_subscription_author_ids` | `IneligibleSubscriptionFilter` |
| `GizmoduckCandidateHydrator` | `author_id`、`retweeted_user_id` | `author_followers_count`、`author_screen_name`、`retweeted_screen_name` | `GizmoduckClient` | `server.rs` 的响应映射、未来分数归一化 |

额外说明：

- `CoreDataCandidateHydrator` 在内部读取了 `core_data.author_id`，但 `update()` 并不会写回 `candidate.author_id`，因此作者 ID 仍以 Source 填充为准。
- `GizmoduckCandidateHydrator` 想补 `retweeted_screen_name`，但它运行时读到的是该 stage 开始前的候选快照，这会影响它读取 `CoreDataCandidateHydrator` 刚补出来的 `retweeted_user_id`。这个问题在风险文档里单独展开。

## 4. Pre-selection Filters

| 组件 | enable 条件 | 核心规则 | 依赖字段 |
| --- | --- | --- | --- |
| `DropDuplicatesFilter` | 默认启用 | 按 `tweet_id` 去重，保留首次出现 | `tweet_id` |
| `CoreDataHydrationFilter` | 默认启用 | 要求 `author_id != 0` 且 `tweet_text` 非空 | `author_id`、`tweet_text` |
| `AgeFilter` | 默认启用 | 通过 Snowflake 解析发布时间，过滤超过 48 小时的帖子 | `tweet_id` |
| `SelfTweetFilter` | 默认启用 | 过滤作者就是 viewer 的帖子 | `query.user_id`、`author_id` |
| `RetweetDeduplicationFilter` | 默认启用 | 原帖与转推共享同一去重域，保留首次出现 | `tweet_id`、`retweeted_tweet_id` |
| `IneligibleSubscriptionFilter` | 默认启用 | 订阅帖作者不在 viewer 的订阅列表里则过滤 | `subscription_author_id`、`query.user_features.subscribed_user_ids` |
| `PreviouslySeenPostsFilter` | 默认启用 | `seen_ids` 或布隆过滤器命中任一 related post id 即过滤 | `tweet_id`、`retweeted_tweet_id`、`in_reply_to_tweet_id`、`seen_ids`、`bloom_filter_entries` |
| `PreviouslyServedPostsFilter` | 仅 `query.is_bottom_request` | `served_ids` 命中 related post id 即过滤 | `served_ids`、related post ids |
| `MutedKeywordFilter` | 默认启用 | 文本命中 muted keyword 即过滤 | `tweet_text`、`query.user_features.muted_keywords` |
| `AuthorSocialgraphFilter` | 默认启用 | 作者在 muted/block 列表则过滤 | `author_id`、`blocked_user_ids`、`muted_user_ids` |

## 5. Scorers

| 组件 | 输入 | 写回 | 外部依赖 | 下游依赖 |
| --- | --- | --- | --- | --- |
| `PhoenixScorer` | `user_action_sequence` + 候选 tweet/author 信息 | `phoenix_scores`、`prediction_request_id`、`last_scored_at_ms` | `PhoenixPredictionClient` | `WeightedScorer` |
| `WeightedScorer` | `phoenix_scores`、`video_duration_ms` | `weighted_score` | 无 | `AuthorDiversityScorer` |
| `AuthorDiversityScorer` | `weighted_score`、`author_id` | `score` | 无 | `OONScorer`、`Selector` |
| `OONScorer` | `score`、`in_network` | `score` | 无 | `Selector` |

额外说明：

- `PhoenixScorer` 对转推会优先使用原始 tweet id / user id 作为模型输入和结果查找 key。
- `AuthorDiversityScorer` 会先按 `weighted_score` 排一个内部顺序，再按作者出现次数做衰减，最后把结果写回原始索引对应的位置。
- `OONScorer` 只对 `in_network == Some(false)` 的候选做乘法降权。

## 6. Selector

| 组件 | 规则 | 使用字段 | 输出规模 |
| --- | --- | --- | --- |
| `TopKScoreSelector` | 按 `candidate.score` 降序排序 | `score` | Top 100 |

说明：

- 若 `score` 缺失，则用 `f64::NEG_INFINITY`，意味着此类候选天然排在最后。
- selector 后还会经历 post-selection 过滤和最终 `result_size=50` 裁剪。

## 7. Post-selection Hydrators

| 组件 | 读取 | 写回 | 外部依赖 | 备注 |
| --- | --- | --- | --- | --- |
| `VFCandidateHydrator` | `query.user_id`、viewer context、`candidate.in_network`、`tweet_id` | `visibility_reason` | `VisibilityFilteringClient` | 网内与网外分开用不同 `SafetyLevel` 查询 |

## 8. Post-selection Filters

| 组件 | 核心规则 | 依赖字段 | 备注 |
| --- | --- | --- | --- |
| `VFFilter` | `visibility_reason` 为 `Drop` 或 generic filtered 时剔除 | `visibility_reason` | `Interstitial` / `SoftIntervention` 不会被 drop |
| `DedupConversationFilter` | 同一会话树只保留分数最高的一条 | `ancestors`、`tweet_id`、`score` | 会话 ID 取 `ancestors` 最小值，否则取自身 tweet id |

## 9. Side Effects

| 组件 | enable 条件 | 输入 | 外部依赖 | 备注 |
| --- | --- | --- | --- | --- |
| `CacheRequestInfoSideEffect` | `APP_ENV == "prod"` 且 `!query.in_network_only` | `query.user_id`、最终 `selected_candidates` 的 `tweet_id` 列表 | `StratoClient.store_request_info` | 结果不影响主链路返回 |

## 10. 组件依赖链总结

当前这条流水线里存在几条特别关键的依赖链：

1. `UserActionSeqQueryHydrator` -> `PhoenixSource`
2. `UserActionSeqQueryHydrator` -> `PhoenixScorer`
3. `UserFeaturesQueryHydrator` -> `ThunderSource`
4. `UserFeaturesQueryHydrator` -> `InNetworkCandidateHydrator`
5. `CoreDataCandidateHydrator` -> `CoreDataHydrationFilter`
6. `CoreDataCandidateHydrator` -> `RetweetDeduplicationFilter`
7. `CoreDataCandidateHydrator` -> `MutedKeywordFilter`
8. `VideoDurationCandidateHydrator` -> `WeightedScorer`
9. `InNetworkCandidateHydrator` -> `OONScorer`
10. `InNetworkCandidateHydrator` -> `VFCandidateHydrator`
11. `VFCandidateHydrator` -> `VFFilter`

如果要调整组件顺序，至少要先核对这些依赖链是否仍然成立。
