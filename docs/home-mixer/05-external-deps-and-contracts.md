# 外部依赖与合同

> **状态：`current-code`**

本文只记录当前 Home Mixer 装配中仍有代码依据的外部边界。仓库没有脱离外部服务的本地完整推荐 Demo；不存在可供恢复的旧 gateway 启动步骤。

## 1. 依赖总览

| 依赖 | 代码边界 | 当前要求 |
| --- | --- | --- |
| Recommendation Data | `recommendation_data.proto`、`clients/mrpyq_adapters.rs` | 承载候选、内容补全、兜底和一级 eligibility；地址由 `MRPYQ_RECOMMENDATION_DATA_ADDR` 提供 |
| Viewer relations | `viewer_relation.proto`、Strato adapter | 提供拉黑、静音和屏蔽词等 viewer 关系；失败不能被当成业务事实 |
| Redis | `RedisUserActionSequenceStore`、`RedisFeedStateStore` | 保存 UAS 投影和有界 served/请求状态；生产需要共享、持久且可恢复的实例 |
| UAS worker | `home-mixer/bin/uas_worker.rs` | 将真实行为事件投影到 Redis；事件 schema、认证、保留和重放需要单独验收 |
| Phoenix client | `phoenix_retrieval_client.rs`、`phoenix_prediction_client.rs` | 当前客户端是既有 Phoenix 合同；xrex serving 合同尚未完成适配 |
| served events | `ServedCandidatesKafkaSideEffect` | 记录服务端曝光，供归因和训练使用；不能用 FeedState 代替原始曝光事件 |
| Thunder / VM Ranker | 各自 proto 和 client | 非生产主线；整数 ID 合同不能替代真实 ObjectId，不应作为当前生产启动步骤 |

## 2. 请求链路

```text
调用方
  → Home Mixer
  → RecommendationData / viewer relations / Redis
  → 候选补全、可见性和去重
  → Phoenix client（合同适配完成后）或 RuleFallbackScorer
  → served persistence / exposure event
  → Feed response
```

安全、删除、权限、拉黑、静音和可见性判断先于排序。Phoenix 不可用时只能按已验收的规则回退，不能填充随机分数或占位内容。

## 3. 配置边界

- `MRPYQ_RECOMMENDATION_DATA_ADDR` 缺失时，真实业务装配必须失败；
- `HOME_MIXER_REDIS_URL` 或 cluster 配置缺失时，不能声称 served/UAS 状态已具备生产能力；
- `PHOENIX_PREDICT_GRPC_ADDR` 与 `PHOENIX_RETRIEVAL_GRPC_ADDR` 只有在协议、metadata、shape、超时和错误语义完成适配后才能指向 xrex；
- 不要使用 `HOME_MIXER_MODE=demo`、旧 `run_grpc_gateway.py`、`demo-client` 或旧 Thunder seed 作为配置示例。

## 4. 合同验收清单

每个 Adapter 进入生产装配前必须有：字段和 ID 空间、认证、连接/请求超时、重试边界、错误分类、指标、日志、fixture 单测和测试环境集成证据。缺少真实合同的能力保持关闭，不用 Disabled 或历史 Demo 实现伪装成生产完成。
