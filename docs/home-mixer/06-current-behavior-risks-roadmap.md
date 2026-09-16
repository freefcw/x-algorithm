# 06. 当前行为、风险与补齐路线

本篇只记录当前请求链真实可达的退化行为、剩余风险和生产验收条件。框架执行语义见 [candidate-pipeline 风险文档](../candidate-pipeline/06-risks-tests-and-roadmap.md)，外部合同见 [05-external-deps-and-contracts](./05-external-deps-and-contracts.md)。

## 1. 三种运行模式

| 模式 | 当前行为 | 启动条件 |
| --- | --- | --- |
| `demo` | 注入 Demo UAS、Strato、TES、Gizmoduck、Topic 和 VF adapter，可跑通网内/网外完整演示链路 | `HOME_MIXER_MODE=demo`；`HOME_MIXER_DEMO=1` 仅作兼容别名 |
| `degraded` | 必须配置 `MRPYQ_RECOMMENDATION_DATA_ADDR` 和 `HOME_MIXER_REDIS_URL`：TES / 网内召回 / 兜底召回 / 一级 VF / Strato 走 mrpyq 适配器；UAS 走 Redis adapter，读取 `uas-worker` 投影到 `UAS_REDIS_URL`（缺省复用 `HOME_MIXER_REDIS_URL`）的行为序列；可以启动但不声称生产可用 | 默认模式；缺 mrpyq 地址、缺 Redis 地址或任一地址不可用时启动失败，不回退到整数 Thunder |
| `production_ready` | 当前拒绝启动 | 调用方身份、TES、UAS 事件 schema/保留策略、Strato、VF、网内 / 兜底、Phoenix 元数据、served 落库合同尚未验收（`runtime_config.rs`） |

`HomeMixerConfig` 是 mode 的唯一入口。`HomeMixerServer::build` 把已校验的 mode、`HomeMixerFeatures` 和 `UasConfig` 传给 `PhoenixCandidatePipeline::assemble_with_uas`，Demo/Disabled adapter 不再由 pipeline 自己读 `HOME_MIXER_MODE`。装配层仍读的环境变量：`MRPYQ_RECOMMENDATION_DATA_ADDR`（非 demo 必填）、`PHOENIX_PREDICT_GRPC_ADDR` / `PHOENIX_RETRIEVAL_GRPC_ADDR`（可选），以及旁路地址 `VM_RANKER_GRPC_ADDR`、`PHOENIX_MOE_GRPC_ADDR`；`assemble_for_mode` 这条测试 / 兼容入口自行从环境解析 `UasConfig`，两个 Redis 地址都缺时得到 `Disabled` 并告警。

## 2. Degraded 模式的真实行为

只设置 `MRPYQ_RECOMMENDATION_DATA_ADDR` 和 `HOME_MIXER_REDIS_URL`、不设置其他环境变量时，一次真实请求会这样走：

- 请求默认 `in_network_only=false`，因此 `FallbackSource` 可以读取 mrpyq FALLBACK 池；QueryBuilder 不依赖 Gizmoduck viewer RPC。作者资料为空，响应 `screen_names` 为空。
- `RedisUserActionSequenceStore` 从 `home_mixer:uas:{user_id}:actions` 读取 `uas-worker` 投影的最近 7 天行为。该用户没有投影数据（job 未运行、事件未接入）时序列为空，聚合报错后 `scoring_sequence` / `retrieval_sequence` 为 `None`：`PhoenixSource` 不可用，`PhoenixScorer` 整批标 `phoenix_missing_sequence`，`RuleFallbackScorer` 用“新鲜度 + 网内 + 互动数 + 作者多样性”的规则分覆盖整批。只有投影 job 持续消费真实行为事件、且 `PHOENIX_*_GRPC_ADDR` 配置了可用网关时，模型才会真正参与。
- `MrpyqInNetworkPostsClient` 以 `query.user_id`（皮的 `member_id`）作为 `account_id` 调 mrpyq NETWORK 收件箱；mrpyq 侧目前按账号键读取，皮维度对齐尚未落地（见 `docs/implementation/mrpyq-member-dimension-requirements.md`）。
- `MrpyqTESClient` 补作者（`creator_member_id`）、正文、`created_at_ms`、互动计数与一级 `recommendation_eligible`；`creator_member_id` 为空的帖子被 `CoreDataHydrationFilter` 丢弃。
- `MrpyqStratoClient` 调 `ViewerRelationService`，mrpyq 尚未实现该 RPC；调用失败时框架只记日志，`user_features` 全空，`AuthorSocialgraphFilter` / `ViewerMutedKeywordFilter` 实际不生效（fail-open，根因是 `candidate-pipeline` 对 query hydrator 失败不中断请求）。
- `MrpyqFirstStageEligibilityClient` 只承载一级 `recommendation_eligible`，对经过 `FirstStageEligibleFilter` 存活的候选恒为 Allow，viewer 级可见性没有数据源。`VFFilter` 对 `Unchecked / Unavailable`（含成功响应缺帖）按 `HOME_MIXER_VF_FAILURE_POLICY` 处理：默认 `fail_closed` 全丢弃，`in_network_only` 仅保留网内，`allow_all` 需显式配置且非 demo 下会在启动时告警。
- served 历史通过 `FeedStateServedPersistence` 在响应前等待异步写入；Demo 默认内存，业务模式要求 Redis 地址，多副本共享同一份历史，写失败返回 `Unavailable`。

因此 degraded 在没有 `uas-worker` 投影数据时实际等价于“mrpyq NETWORK + FALLBACK 候选 → 规则排序”的全网 Feed 骨架；接上行为事件流后个性化模型路径才可达。需要本地完整链路时使用 Demo；需要生产流量时必须先让 `production_ready` 的合同校验通过。

## 3. 已经收口的高风险语义

- 网络范围只由请求字段决定：显式 `in_network_only=true` 才限制为仅网内，否则同时允许网内和网外；QueryBuilder 不持有 Gizmoduck client。
- VF 使用 `Allowed / Restricted / Unchecked / Unavailable`，缺失结果记为 `Unavailable` 而非审核通过；故障分支保留范围由 `HOME_MIXER_VF_FAILURE_POLICY` 决定；调用上限 500 ms。
- Phoenix 标准/MoE 召回上限 3 s，预测上限 5 s；Thunder、VF、Topic profile/recall、UAS、Strato read/write、TES 单批和 Gizmoduck profile 均为 500 ms；mrpyq 单次 RPC 500 ms（`MRPYQ_RECOMMENDATION_DATA_TIMEOUT_MS`），一次召回跨页总预算 1500 ms（`MRPYQ_RECALL_BUDGET_MS`）。超时由对应 Source/Hydrator/Scorer/SideEffect 隔离并记录。
- `DebugScoredPosts` 默认关闭，启用时要求 metadata token；未签名 `cached_posts` 默认拒绝且只允许显式 Demo fixture。
- Pipeline 对 Query Hydrator、Source、Hydrator、Scorer、Selector 和 SideEffect 都输出 request-scoped 成功、失败和耗时日志。
- 对外协议与 mrpyq 边界上的身份都是 24 位小写 hex ObjectId 字符串，流水线内是 `PostId` / `UserId`（`models/ids.rs`）；非法串在边界丢弃并计数，`"0"` 不再是合法哨兵。
- Phoenix 响应在适配器内做 serving metadata（`feature-schema` / `model-version` / `random-weights` / `supported-actions`）与形状校验，非 demo 拒绝随机权重；校验失败整批走 `RuleFallbackScorer`。
- UAS、Strato、TES request-scoped provider 已有跨用户/跨请求隔离测试。
- Weighted 负分归一化已与注释和排序测试一致。

## 4. 仍然可达的风险

### 4.1 Post-selection 过滤后不回补

Selector 先保留 50 条，VF、Gizmoduck profile 和会话去重随后执行，最终再截到 35 条。如果 post-selection 删除超过 15 条，响应会少于目标数量；Pipeline 当前不会从 `non_selected_candidates` 回补，但会输出 `result_underfilled` 告警。

修复前先定义回补候选是否必须重新执行 VF、附属内容检查和会话去重，以及额外调用和延迟预算，避免为了补量绕过安全阶段。

### 4.2 普通业务 RPC 的调用方身份合同未闭合

Debug RPC 已有独立 token，unsigned cache 已被隔离，但普通 ScoredPosts/ForYou 请求仍依赖部署层提供可信调用方身份和 viewer 绑定。`production_ready` 当前拒绝启动，所以该缺口不会被误标为生产完成；接入时需明确 mTLS/service identity、viewer 防冒用、审计和密钥轮换责任。

### 4.3 本地 Feed 状态不是生产持久层

Demo 的 `InMemoryFeedStateStore` 有 10,000 用户上限，进程重启会丢失历史。业务模式使用 Redis，以原子事务更新有界下发历史和请求时间戳，默认滑动保留七天；同一请求的各个步骤只加载一份快照。Redis 重启或切主后，首次读取会在新连接上重试一次，保证恢复后的第一个请求仍能看到历史；Redis 持久化、备份和恢复由部署策略决定，未开持久化时历史随重启丢失。客户端只连单一端点，原生 Cluster 路由不受支持，部署前需确认复用的 Redis 是单节点或代理端点。该接口没有 position / event_type，不能作为训练归因的原始事件源——训练归因用的是 `ServedCandidatesKafkaSideEffect` 发布的服务端曝光事件（配置 `SERVED_EVENTS_*` 后装配，见 `docs/implementation/served-candidates-event-contract.md`）；历史共享也不等于同一用户并发请求绝不重复下发。

### 4.4 非 demo 下模型路径依赖 UAS 投影的真实数据

Home Mixer 已在非 demo 下装配 Redis UAS adapter，但序列内容完全取决于 `uas-worker` 是否在消费真实行为事件：没有投影数据的用户拿不到序列，Phoenix 召回与精排对其不执行，候选由 `RuleFallbackScorer` 排序；默认全网范围下 mrpyq FALLBACK 兜底召回仍然可达。事件 JSON 合同、埋点 topic 的 schema / 认证 / 保留与重放策略尚未验收，`action_type` 编号与 proto `ActionName` 强耦合；`product_surface` 已进入事件（省略或旧 v1 成员当 0），真实多入口要靠客户端按 [uas-client-event-reporting.md](../implementation/uas-client-event-reporting.md) 上报。接通事件流并验收这些合同之前，不能把“配置了 Phoenix 地址”当成“模型已上线”。投影 job 单实例串行，吞吐受 Redis 往返时间限制，正式流量前需要按分区多实例部署并压测。

### 4.5 viewer 维度准入没有数据源

`MrpyqStratoClient` 依赖的 `ViewerRelationService` 尚未由 mrpyq 实现，而框架对 query hydrator 失败只记日志，因此“读不到拉黑名单”和“这个皮没拉黑任何人”在过滤器眼里完全一样。`MrpyqFirstStageEligibilityClient` 只承载帖子维度的一级标志。补齐前，拉黑 / 屏蔽词 / 反向屏蔽都不生效；mrpyq 上线关系服务与推荐侧把 Strato 端口迁到 VF 端口必须同批进行，否则按账号键回答皮维度查询会静默 fail-open（见 `docs/implementation/mrpyq-member-dimension-requirements.md` §6.1）。

## 5. 外部合同补齐顺序

1. **Viewer relations + VF**：先闭合用户关系和内容安全语义、认证、超时、漏返回及审计；viewer 关系后端（mrpyq `ViewerRelationService`）与推荐侧端口迁移同批上线。
2. **TES + Gizmoduck**：TES 已由 mrpyq `BatchGetRecommendationContents` 承载，待确认 `creator_member_id` 必填；Gizmoduck 作者昵称 / 粉丝数适配器仍缺。
3. **UAS 事件流 + 持久化 served / feedback**：Redis UAS adapter 与 `uas-worker` 已就位，待验收真实埋点 topic 的事件 schema、认证、保留与重放合同并接入；服务端曝光事件流已由 `ServedCandidatesKafkaSideEffect` + `ServedCandidatesSink` 承担（带 position / served_type / score，幂等键 `request_id`），待确定 topic、保留期与消费方；客户端真实曝光和 feedback 仍需埋点侧回传，按 `request_id` + `post_id` 关联。
4. **Phoenix artifact/service**：用真实 LFS artifact 验证 offline/gRPC 一致性、延迟、容量和 fallback。
5. **普通 RPC 身份边界**：完成调用方身份与 viewer 绑定后，才允许 `production_ready` 启动。

Ads、Prompt、WhoToFollow、PushToHome、Kafka 和 Grox 等旁路能力继续默认关闭；开关存在不等于合同完成。Feed state 的 Redis 为业务模式必需配置。

## 6. 生产验收条件

只有同时满足以下条件，才应解除 `production_ready` 启动拒绝：

1. Viewer、TES、Gizmoduck、VF 使用非 `Disabled*` adapter，并有 owner、schema、认证和 deadline 证据。
2. VF 错误、超时、漏返回、Restricted 和附属内容路径均有回归测试。
3. 普通 RPC 调用方身份不能伪造 viewer，Debug 和缓存数据有独立信任边界。
4. Phoenix 慢服务、不可用和长度不匹配时仍在总请求预算内返回定义好的 fallback。
5. 多用户并发隔离、服务端去重状态和隐私删除通过集成验收。
6. 真实 artifact 的 offline/gRPC 输出与延迟验收完成，而不是只使用 LFS pointer 或 Demo fixture。
