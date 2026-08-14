# 06. 当前行为、风险与补齐路线

本篇只记录当前请求链真实可达的退化行为、剩余风险和生产验收条件。框架执行语义见 [candidate-pipeline 风险文档](../candidate-pipeline/06-risks-tests-and-roadmap.md)，外部合同见 [05-external-deps-and-contracts](./05-external-deps-and-contracts.md)。

## 1. 三种运行模式

| 模式 | 当前行为 | 启动条件 |
| --- | --- | --- |
| `demo` | 注入 Demo UAS、Strato、TES、Gizmoduck、Topic 和 VF adapter，可跑通网内/网外完整演示链路 | `HOME_MIXER_MODE=demo`；`HOME_MIXER_DEMO=1` 仅作兼容别名 |
| `degraded` | 注入显式 `Disabled*` adapter；Viewer/VF 未知时采用保守降级，主服务可以启动但不声称生产可用 | 默认模式 |
| `production_ready` | 当前拒绝启动 | 调用方身份、Viewer、UAS、Strato、TES、Gizmoduck、VF、Phoenix、Thunder 权威合同尚未全部闭合 |

`HomeMixerConfig` 是 mode 的唯一入口。`HomeMixerServer::build` 把已校验 mode 直接传给 `PhoenixCandidatePipeline::assemble_for_mode`，Pipeline 不再重新读取环境变量决定依赖，避免程序化配置与实际装配不一致。

## 2. Degraded 模式的真实行为

默认不设置环境变量时：

- `DisabledUserActionSequenceFetcher` 返回空行为序列，Phoenix 个性化召回和打分输入缺失。
- `DisabledStratoClient` 返回空用户特征并拒绝持久化写入，Thunder 缺少关注列表，请求缓存 SideEffect 默认关闭。
- `DisabledTESClient` 返回空 core data，普通候选可能被 `CoreDataHydrationFilter` 删除。
- `DisabledGizmoduckClient` 返回未知 viewer policy，`QueryBuilder` 将请求限制为仅网内。
- `DisabledVisibilityFilteringClient` 返回 `Unavailable`；`VFFilter` 删除网外候选、保留网内候选，附属引用/转发内容保守删除。

因此 degraded 的目标是“明确、保守、可观测地退化”，不是提供完整 Feed。需要本地完整链路时使用 Demo；需要生产流量时必须先让 `production_ready` 的合同校验通过。

## 3. 已经收口的高风险语义

- Viewer policy 只有明确 Allow 才开放网外；错误和 200 ms 超时都限制为仅网内。
- VF 使用 `Allowed / Restricted / Unchecked / Unavailable`，不再把缺失结果当作审核通过；调用上限 500 ms。
- Phoenix 标准/MoE 召回上限 3 s，预测上限 5 s；Viewer 为 200 ms；Thunder、VF、Topic profile/recall、UAS、Strato read/write、TES 单批和 Gizmoduck profile 均为 500 ms。超时由对应 Source/Hydrator/Scorer/SideEffect 隔离并记录。
- `DebugScoredPosts` 默认关闭，启用时要求 metadata token；未签名 `cached_posts` 默认拒绝且只允许显式 Demo fixture。
- Pipeline 对 Query Hydrator、Source、Hydrator、Scorer、Selector 和 SideEffect 都输出 request-scoped 成功、失败和耗时日志。
- signed wire/store ID 到 `u64` domain ID 使用 checked conversion，负值不会静默变成大整数。
- UAS、Strato、TES request-scoped provider 已有跨用户/跨请求隔离测试。
- Weighted 负分归一化已与注释和排序测试一致。

## 4. 仍然可达的风险

### 4.1 Post-selection 过滤后不回补

Selector 先保留 50 条，VF、Gizmoduck profile 和会话去重随后执行，最终再截到 35 条。如果 post-selection 删除超过 15 条，响应会少于目标数量；Pipeline 当前不会从 `non_selected_candidates` 回补，但会输出 `result_underfilled` 告警。

修复前先定义回补候选是否必须重新执行 VF、附属内容检查和会话去重，以及额外调用和延迟预算，避免为了补量绕过安全阶段。

### 4.2 普通业务 RPC 的调用方身份合同未闭合

Debug RPC 已有独立 token，unsigned cache 已被隔离，但普通 ScoredPosts/ForYou 请求仍依赖部署层提供可信调用方身份和 viewer 绑定。`production_ready` 当前拒绝启动，所以该缺口不会被误标为生产完成；接入时需明确 mTLS/service identity、viewer 防冒用、审计和密钥轮换责任。

### 4.3 本地 Feed 状态不是生产持久层

`InMemoryFeedStateStore` 有 10,000 用户上限，不是无界内存增长；但进程重启会丢失去重历史，多实例也不共享状态。生产合同仍需定义服务端存储、保留期、并发一致性、隐私删除和恢复策略。

## 5. 外部合同补齐顺序

1. **Viewer + VF**：先闭合用户选择和内容安全语义、认证、超时、漏返回及审计。
2. **TES + Gizmoduck**：恢复权威内容和作者字段，验证删除/停用账号行为。
3. **UAS + Strato**：恢复个性化序列、关系特征和持久去重；写回必须有幂等和保留期。
4. **Phoenix artifact/service**：用真实 LFS artifact 验证 offline/gRPC 一致性、延迟、容量和 fallback。
5. **普通 RPC 身份边界**：完成调用方身份与 viewer 绑定后，才允许 `production_ready` 启动。

Ads、Prompt、WhoToFollow、PushToHome、Kafka/Redis 和 Grox 等旁路能力继续默认关闭；开关存在不等于合同完成。

## 6. 生产验收条件

只有同时满足以下条件，才应解除 `production_ready` 启动拒绝：

1. Viewer、TES、Gizmoduck、VF 使用非 `Disabled*` adapter，并有 owner、schema、认证和 deadline 证据。
2. VF 错误、超时、漏返回、Restricted 和附属内容路径均有回归测试。
3. 普通 RPC 调用方身份不能伪造 viewer，Debug 和缓存数据有独立信任边界。
4. Phoenix 慢服务、不可用和长度不匹配时仍在总请求预算内返回定义好的 fallback。
5. 多用户并发隔离、服务端去重状态和隐私删除通过集成验收。
6. 真实 artifact 的 offline/gRPC 输出与延迟验收完成，而不是只使用 LFS pointer 或 Demo fixture。
