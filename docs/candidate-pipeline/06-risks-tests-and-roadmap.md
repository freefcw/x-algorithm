# 风险、测试缺口与改进路线

本篇只记录 candidate-pipeline 框架层面仍然可达的风险。Home Mixer 的 Viewer、VF、运行模式和外部合同见 [Home Mixer 风险文档](../home-mixer/06-current-behavior-risks-roadmap.md)。

## 1. 已解决的框架级问题

- 同 stage Hydrator 的快照语义已被明确记录；Home Mixer 将依赖 CoreData 的 Gizmoduck profile hydration 移到 post-selection，避免读取未写回的 retweet author。
- Pipeline 对每个 stage 输出 request-scoped component、耗时、输入/输出规模和错误日志；SideEffect 失败不会静默丢失。
- Selector 后的 underfill 会输出 `result_underfilled` 告警，包含 target、actual、post-selection 删除数量和未选候选数量，但不会绕过安全过滤进行补量。
- Candidate Hydrator 和 Scorer 的等长保护与逐候选错误隔离（`hydrator.rs` / `scorer.rs` 中长度不匹配时整份转为逐候选 `Err`）、Query Hydrator 和 Source 的逐组件失败隔离（失败后忽略该组件输出）由框架统一处理。

## 2. 仍然可达的风险

### 2.1 Post-selection 删除后不回补

Selector 当前保留 Top 50，post-selection 过滤后再截到 35。如果 VF、附属内容安全或会话去重删除较多候选，最终响应可能少于目标数量。当前只告警，不从未选候选回补，因为回补必须重新执行完整 VF、附属内容检查和会话去重，且会增加外部调用与延迟。

要实现回补，需要先确定请求总预算、最大补回批次、VF 缓存策略、排序稳定性和安全责任，不能只把 `non_selected_candidates` 直接拼入响应。

### 2.2 Filtered candidates 没有统一来源码

`PipelineResult.filtered_candidates` 保留被删除的候选，但公共结果目前没有统一的 filter name/reason 结构。日志包含组件名，Debug RPC 也有阶段计数和 ID，但长期观测仍需要稳定的内部 reason code、采样策略和隐私保留期。

### 2.3 Fire-and-forget SideEffect 的生命周期

SideEffect 失败会记录日志，但 `tokio::spawn` 任务在进程关闭或 runtime 被回收时可能未完成。生产缓存写回必须有 durable queue、重试/幂等、超时、停机 drain 和恢复责任，不能把日志视为持久化成功证据。

### 2.4 框架泛型合同仍允许过宽的降级

Candidate Pipeline 对单个 hydrator/scorer/filter 错误采取逐组件隔离，这是 portable migration 需要的默认语义；内容安全、viewer eligibility 和身份认证因此不能只依赖通用 fallback，必须在对应领域 filter 或 RPC 边界显式定义 fail-safe 状态。当前 VF 和 Viewer 已采用该方式，新增安全组件需要遵循同一约束。

## 3. 测试重点

当前已有：

- workspace/Home Mixer 全链路测试
- Selector 结果顺序和 underfill 相关框架行为测试
- UAS、Strato、TES 跨用户/跨请求隔离测试
- Viewer policy、VF 缺失/错误/超时、Debug token、unsigned cache 和 mode validation 测试
- Demo ScoredPosts、ForYou、Debug wire 验收

下一步应优先补：

1. post-selection underfill 的可观测性回归，确认过滤数量和最终条数一致。
2. Gizmoduck profile 在 post-selection 读取 CoreData retweet author 的装配合同测试。
3. SideEffect shutdown drain、重试和幂等的 adapter 集成测试。
4. 生产持久化状态与多实例一致性的端到端测试。

## 4. 修改原则

框架层继续保持上游公共执行语义；安全和身份策略放在 Home Mixer application boundary；外部 adapter 用显式类型表示 `Demo`、`Disabled` 或真实集成状态。任何回补、写回或放宽降级的改动，都必须同时给出延迟预算、认证、隐私保留、恢复 owner 和回归测试。
