# Goal Document: Home Mixer 身份边界五阶段加固

## Go / No-Go

- **Judgment**: Go
- **Reason**: 当前工作树已有 request-local identity、Resolve/Allocate capability 和多数 adapter 接入；剩余工作可以拆成可独立验证的行为切片，未发现必须先做产品决策的阻断项。

## Target Outcome

Home Mixer 在一次推荐请求内复用 ObjectId ↔ Snowflake 映射，所有生产身份外部接触点都显式经过请求上下文或明确的注册边界；VF、响应、Feed State、UAS、Served Persistence 和 Served Event 的预算、失败语义和观测口径可验证，未知 hint 不再造成逐 ID Registry RPC 扇出。

## Goal Definition

- **Type**: technical / quality
- **Boundary**: Home Mixer RPC ingress、request identity context、mrpyq/TES/VF/Strato/UAS/Feed State/response/served-event identity adapters，以及 ID Registry 的 ResolveBatch 合同。
- **Non-goals**:
  - 不重写 candidate-pipeline 的通用泛型执行框架。
  - 不改变推荐排序、过滤策略或 ObjectId/Snowflake 的业务语义。
  - 不把 UAS worker 的跨进程事件注册改成 request-local；worker 仍是独立注册边界。
- **Deferred work**:
  - 复杂 batch coalescer 仍需线上 batch/latency 数据后评估；本轮已完成低风险 per-key single-flight。
  - 真实 Redis / mrpyq / VF 线上联调不在本轮自动化范围内。
- **Verification rule**: 每个阶段必须有针对边界行为的测试和对应的格式/编译验证；最终以全 workspace 测试、fmt、home-mixer tests check 和代码边界复核通过为准。
- **Evidence source**: Rust 单元/集成测试、Registry contract tests、请求 deadline 观察、identity call counters、代码搜索和文档一致性复核。
- **Pass criteria**: VF-first 不调用 adapter 级 identity；请求所有阶段共享一个绝对 deadline；未知 hint 使用 partial batch 而非逐条 fallback；生产 Query 只通过显式 registration boundary 使用 Allocate；主流程与异步 side effect 的 identity stats 语义明确；同一请求内不同 identity key 可并发、相同 key 仍 single-flight；全量验证通过。
- **Confidence note**: 现有 request-local、TES、UAS、Strato 和 Registry timeout 测试已经覆盖大部分基础路径；VF 并发顺序、端到端 deadline 和 partial resolve 是新增证据点。
- **Judgment owner**: 测试套件负责行为门禁，代码审查负责边界与迁移判断，用户负责最终接受 API/协议迁移范围。

## Current State

- `IdentityContext` 已提供 forward/reverse cache、去重、按键 single-flight 和 deadline forwarding。
- QueryBuilder 已从 Allocate 改为 Resolve；未知 hint 在一次 partial batch 中过滤。
- `IdentityReader` 为外部旧适配器保留了逐项 partial 的兼容默认实现；生产 RegistryClient 必须走真正的 `ResolveBatchPartial`，因此在线主路径不再有 O(N) fallback。
- TES、Feed State、UAS 在线读取、Strato、响应和 Served Persistence 已有 request-aware 路径。
- VF 三路请求均携带 request-local registration context；缓存 miss 不再回退到 adapter 级 resolver。
- RPC 已在 QueryBuilder、pipeline、For You 出口 reverse 和 scored persistence 之间共享同一个绝对 deadline。
- Served Event 异步执行；主流程输出 `main_path_identity_registry_snapshot`，side effect 完成时另行输出 `side_effect_identity_registry_snapshot`。side effect 通过共享同一份 request-local cache/stats、但重启自己的 `SIDE_EFFECT_TIMEOUT_MS` 绝对预算，避免主请求耗尽预算后曝光事件无法反查身份；两者都是明确标注生命周期的快照，而不是跨生命周期的单一最终账单。

## Priority Rationale

先修 VF 和统一 deadline，因为两者分别是 request-local 正确性和请求生命周期正确性的 P1；随后解决 partial Resolve，降低输入规模导致的 Registry 扇出；再收紧 capability、清理死字段和明确观测语义；最后才优化 single-flight，避免在行为未稳定前放大迁移面。

## Assumptions and Open Decisions

| Item | Status | Impact | Owner / Next step |
|------|--------|--------|-------------------|
| Served Event 是否必须阻塞主请求 | unresolved | 决定 side-effect 是否纳入响应延迟 | 保留异步；side effect 独立受 `SIDE_EFFECT_TIMEOUT_MS` 约束并单独记账。若训练链路要求强一致，再单独决策是否改为同步 |
| ResolveBatch partial contract 形态 | resolved | `ResolveBatchPartial` 保留 all-or-none `ResolveBatch`，逐行返回 optional Snowflake | gRPC/HTTP/Home Mixer contract tests 已通过 |
| 兼容构造器是否继续支持无 request context 的测试 Query | confirmed | 影响 registration capability 收口方式 | 保留 padded resolver 测试路径，但生产构造必须来自 RequestContext |

## Phases

### Phase 1: VF request-local identity

- **Purpose**: 消除 VF/TES 并发下的 stage-order 依赖。
- **Entry condition**: TES request-aware API 已通过现有测试。
- **Phase rules**:
  - 保留旧 `get_result` 兼容实现；新增 request-aware 默认接口。
  - 不改变 VF 的过滤结果语义，只改变身份来源。
  - 必须先 RED 再改生产代码。
- **Todos**:
  - [x] 为 VF trait、hydrator 和 mrpyq VF adapter 增加 request-aware 调用测试。
    - **Surface**: `home-mixer/visibility`, `candidate_hydrators`, `clients/mrpyq_adapters.rs`
    - **Proof**: VF-first / 并发测试中 request resolver Allocate > 0 且 adapter resolver Allocate == 0。
  - [x] 传递 `IdentityRegistrationContext` 到三路 VF 查询。
    - **Surface**: VF trait/hydrator/ContentCache
    - **Proof**: targeted home-mixer tests。
- **Exit proof**: `cargo test -p home-mixer candidate_hydrators::vf_candidate_hydrator clients::mrpyq_adapters --lib` 通过。
- **Stop condition**: VF 真实契约需要 viewer-specific ObjectId 字段且当前 mrpyq 不能提供时，暂停并记录接口决策。

### Phase 2: 绝对请求 deadline

- **Purpose**: 把 QueryBuilder、pipeline、出口 reverse 和 persistence 纳入同一请求预算。
- **Entry condition**: Phase 1 通过。
- **Phase rules**:
  - 使用绝对 `Instant`，不再在阶段之间重新发放完整 budget。
  - side effect 继续异步；不把其等待强行加入主响应路径。
- **Todos**:
  - [x] 增加 build 已消耗预算后 pipeline 只能使用剩余时间的 RED 测试。
  - [x] 引入 absolute deadline helper，并接入 RPC 入口与出口 reverse。
  - [x] IdentityContext/Registry transport 使用同一请求剩余时间；过期 deadline 立即失败。
- **Exit proof**: deadline integration tests、`cargo test -p home-mixer rpc_policy id` 通过。
- **Stop condition**: 发现 tonic Endpoint timeout 与 per-request timeout 语义冲突时，先补 transport contract test 再继续。

### Phase 3: partial Resolve 与 hint fan-out

- **Purpose**: 删除 QueryBuilder 的逐 ID fallback。
- **Entry condition**: Phase 2 通过，Registry contract 形态已确定。
- **Phase rules**:
  - viewer mapping 必须存在；未知 history/filter hint 只丢弃。
  - transport、版本、entity kind 等非 mapping miss 仍终止请求。
- **Todos**:
  - [x] 先为 id-service 添加逐行 found/missing contract test。
  - [x] 实现 Registry partial batch API 或等价响应。
  - [x] 删除 `resolve_known_individually` 并增加调用次数回归测试。
- **Exit proof**: N 个未知 hint 的 Registry call count 不随 N 线性增长，id-service 与 home-mixer tests 全绿。
- **Stop condition**: API 变更需要外部消费者迁移且无兼容窗口时，暂停并请求用户确认。

### Phase 4: capability 与观测边界

- **Purpose**: 让只读 Query 不再随意取得 Allocate，并使 side-effect stats 语义诚实。
- **Entry condition**: Phase 1-3 通过。
- **Phase rules**:
  - 不重写 pipeline 泛型；优先采用私有 capability 或显式 execution context。
  - 保留 UAS worker 独立注册能力。
- **Todos**:
  - [x] 移除 `PhoenixDependencies.identity` public dead field。
  - [x] 收紧 `ScoredPostsQuery` 的生产 identity 构造，避免 read 与 registration context 脱节；为兼容既有 `..Default::default()` 集成测试，三个 doc-hidden 字段仍保留 public，强类型完全私有化留待单独迁移。
  - [x] 将日志和文档拆成 main-path 与 side-effect identity stats，明确异步 side-effect 的快照语义（超时仍由 pipeline side-effect timeout 控制）。
- **Exit proof**: 代码搜索不再发现 pipeline 持有死 identity 字段；观测文档与实现一致；相关 tests 通过。
- **Stop condition**: capability 收口需要改变外部公共 adapter API 时，先保留兼容 shim 并记录迁移边界。

### Phase 5: 性能优化与最终验证

- **Purpose**: 在正确性闭环后消除方向级串行和不必要的批次等待。
- **Entry condition**: 前四阶段通过且已有 Registry latency/batch metrics。
- **Phase rules**:
  - 只做有测试和指标证明收益的 single-flight/coalescing 优化。
  - 不为了减少锁而牺牲 mapping 顺序、错误语义或 deadline。
- **Todos**:
  - [x] 评估并实现 per-key in-flight；复杂 batch coalescer 暂不引入。
  - [x] 增加不同 key 并发、相同 key single-flight 回归测试；既有 deadline/错误传播测试继续覆盖。
  - [x] 完成文档、fmt、check、workspace tests。
- **Exit proof**: 性能测试证明并发不再被单一方向锁完全串行，且全量验证通过。
- **Stop condition**: 无可重复的性能收益或引入复杂状态机时，保留当前实现并记录 defer。

## Dry-Run Findings

- Phase 1 可以独立完成，且不会与 Phase 2 的 deadline API 冲突。
- Phase 3 依赖 id-service 协议决策，不能仅在 Home Mixer 内部伪造 partial 语义。
- Phase 4 的 capability 收口应晚于 VF/TES 接口稳定，否则会同时迁移两套 adapter API。
- Phase 5 的 per-key 优化以可重复并发测试为第一证据；复杂 batch coalescer 仍需线上 batch/latency 数据后再决定。

## Final Validation

- `cargo fmt --all -- --check` ✅
- `cargo check -p home-mixer --tests` ✅
- `cargo test --workspace` ✅
- `cargo test -p id-service` ✅
- `cargo clippy -p home-mixer --all-targets -- -D warnings` ✅
- 关键边界测试：VF-first、绝对 deadline、partial Resolve、独立的 main/side-effect stats、partial miss negative cache、重叠 batch single-flight。
- 代码搜索确认 candidate-pipeline、Thunder、VM Ranker 不直接访问 Registry。

## Completion Note

五个阶段已按 RED/GREEN、合同测试、质量门禁和全量验证闭环；下一步仅保留两项需要线上证据的后续工作：真实 Redis/mrpyq/VF 联调，以及复杂 batch coalescer 的 latency/batch-size 评估。
