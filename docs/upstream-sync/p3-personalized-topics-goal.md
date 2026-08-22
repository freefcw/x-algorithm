# Goal Document: P3 个性化话题召回改造

> 文档性质：本文件记录该改造阶段的目标、决策和完成时证据；“阶段完成时证据”是不可变历史快照，不代表仓库当前累计测试数。最新回归结果单列在“当前回归证据”，跨阶段权威汇总见 [`e414c17-to-mp-capability-inventory.md`](./e414c17-to-mp-capability-inventory.md) 的 `EV-*` 与 Final Validation。

## Go / No-Go
- **Judgment**: Go
- **Reason**: 现有候选流水线已支持 Query Hydrator、可注入 Client 和多来源并行执行，能够在不新增服务的前提下完成改造。

## Target Outcome
话题页只返回指定话题的帖子；`new_user_topic_ids` 保持上游冷启动语义，只保留网内或命中冷启动话题的候选；推荐首页只有在显式注入补充话题 Adapter 时才混合话题召回，并在依赖失败时保留标准召回。

## Goal Definition
- **Type**: product / technical / quality
- **Boundary**: Home Mixer 内部的话题语义、用户话题读取边界、话题选择规则、来源启用规则、装配和自动化测试。
- **Non-goals**:
  - 新建独立个性化话题微服务。
  - 在 Home Mixer 内保存关注关系或运行兴趣推断模型。
  - 在没有真实下游合同的情况下虚构生产 gRPC/HTTP 协议。
- **Deferred work**:
  - 推断话题的生产数据管道、隐私审批和效果实验。
  - 关注话题与推断话题的复杂比例分配。
- **Verification rule**: 相关单元测试和 `cargo test -p home-mixer` 全部通过，且格式化、Clippy 不引入本次变更导致的问题。
- **Evidence source**: Rust 自动化测试、编译结果和代码边界复核。
- **Pass criteria**: 显式话题、冷启动话题、补充话题三种来源的召回和过滤规则均有测试；补充话题选择由外部 Adapter 负责；读取失败和话题召回失败均安全降级。
- **Confidence note**: 测试直接覆盖 Source 启用规则和 Query Hydrator 的公开行为，不依赖真实外部服务。
- **Judgment owner**: 自动化测试和最终代码复核。

## Current State
- `TopicRetrievalClient` 和 Demo 实现已经存在，但生产装配只在 Demo 模式启用。
- `topic_ids` 是显式话题页输入，`new_user_topic_ids` 是公开冷启动合同；两者不能再与内部补充话题共用同一领域字段。
- 生产 Topic Adapter 默认不装配；Demo 通过显式依赖对象启用补充话题读取和话题召回。
- 工作区已有其他功能改动；本目标不得覆盖或回滚这些改动。

## Priority Rationale
- 先用测试固定“严格”和“混合”语义，避免接入画像后误替换整个首页召回。
- 再增加用户话题读取边界，最后装配，减少跨模块同时变化带来的定位成本。

## Assumptions and Open Decisions
| Item | Status | Impact | Owner / Next step |
|---|---|---|---|
| `new_user_topic_ids` 保持上游冷启动语义 | confirmed | 防止同一 wire 字段发生行为破坏 | 公开 proto 合同测试 |
| 首页补充话题由 Adapter 直接返回已选 ID | confirmed | Home Mixer 不推断关注/推断优先级或用户资格 | `UserTopicReader` 窄端口 |
| 补充话题是条件启用的混合召回 | confirmed | 只有显式装配才保留标准 Phoenix 并增加话题 Source | 公开装配合同 |
| 生产用户话题远端协议 | unresolved | 无法实现真实网络 Adapter | 平台确定数据源后补充 |
| 生产帖子话题召回协议 | unresolved | 无法实现真实网络 Adapter | 通过公开原子依赖边界保留接入点 |

## Phases

### Phase 1: 固定三种话题来源的业务语义
- **Purpose**: 用测试证明显式话题、冷启动话题和补充话题不会互相改写合同。
- **Entry condition**: 当前测试可运行。
- **Phase rules**:
  - 先写失败测试，再改生产代码。
  - 不修改公开 proto 字段及编号。
  - `new_user_topic_ids` 关闭普通 Phoenix/MoE，并执行网内或话题命中过滤。
  - 补充话题只在内部显式装配后使用 Blend。
- **Todos**:
  - [x] 为三种来源的 Query、Source、Filter 和 Scorer 规则增加失败测试。
    - **Surface**: home-mixer 单元测试
    - **Proof**: 测试因缺少 `ColdStart` 和独立字段而编译失败
    - **Depends on**: none
  - [x] 增加明确的话题来源字段和召回模式并使测试通过。
    - **Surface**: query / sources / filter / scorer / proto mapping
    - **Proof**: 冷启动定向测试通过
    - **Depends on**: 前一项
- **Exit proof**: 显式话题严格召回；冷启动执行上游限定规则；补充话题保留普通召回。
- **Stop condition**: 现有公开协议无法保持冷启动字段语义。

### Phase 2: 收窄补充话题读取边界
- **Purpose**: 让外部 Adapter 决定用户资格和话题选择，Home Mixer 只消费结果。
- **Entry condition**: Phase 1 通过。
- **Phase rules**:
  - Home Mixer 不拥有关注关系、兴趣推断、用户年龄或关注/推断优先级。
  - Reader 直接返回已选的补充话题 ID；Home Mixer 只执行排除和去重。
  - 不扩展职责已经混杂的 `StratoClient`。
- **Todos**:
  - [x] 先测试 Adapter 选定话题、排除去重、已有话题来源跳过和读取失败降级。
    - **Surface**: UserTopicReader / Query Hydrator
    - **Proof**: RED 后 GREEN
    - **Depends on**: Phase 1
  - [x] 将 Hydrator 只随显式 Topic 依赖装入流水线。
    - **Surface**: pipeline wiring
    - **Proof**: Home Mixer 测试通过
    - **Depends on**: 前一项
- **Exit proof**: Home Mixer 不再编码关注/推断产品策略；失败时查询保持无补充话题状态。
- **Stop condition**: Adapter 无法在边界外完成资格和话题选择。

### Phase 3: 生产接入边界与回归验证
- **Purpose**: 让真实实现可通过装配注入，并验证整体不回归。
- **Entry condition**: Phase 2 通过。
- **Phase rules**:
  - 未配置生产实现时明确禁用，不伪装为成功。
  - 不虚构远端数据合同。
- **Todos**:
  - [x] 补齐 Demo/禁用实现和装配配置。
    - **Surface**: clients / pipeline wiring
    - **Proof**: 单元测试和编译通过
    - **Depends on**: Phase 2
  - [x] 运行格式化、Clippy 和 crate 测试。
    - **Surface**: workspace verification
    - **Proof**: 命令输出
    - **Depends on**: 前一项
- **Exit proof**: 本地 Demo 可运行，生产未配置时安全降级，所有验证通过。
- **Stop condition**: 本次改动与工作区已有改动发生不可合并冲突。

## Dry-Run Findings
- 不能把 `new_user_topic_ids` 政名后解释成补充召回；wire 兼容不代表行为兼容。
- 关注、推断、年龄和资格策略缺少本地合同，应由外部 Adapter 返回最终补充话题，而不是在 Home Mixer 猜测。
- Query Hydrator 失败时流水线会保留原查询，适合复用为补充话题读取降级路径。
- 真实生产 Adapter 依赖尚未提供的远端协议；本轮完成公开装配入口，但不虚构网络实现。

## 阶段完成时证据（历史快照）

以下数字只描述 P3 个性化话题改造完成当时的工作树，不作为当前累计结果：

- `cargo test --workspace`：13 个套件、106 项通过。
- `cargo test -p home-mixer`：91 项通过。
- `cargo test -p xai_candidate_pipeline`：12 项通过。
- `cargo clippy -p home-mixer --all-targets`：0 error；18 条 warning 均为当时已有代码或并行改动中的存量问题。
- 本次触及的 Rust 文件通过独立 `rustfmt --check`，`git diff --check` 通过。
- 当时 `cargo fmt --all -- --check` 会被 `thunder/kafka/tweet_events_listener.rs` 中既有的 Rust 2024 let-chain 阻塞；该文件不属于该阶段改动。

## 当前回归证据（2026-08-15）

- `cargo test --workspace`：220 项通过。
- `cargo test -p home-mixer --all-targets`：185 项通过。
- `cargo test -p xai_candidate_pipeline`：21 项通过。
- `cargo test -p home-mixer --test p4_final_feed`：25 项通过。
- `cargo clippy -p home-mixer --all-targets -- -W clippy::all`：0 error，0 warning。
- `cargo clippy --workspace --all-targets`：0 error，1 warning；唯一告警位于上游逐字节保留的 `thunder/strato_client.rs`（`new_without_default`）。
- 默认 ScoredPosts Demo：35 条（4 网内 + 31 网外）。
- ForYou Demo：35 条（4 网内 + 31 网外）。
- 显式话题 Demo：35 条 `Phoenix 话题`（0 网内 + 35 网外）。
- 显式缓存 Demo：8 条 `请求缓存`（0 网内 + 8 网外）。

## Final Validation
- `cargo test --workspace`
- `cargo clippy -p home-mixer --all-targets`
- 本次触及文件的 `rustfmt --edition 2021 --check`
- `git diff --check`

## First Execution Step
为严格话题页和首页混合话题分别添加 Source 启用规则的失败测试。
