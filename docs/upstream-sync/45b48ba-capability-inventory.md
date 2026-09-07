# 提交 d011592–45b48ba 能力清点与 U0–U3 分类（2026-08-24 至 2026-08-26 上游快照）

> 文档状态：清点完成，当前有业务落点的能力已落地
> 上游范围：`28e414f535e4b5a50ca12ee87674e7649e50c7ad..45b48ba6baa40e212f6dcbaf8fe9fdc8d9da722e`
> 上游提交：`d011592`、`0d3cdd8`、`45b48ba`
> 此前已吸收锚点：`28e414f535e4b5a50ca12ee87674e7649e50c7ad`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)
> 迁移结果：[`../update/20260826.md`](../update/20260826.md)

## 1. 范围概览

三次上游提交共涉及 89 个文件，+8078/-1165。改动可归为七组：

1. Home Mixer 社交关系、排序、广告安全、选举合规与响应装配。
2. Phoenix proto、模型服务、checkpoint 读写和训练配置。
3. visibility-filtering 参照比对与 dark traffic 装配。
4. abuse-enforcement-service 通用处罚动作和规则执行。
5. phoenix-rankall 存储与集成测试。
6. grox PTOS/reply-spam 分类与写入流程。
7. 少量通用配置和观测性优化。

本地当前业务目标是 Home Mixer 业务 Feed/MVP 链路和可独立运行的 Phoenix 演示/服务链。迁移判断以“当前业务是否能执行并验证”为准，不以改动行数或是否容易复制为准。

## 2. 已落地能力

| 编号 | 能力 | 分类 | 本地处置 |
|---|---|---|---|
| H1 | `FollowingBlockedByHydrator`：检查转推原作者、引用作者是否反向屏蔽 viewer | **U1 + U2** | 没有复制第二个 Hydrator；扩展现有 `BlockedByHydrator`，一次批量查询普通作者、转推原作者和引用作者，去重 ID 后分别写入 `author_blocks_viewer`/`quoted_author_blocks_viewer`。外部 SocialGraph 继续由本地端口替代（U1），单 Hydrator 合并是减少重复 RPC 的本地加法（U2）。提交 `b89c610` |
| P1 | `PredictNextActionsRequest.conv_asset_ids=18` 与 `ConvAssetIds`；ActionName 191–193；`SlateContext.reconCosMilli=13` | **U0** | Rust/Python 两份 proto 同步采用；只保持合同兼容，不接入广告训练或 SlateContext 排序行为。提交 `6c9d7bd` |

### 2.1 H1 行为边界

- 普通作者或转推原作者反向屏蔽 viewer 时，`author_blocks_viewer=Some(true)`。
- 引用作者反向屏蔽 viewer 时，`quoted_author_blocks_viewer=Some(true)`。
- 同一用户出现在多个候选或多个角色中时只进入一次批量查询。
- SocialGraph 失败时保持候选基数且关系字段为中立值，不把依赖失败误判为屏蔽。
- 组件仍未进入默认装配；真实 Adapter 通过认证、超时、批量上限和错误语义验收后再显式注入。

## 3. 延期能力与重入条件

| 编号 | 能力 | 分类 | 不立即落地的业务原因与重入条件 |
|---|---|---|---|
| C1 | `load_checkpoint_streamed`、读计划/批量预取/host staging、restore `_NodeBatchLock` | N/A（当前运行目标） | 当前受支持的本地训练和演示入口没有大 checkpoint、多机恢复或加载 OOM 证据。重入条件：记录到 checkpoint 加载峰值内存、启动/热切换耗时或多机恢复成为验收目标；先用真实产物做基准，再迁移 `45b48ba` 终态，不迁中间态 |
| C2 | save 侧 `ThrottledD2HArrayHandler`、节点批锁、并发字节限制和内存释放 | N/A（当前运行目标） | 同 C1；需有设备到主机复制成为瓶颈或多节点保存并发需求的证据 |
| E1 | admission controller `reset_estimates` | **U3** | 当前缺少会在模型 reload/hotswap 后调用它的完整服务装配。重入条件：模型切换流程需要清除旧模型耗时估计时，连调用方和回归测试一起迁移，不先放置孤立 API |
| E2 | copy-port dense/embedding 下载吞吐日志 | N/A（当前故障面） | 当前没有 copy-port 加载慢的故障证据；随 C1/C2 或模型加载性能排查一起引入 |
| S1 | `served_slate_contexts`、recon count/gap、ranking scorer 与 VM ranker 的权重/DPP 重构 | **U3** | 依附既有 T3 served SlateContext 回路。重入条件：生产、传输、消费、排序四段合同完整，并可对排序结果做回归对照 |
| H2 | `view_count_on_home`、engagement counts hydrator、following night-owl source | **U3** | 本地没有对应 engagement-count 数据端口和 source。重入条件：明确数据来源、缺失语义和 cold-start 使用方后整体迁移，不能只增加无人生产的字段 |
| G1 | PTOS `SafetyPtosPolicyCrossValidator`、reply-spam 模型和写入流程 | **U3** | P6-B grox 线路；依赖 EAPI 模型、Prompt、策略和 Sink 合同。合同齐全且进入当前产品目标后重入 |
| A1 | Home Mixer ads brand-safety VF 调整 | **U3** | 本地没有对应 VF 服务和广告安全数据合同；不能用看似可运行的 stub 代替真实安全判定 |
| O1 | `xai-configlib.resolve_type_hints` 缓存 | N/A（当前故障面） | 纯内部优化，当前没有配置解析热点证据；profiling 证明为热点时再采用，避免为无业务影响的小优化制造同步噪音 |

## 4. 不适用能力

| 编号 | 能力 | 分类 | 原因 |
|---|---|---|---|
| V1 | visibility-filtering `reference_compare.rs`、dark traffic、TES hydrator/server deps | N/A | 本地已剥离 visibility-filtering。staging 双调用对照方法保留为迁移验证思路，不新增无执行入口的框架 |
| B1 | Brazil 2026 election filter 名单扩充 | N/A | 延续既有产品决定：本地不迁移该国家/选举专项规则 |
| AE1 | abuse-enforcement-service facts/rules/generic actions/GrowthBook | N/A | 本地无该服务；处罚执行属于独立业务边界，不放进 Feed 推荐域 |
| R1 | phoenix-rankall store、配置与集成测试 | **U3** | 完成评估：**不引入**，理由见 §4.1 |
| U1 | `util/urt`、reverse-chron、night-owl 等无本地入口的装配改动 | N/A | 当前仓库没有对应响应/数据入口；只迁一段会形成无人调用代码 |

### 4.1 R1 phoenix-rankall 的 Go / No-Go（2026-09-07 补评）

原先"本地无该 crate 和调用入口"是循环论证——本地没有正是因为没迁，说明不了该不该迁。按 P3.3（visibility-filtering）同一口径重评：

**这是什么业务能力。** 离线全量排序物料链：消费帖子创建、互动（fav/indexing/metadata）Kafka 事件，经 `sid_processor` / `topic_processor` / `metadata_processor` 落进 SID store 与 parquet，供全库候选（而非召回后候选）打分使用。它不在请求链路上，主干只消费其产物。

**能不能迁。** 不能，且比 visibility-filtering 更硬：

| 阻塞项 | 事实 |
|---|---|
| 无构建清单 | 上游 `phoenix-rankall/` 共 35 个文件，**没有 `Cargo.toml`**。上游并非不发清单——`phoenix/` 下 7 个 crate 和 `bdsm/rust/` 下 2 个都带清单，共 10 份。这里的缺失是有意义的：它不是一个可构建的开源 crate。 |
| 核心领域类型未开源 | `xai_recsys_rankall` 被引用 27 次，承载全部记录/评分结构，上游树中不存在。 |
| 基础设施依赖未开源 | `xai_wily`、`xai_kafka` 均无 Rust 实现（只有 `grox/libs/wily_cli` 的 Python 版）。 |
| 数据面绑定内部部署 | 事件流与 store 定义在 `phoenix-rankall-strato/`，Strato 是 X 内部部署环境，本地无对应设施（口径同 [`b089ce6-capability-inventory.md`](./b089ce6-capability-inventory.md) B2）。 |

迁进来只能得到一层调不通的 processor 骨架：thrift 类型要自己发明，Kafka topic 与 SID store 合同要自己发明，正是 `U3` 禁止的"看起来能跑的桩"。

**结论：No-Go（U3）。** 重入条件是两件事同时成立：(1) 产品目标出现"全库候选全量排序"而不是现有的"召回后候选打分"；(2) `xai_recsys_rankall` 记录结构、Kafka 事件与 SID store 三段数据合同可独立验证。当前本地离线训练数据走 parquet 生成器，不消费该流，两条都不成立。

## 5. 设计说明

H1 没有照搬上游两个职责重叠的 Hydrator。对业务来说，目标是“不要推荐任何由屏蔽 viewer 的相关作者产生的候选”；普通作者、转推原作者和引用作者都属于同一条候选社交关系补全职责。一次收集、一次 RPC、一次更新比两个组件串行查询更容易解释、测试和运维。

领域模型只保存关系判定结果；RPC、认证、超时和批量限制仍留在 `SocialGraphClientOps` Adapter 中。Filter 只消费结果，不直接访问外部服务，保持业务判断和基础设施分离。

## 6. 验证

- `cargo test -p home-mixer`：198 通过。
- 根 workspace `cargo test --workspace`：233 通过。
- `cd phoenix && cargo test -p xai-recsys-proto`：5 通过。
- `cd phoenix && PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`：118 通过、3 ignored。
- `cd phoenix && uv run pytest`：92 通过。
- Python proto 重新生成并验证 `ConvAssetIds`、`reconCosMilli`、`ADS_PIXEL_FIRE=191`。
- Rust/Python 两份 `recsys.proto` 字节一致；`cargo fmt --all -- --check` 与 `git diff --check` 通过。

## 7. 锚点结论

范围内每项能力均已有“落地、延期或不适用”结论，延期项写明业务证据和重入条件。因此同步锚点可前移至 `45b48ba`；这不表示启用了广告、SlateContext、PTOS、VF 或多机 checkpoint 能力。
