# x-algorithm 文档总入口

按你的目的选择入口，读文档时先看开头的状态标签，避免把设计规划当成当前代码事实。

## 我想把系统跑起来

直接进 **[getting-started/](./getting-started/)**：从装环境到端到端跑通完整推荐链路，循序渐进六步，每条命令都验证过。

## 我想理解当前代码

按这个顺序读模块文档（全部以当前仓库代码为事实基准）：

1. [home-mixer/](./home-mixer/)：Feed 编排层——一次请求如何经过召回、补全、过滤、打分、选择。
2. [candidate-pipeline/](./candidate-pipeline/)：流水线框架——各阶段的执行顺序、并发与容错语义。
3. [thunder/](./thunder/)：网内实时帖子缓存——Kafka 摄入、内存索引、查询服务。
4. [phoenix/](./phoenix/)：Python/JAX 模型——精排、双塔召回、服务化与训练。

## 我想继续建设生产链路

1. [getting-started/06-从演示到真实系统](./getting-started/06-从演示到真实系统.md)：缺口清单和推进顺序（先读这个）。
2. [training/](./training/)：训练数据规格与离线链路的目标设计。
3. [operations/](./operations/)：数据持续更新、索引切版、发布运维建议。
4. [research/](./research/)：双塔、冷启动等算法背景调研。

## 文档状态标签

| 状态 | 含义 |
| --- | --- |
| `current-code` | 以当前仓库代码为事实基准，可用于定位实现行为。 |
| `design` | 设计建议或目标形态，不代表已经完整落地。 |
| `runbook` | 操作手册，包含生产化假设，需要结合真实环境校准。 |
| `historical` | 历史过程记录，可能落后于当前代码，仅供考古。 |
| `research` | 外部资料整理或算法背景，不能直接推导当前仓库行为。 |

## 目录总览

| 目录 | 状态 | 说明 |
| --- | --- | --- |
| [getting-started/](./getting-started/) | `current-code` | 从零跑通主线（推荐入口）。 |
| [home-mixer/](./home-mixer/) | `current-code` | 首页 Feed 编排服务、请求生命周期、组件和字段字典。 |
| [candidate-pipeline/](./candidate-pipeline/) | `current-code` | 通用候选流框架及 `PhoenixCandidatePipeline` 当前装配。 |
| [thunder/](./thunder/) | `current-code` | Kafka 摄入、内存索引、gRPC 查询和运维缺口。 |
| [phoenix/](./phoenix/) | `current-code` | 精排、召回、服务封装的代码导读（操作类内容以 `phoenix/docs/` 为准）。 |
| [training/](./training/) | `design` | 数据准备、训练样本和模型产物约束。 |
| [operations/](./operations/) | `runbook` | 持续更新、切版、故障处理和发布检查。 |
| [upstream-sync/](./upstream-sync/) | `design` / `current-code` | 上游能力同步（P3/P3b/P6 个性化话题与 MoE 召回演进记录）。 |
| [research/](./research/) | `research` | 双塔、冷启动等背景资料。 |
| [archive/](./archive/) | `historical` | 迁移记录、早期缺失盘点、依赖分析快照。 |

Phoenix 子项目内还有一套操作向文档（训练指引、真实数据接入等），入口在 [../phoenix/docs/](../phoenix/docs/)；两边的分工是：`docs/phoenix/` 讲代码怎么实现，`phoenix/docs/` 讲手上怎么操作。

最新上游同步结果见 [`update/20260814.md`](./update/20260814.md)：记录 `c65aa17` 的 Cold Start、Phoenix 输入修复、明确不迁移项和 U3 重入条件。

上游同步以 [upstream-first maintenance policy](./upstream-sync/upstream-first-maintenance.md) 为维护规则，以 [entrypoint migration map](./upstream-sync/entrypoint-migration-map.md) 为执行顺序，以最新 snapshot report 和 [e414c17 capability inventory](./upstream-sync/e414c17-to-mp-capability-inventory.md) 为能力状态和验收证据入口。

## 维护原则

- 写当前实现时，优先链接到具体代码文件和模块专题文档。
- 写未来设计时，在开头标明 `design` 或 `runbook`，不要混入当前事实文档。
- 操作命令只写在一个地方（getting-started 或 phoenix/docs），其他文档用链接引用，避免命令漂移。
- 新增文档时同步更新本文件和对应子目录的 `README.md`。
- 引用仓库内文件统一使用相对路径，不使用本机绝对路径。
