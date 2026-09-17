# x-algorithm 文档总入口

按你的目的选择入口，读文档时先看开头的状态标签，避免把设计规划当成当前代码事实。

## 我想把系统跑起来

- **[bootstrap/](./bootstrap/)**：从本地环境、模块编译、模型训练、端到端 Demo，到真实数据接入、能力屏蔽和生产验收的完整启动手册。
- **[getting-started/](./getting-started/)**：更短的当前代码快速上手教程，适合先用最少步骤跑通 Demo。

## 我想理解当前代码

按这个顺序读模块文档（全部以当前仓库代码为事实基准）：

1. [home-mixer/](./home-mixer/)：Feed 编排层——一次请求如何经过召回、补全、过滤、打分、选择。
2. [candidate-pipeline/](./candidate-pipeline/)：流水线框架——各阶段的执行顺序、并发与容错语义。
3. [thunder/](./thunder/)：网内实时帖子缓存——Kafka 摄入、内存索引、查询服务。
4. [phoenix/](./phoenix/)：模型层——演示用 Python/JAX 链路，以及 `xrex/`/`crates/` 生产引擎导读。
5. 旁路（默认不进演示）：根目录 `vm-ranker/`（二次重排）、`grox/`（独立任务 DAG，见 [../grox/README.md](../grox/README.md)）。

## 我想继续建设生产链路

1. [以 PhoenixCandidatePipeline 为主干的推荐链路收敛方案](./implementation/phoenix-pipeline-trunk-plan.md)（`decision`）：已定方向的主干收敛决策——以 `mp` 的 `PhoenixCandidatePipeline` 为唯一编排主干、吸收 `mp-slim` 的契约层成果、ID 方案选 Copy newtype `ObjectId`、新增 U4 / U5 差异类，以及 P0–P3 实施批次与执行记录。要接业务、改 ID 类型或删组件之前先读它。
2. [推荐服务外部依赖梳理与 MVP 建设建议](./recommendation-service-mvp-assessment.md)：从业务角度判断首版保留、延后和不复制的能力，适合立项与范围决策时先读。
3. [推荐链路正式上线就绪度评估](./recommendation-production-readiness-assessment.md)：x-algorithm × rec-bff × mrpyq 三仓库快照评估——阻碍执行的问题、数据/基建推进工作流、性能优化建议、Phoenix 准备清单和分里程碑检查单。做上线排期前先读它（注意其快照日期）。
4. [getting-started/06-从演示到真实系统](./getting-started/06-从演示到真实系统.md)：当前演示链路的真实数据缺口和推进顺序。
5. 对 mrpyq / rec-bff 的接口约定（`proposed`）：[皮维度对齐要求](./implementation/mrpyq-member-dimension-requirements.md)（§2 召回已由 rec-bff 满足，剩余项及三方同批上线约束见文内）与 [UAS 行为事件流合同](./implementation/uas-event-contract.md)。要给 mrpyq / rec-bff 排期时直接交付这两份。
6. [training/](./training/)：训练数据规格与离线链路的目标设计。
7. [operations/](./operations/)：数据持续更新、索引切版、发布运维建议。
8. [research/](./research/)：双塔、冷启动等算法背景调研。

## 文档状态标签

| 状态 | 含义 |
| --- | --- |
| `current-code` | 以当前仓库代码为事实基准，可用于定位实现行为。 |
| `design` | 设计建议或目标形态，不代表已经完整落地。 |
| `decision` | 已定方向的决策记录：结论、依据与执行边界已确定，正按批次落地；文末的执行记录说明哪些批次已完成。 |
| `runbook` | 操作手册，包含生产化假设，需要结合真实环境校准。 |
| `historical` | 历史过程记录，可能落后于当前代码，仅供考古。 |
| `research` | 外部资料整理或算法背景，不能直接推导当前仓库行为。 |

## 目录总览

| 目录 | 状态 | 说明 |
| --- | --- | --- |
| [bootstrap/](./bootstrap/) | `runbook` / `current-code` | 从环境、编译、模型、Demo 到真实数据和生产验收的完整启动手册。 |
| [getting-started/](./getting-started/) | `current-code` | 从零跑通主线（推荐入口）。 |
| [recommendation-service-mvp-assessment.md](./recommendation-service-mvp-assessment.md) | `design` | 面向业务决策的外部依赖评估、MVP 边界和三阶段演进建议。 |
| [recommendation-production-readiness-assessment.md](./recommendation-production-readiness-assessment.md) | `design` | 三仓库上线就绪度快照评估：阻碍项、推进工作流、性能与 Phoenix 准备清单、里程碑检查单。 |
| [implementation/user-action-collect.md](./implementation/user-action-collect.md) | `current-code` | UAS 客户端行为上报定义（取代 `uas-client-event-reporting.md`，旧文已成指向横幅）：上报责任划分、事件格式、动作与入口编码、投递语义、消费端行为与联调验收。 |
| [implementation/phoenix-pipeline-trunk-plan.md](./implementation/phoenix-pipeline-trunk-plan.md) | `decision` | 以 `PhoenixCandidatePipeline` 为主干的推荐链路收敛方案：分支取舍、U4 / U5 差异类、ObjectId 方案、组件取舍、P0–P3 实施批次与执行记录。 |
| [implementation/mrpyq-member-dimension-requirements.md](./implementation/mrpyq-member-dimension-requirements.md) | `proposed` | 皮维度接口要求：§2 召回已由 rec-bff 满足；剩余字段改名、`creator_member_id` 必填、viewer 关系换皮、mrpyq 底层改造与三方同批上线约束。 |
| [implementation/uas-event-contract.md](./implementation/uas-event-contract.md) | `proposed` | 对 mrpyq 的 UAS 行为事件流合同：Kafka 消息格式、`action_type` 映射与分批、身份空间、投递语义、联调验收。 |
| [home-mixer/](./home-mixer/) | `current-code` | 首页 Feed 编排服务、请求生命周期、组件和字段字典。 |
| [candidate-pipeline/](./candidate-pipeline/) | `current-code` | 通用候选流框架及 `PhoenixCandidatePipeline` 当前装配。 |
| [thunder/](./thunder/) | `current-code` | Kafka 摄入、内存索引、gRPC 查询和运维缺口。 |
| [phoenix/](./phoenix/) | `current-code` | 精排、召回、服务封装的代码导读（操作类内容以 `phoenix/docs/` 为准）。演示链与生产引擎两套代码都在 `phoenix/`。 |
| [`../vm-ranker/`](../vm-ranker/) | `current-code` | 可选 VM Ranker + DPP 重排服务，默认关闭。 |
| [`../grox/README.md`](../grox/README.md) | `current-code` | 独立 Grox DAG 运行时，不进主推荐链。 |
| [training/](./training/) | `design` | 数据准备、训练样本和模型产物约束。 |
| [operations/](./operations/) | `runbook` | 持续更新、切版、故障处理和发布检查。 |
| [upstream-sync/](./upstream-sync/) | `design` / `current-code` | 上游能力同步（P3/P3b/P6 个性化话题与 MoE 召回演进记录）。 |
| [research/](./research/) | `research` | 双塔、冷启动等背景资料。 |
| [archive/](./archive/) | `historical` | 迁移记录、早期缺失盘点、依赖分析快照。 |

Phoenix 子项目内还有一套操作向文档（训练指引、真实数据接入等），入口在 [../phoenix/docs/](../phoenix/docs/)；两边的分工是：`docs/phoenix/` 讲代码怎么实现，`phoenix/docs/` 讲手上怎么操作。

最新上游同步结果见 [`update/20260907.md`](./update/20260907.md)：记录 `9b0dc31`–`902a06f` 的两项行为中性 Phoenix 对齐（SID 快照路径构造、结构化日志渲染），以及 MoE 共分流实验、StableHLO bundle 导出、加密 checkpoint 等线路的延期或不适用结论。能力清点入口是 [902a06f capability inventory](./upstream-sync/902a06f-capability-inventory.md)。

上游同步以 [upstream-first maintenance policy](./upstream-sync/upstream-first-maintenance.md) 为维护规则，以 [entrypoint migration map](./upstream-sync/entrypoint-migration-map.md) 为执行顺序。

## 维护原则

- 写当前实现时，优先链接到具体代码文件和模块专题文档。
- 写未来设计时，在开头标明 `design` 或 `runbook`，不要混入当前事实文档。
- 操作命令只写在一个地方（getting-started 或 phoenix/docs），其他文档用链接引用，避免命令漂移。
- 新增文档时同步更新本文件和对应子目录的 `README.md`。
- 引用仓库内文件统一使用相对路径，不使用本机绝对路径。
- 代码行为变化后更新文档时，同一文档内的结论、缺口清单和示意图必须与篇首的更新说明一并重写，避免出现"开头说已实现、文末说未实现"的自相矛盾。
