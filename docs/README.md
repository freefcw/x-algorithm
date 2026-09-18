# x-algorithm 文档入口

先决定你要完成什么，再选一组文档。当前 checkout 已移除旧的单机 JAX Demo，因此不要把历史上手章节当作可执行的完整 Demo；生产引擎入口以 `phoenix/README.md` 为准。

## 先走一条主线

| 目标 | 入口 | 结果 |
| --- | --- | --- |
| 了解原本地 Demo 路径 | [从零跑起来](./getting-started/) | 查看保留的上手章节和当前失效边界 |
| 理解一次请求怎么走 | [Home Mixer 手册](./home-mixer/00-handbook.md) | 理解召回、补全、过滤、打分和选择 |
| 理解模型和服务 | [Phoenix 导读](./phoenix/) | 理解精排、召回、训练和服务边界 |
| 接入真实数据或规划上线 | [完整启动与建设手册](./bootstrap/) | 明确依赖、阶段、验收和故障处理 |
| 改流水线组件 | [Candidate Pipeline](./candidate-pipeline/) | 理解 trait、执行语义和扩展约束 |
| 查协议或业务接口 | [实现与合同](./implementation/) | 查 UAS、曝光、数据面和迁移决策 |

## 文档按什么规则分工

| 目录 | 只回答什么问题 | 不要拿它做什么 |
| --- | --- | --- |
| `getting-started/` | 当前代码怎样最短跑通 | 不放生产方案和长篇原理 |
| `bootstrap/` | 从 Demo 到真实系统怎样推进 | 不作为日常代码索引 |
| `home-mixer/`、`phoenix/`、`thunder/`、`candidate-pipeline/` | 当前代码如何实现 | 不替代操作命令的唯一来源 |
| `implementation/` | 已定决策、接口合同和落地要求 | 不把提案写成已实现事实 |
| `training/`、`operations/` | 生产训练、数据和运维的目标形态 | 不暗示仓库已经提供全部外部系统 |
| `research/` | 算法背景和外部资料 | 不作为当前代码的事实来源 |
| `update/`、`upstream-sync/`、`archive/` | 变更过程和历史依据 | 不作为当前状态的首选入口 |

## 重要事实

- 当前 checkout 保留推荐系统的 Rust 编排代码和 Phoenix 生产引擎，但已移除旧的单机 JAX Demo、假模型和假语料；不包含 X 的生产依赖、业务数据或预训练权重。
- `phoenix/` 的生产入口以 [`phoenix/README.md`](../phoenix/README.md) 为准；`docs/phoenix/` 负责代码导读，当前 checkout 未保留 `phoenix/docs/` 操作手册。
- 命令只应维护在 [getting-started](./getting-started/) 或对应模块 README；其他文档只链接，不复制命令。
- 看到 `current-code`、`design`、`decision`、`runbook`、`historical`、`research` 标签时，先按标签判断可信范围。

## 当前代码专题

- [Home Mixer](./home-mixer/)：Feed 编排、请求生命周期、组件和字段。
- [Candidate Pipeline](./candidate-pipeline/)：通用流水线框架和 `PhoenixCandidatePipeline` 装配。
- [Thunder](./thunder/)：Kafka 摄入、内存索引和网内查询。
- [Phoenix](./phoenix/)：精排、召回、服务化和训练导读。
- [VM Ranker](../vm-ranker/README.md)：默认关闭的二次重排。
- [Grox](../grox/README.md)：不进入主推荐链的独立任务 DAG。

## 维护规则

1. 新增文档前先判断能否并入现有入口；同一条命令、字段或合同只保留一个事实源。
2. 当前实现、设计、决策和历史记录分开存放，并在标题附近标明状态与日期。
3. 代码行为变化时，优先更新对应模块专题和本页入口；不要新增一篇重复的“分析报告”。
4. 历史记录可以保留，但必须从主路径可达且明确标为历史。
