# candidate-pipeline 文档索引

这组文档基于当前仓库中的 `candidate-pipeline/` 框架 crate 与 `home-mixer/` 里的 `PhoenixCandidatePipeline` 实际装配整理，目标不是解释“推荐系统通常怎么做”，而是把这份代码现在真实在做什么、缺什么、扩展时会踩什么坑说清楚。

结论先行：

- `candidate-pipeline` 是一个通用的候选流编排框架，负责阶段顺序、并发策略、结果合并和容错。
- 业务策略不在框架里，而是在 `home-mixer/candidate_pipeline/phoenix_candidate_pipeline.rs` 中通过组件列表装配出来。
- Thunder 是真 gRPC；Phoenix 配了地址后也是真 gRPC。非 demo 下 Strato / TES / VF 走 mrpyq 适配器，UAS 走 Redis 投影（`uas-worker` 写入），Gizmoduck 仍是 Disabled；demo 下全部是 `Demo*` 实现。框架完整，默认业务链路在 `HOME_MIXER_MODE=demo` 下可跑通，不是纯 stub。

## 推荐阅读顺序

1. [01-framework-overview.md](./01-framework-overview.md)
2. [02-execution-semantics.md](./02-execution-semantics.md)
3. [03-phoenix-pipeline-current-state.md](./03-phoenix-pipeline-current-state.md)
4. [04-component-reference.md](./04-component-reference.md)
5. [05-extension-guide.md](./05-extension-guide.md)
6. [06-risks-tests-and-roadmap.md](./06-risks-tests-and-roadmap.md)

## 文档覆盖范围

- `candidate-pipeline/` 的核心 trait、执行入口、并发和容错语义
- `home-mixer/` 当前这条候选流的请求结构、候选结构、组件装配顺序和外部依赖
- 当前代码的真实运行约束、默认行为、测试覆盖缺口和改进方向

## 不覆盖的内容

- `phoenix/` 模型训练细节和 JAX 实现细节
- `thunder/` 内部缓存实现细节
- Proto 定义的全量字段解释

## 适用对象

- 需要快速理解这套推荐编排框架的人
- 需要在现有流水线上新增 Source / Hydrator / Filter / Scorer 的人
- 需要判断当前仓库距离“可运行生产版”还有哪些缺口的人
