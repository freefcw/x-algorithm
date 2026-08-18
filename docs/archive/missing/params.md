# Params 模块缺失记录

状态：`historical`

本文是早期迁移过程中对 `home-mixer/params` 缺失影响的说明。当前仓库已经在 `home-mixer/params/` 中重建了一组开源可运行的参数常量，因此本文只作为历史背景，不作为当前代码事实。

## Params 的作用

在 X 的推荐系统中，Params 模块通常承担算法参数中心的角色：

1. 存储评分权重，例如点赞、转推、回复、点击等行为各自占多少分。
2. 定义过滤阈值，例如内容年龄、候选数量和输出数量。
3. 控制召回规模，用于平衡性能与准确性。
4. 保存部分系统运行参数，例如 gRPC 消息大小、候选截断上限等。

这些参数会直接影响排序、过滤和候选规模。原始私有实现中的真实参数无法从开源仓库恢复，因此迁移时必须用可解释的开源默认值替代。

## 当前仓库的处理方式

当前仓库不再依赖缺失的私有 Params 模块，而是在 `home-mixer/params/` 中提供本地常量。相关解释请优先看：

- [../home-mixer/07-config-and-params.md](../../home-mixer/07-config-and-params.md)
- [../home-mixer/06-current-behavior-risks-roadmap.md](../../home-mixer/06-current-behavior-risks-roadmap.md)

## 保留价值

本文保留的价值是提醒读者：推荐系统的可运行骨架和生产效果之间，还隔着大量参数校准、训练数据和外部依赖接入工作。不要把开源默认参数等同于 X 线上真实参数。
