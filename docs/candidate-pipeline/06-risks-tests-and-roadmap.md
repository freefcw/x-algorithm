# 风险、测试缺口与改进路线

本篇不重复讲组件职责，只聚焦当前实现里最值得优先关注的问题。

## 1. 高优先级问题

### 1.1 同 stage Hydrator 的依赖关系当前不成立

问题来源：

- 框架的 `run_hydrators()` 会把同一个 stage 的所有 hydrator 基于同一份旧候选快照并发执行
- `GizmoduckCandidateHydrator` 读取 `candidate.retweeted_user_id`
- `retweeted_user_id` 又是 `CoreDataCandidateHydrator` 才会补出来的字段

结果：

- 即使 `CoreDataCandidateHydrator` 成功拿到了转推原作者
- `GizmoduckCandidateHydrator` 也看不到这次补全结果
- `retweeted_screen_name` 实际上很难在当前装配里被正确写入

这不是单个组件的小 bug，而是“装配顺序与框架语义不匹配”。

### 1.2 默认 stub 组合下，流水线几乎必然返回空结果

关键链路如下：

1. UAS fetcher 返回空序列
2. `UserActionSeqQueryHydrator` 失败，`user_action_sequence` 为空
3. `PhoenixSource` 因缺少 `user_action_sequence` 失效
4. `StratoClient` 返回空用户特征
5. `TESClient` 返回空 core data
6. `CoreDataHydrationFilter` 过滤掉所有 `tweet_text` 为空的候选

结果：

- 不是“排序效果不好”
- 而是默认装配下几乎没有候选能穿过整条链路

如果目标是让 demo 先跑出非空结果，优先级应该是补齐外部依赖或临时放宽过滤条件，而不是继续增加 scorer。

### 1.3 `WeightedScorer` 的负分 offset 公式与注释不一致

`params.rs` 注释声称负分会被映射到一个非负区间，但当前代码：

```rust
(combined_score + p::NEGATIVE_WEIGHTS_SUM) / p::WEIGHTS_SUM * p::NEGATIVE_SCORES_OFFSET
```

在 `NEGATIVE_WEIGHTS_SUM = -591.0` 时会把负分推得更负，而不是映射到 `[0, 1]`。

这意味着：

- 当前实现语义和文档注释不一致
- 排序结果可能与预期相反
- 如果后续有人按注释调参数，会得到错误直觉

### 1.4 Post-selection 过滤后没有回填机制

当前流程是：

1. selector 先选 Top 100
2. `VFFilter` 和 `DedupConversationFilter` 再删
3. 直接 truncate 到 50

如果 post-selection 阶段删掉很多候选：

- 框架不会从 selector 之前的“第 101 名以后”回填
- 最终返回条数可能远小于 50

这在有严格供给要求的首页 Feed 中会成为稳定性问题。

### 1.5 SideEffect 结果被完全丢弃

`run_side_effects()` 的当前行为是：

- `tokio::spawn`
- `join_all`
- 结果赋给 `_`

这意味着：

- SideEffect 失败不会影响主链路，这本身没问题
- 但默认也没有统一日志或 metrics
- 如果缓存写回持续失败，框架层不会给出明显信号

## 2. 中优先级问题

### 2.1 Selector 不在统一 stage 观测模型里

`PipelineStage` 没有 `Selector` 和 `SideEffect`。后果是：

- 阶段日志不完整
- 无法在统一 stage 维度下统计“selector 前后规模变化”
- 排查“为什么只剩 17 条结果”时要跨业务代码找日志

### 2.2 `PipelineResult.filtered_candidates` 缺乏来源信息

当前 `filtered_candidates` 只是一个合并列表，不记录：

- 是哪一个 filter 移除的
- 是 pre-selection 还是 post-selection 阶段移除的

这会降低调试效率，也不利于离线分析各个 filter 的影响。

### 2.3 `normalize_score()` 仍是 stub

当前 `WeightedScorer` 调了 `normalize_score()`，但实现只是原样返回。后果是：

- `author_followers_count` 目前没有进入归一化逻辑
- 新鲜度衰减也没做
- 这让 Gizmoduck 补的粉丝数还没有真正进入排序闭环

## 3. 测试覆盖缺口

当前与 candidate-pipeline 相关的测试覆盖非常薄：

- 框架 crate 里只有 `short_type_name()` 的单测
- `home-mixer` 里只有少量工具函数和个别 filter/scorer 的单测
- 没有针对 `CandidatePipeline::execute()` 主路径的集成测试

最值得补的测试不是更多工具函数测试，而是以下几类：

### 3.1 框架级行为测试

- `Hydrator` 长度不匹配时会被跳过
- `Filter` 失败时会回滚
- `Source` 失败时不会影响其他 source
- `post_selection_filters` 删除候选后不会回填

### 3.2 装配级集成测试

- 给 `PhoenixCandidatePipeline` 注入 fake clients，验证完整结果规模
- 验证 `PreviouslySeenPostsFilter` / `PreviouslyServedPostsFilter` 对 related post ids 的语义
- 验证 `VFCandidateHydrator` 按 `in_network` 分 safety level

### 3.3 排序语义测试

- `WeightedScorer` 负分公式是否符合预期
- `AuthorDiversityScorer` 在不同作者分布下的衰减行为
- `OONScorer` 只影响网外内容

## 4. 建议的改造路线

如果目标是把这条链路从“骨架”推进到“可用”，建议按下面顺序处理。

### 第一阶段：先让链路稳定产出非空结果

1. 替换或 mock `TESClient`，保证能拿到 `tweet_text`
2. 替换或 mock `StratoClient`，保证能拿到基础用户特征
3. 替换或 mock `UserActionSequenceFetcher`，让 `PhoenixSource` 和 `PhoenixScorer` 可以拿到序列
4. 为 `PhoenixCandidatePipeline` 写一条完整集成测试

### 第二阶段：修语义错误和观测缺口

1. 修正 `WeightedScorer::offset_score()` 的负分公式，或者同步修改注释和参数定义
2. 给 side effect 增加错误日志和 metrics
3. 给 selector 增加统一观测点
4. 为 `filtered_candidates` 增加过滤来源信息

### 第三阶段：解决框架结构限制

1. 处理 hydrator 之间的依赖问题
2. 评估是否需要把 `Hydrator` 拆成多个有顺序的子阶段
3. 评估 post-selection 删除后的回填机制

## 5. 一个更务实的判断标准

判断这条流水线是不是“已经可用”，不要只看：

- 是否能编译
- 是否有完整的阶段列表

更应该看下面四个问题：

1. 能否稳定返回非空结果？
2. 关键外部依赖是否已摆脱 stub？
3. 排序分数是否与参数注释和产品预期一致？
4. 出现候选被清空时，能否在日志和 metrics 中快速定位是哪个阶段导致？

按这个标准看，当前仓库已经具备很好的框架骨架，但距离真正可运营的候选流系统仍有明确的工程补齐工作要做。
