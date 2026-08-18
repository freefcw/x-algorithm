# 执行流程与语义

本篇只讨论 `candidate-pipeline/candidate_pipeline.rs` 里的真实执行行为，不讨论业务组件内部逻辑。

## 1. `execute()` 的逐步展开

`CandidatePipeline::execute(query)` 的固定流程如下：

| 步骤 | 调用 | 输入 | 输出 | 说明 |
| --- | --- | --- | --- | --- |
| 1 | `hydrate_query` | 原始 `Q` | hydrated `Q` | 并行执行所有启用的 `QueryHydrator` |
| 2 | `hydrate_dependent_query` | hydrated `Q` | hydrated `Q` | 并行执行 `dependent_query_hydrators()`（默认空；可读取第一段 hydrator 写入的 query 字段） |
| 3 | `fetch_candidates` | hydrated `Q` | `Vec<C>` | 并行执行所有启用的 `Source` 并拼接结果 |
| 4 | `hydrate` | hydrated `Q` + candidates | hydrated `Vec<C>` | 并行执行候选补全 |
| 5 | `filter` | hydrated `Q` + hydrated candidates | `(kept, removed)` | 串行执行 pre-selection filters |
| 6 | `score` | hydrated `Q` + kept | scored `Vec<C>` | 串行执行 scorers |
| 7 | `select` | hydrated `Q` + scored | selected `Vec<C>` | selector 排序和裁剪 |
| 8 | `hydrate_post_selection` | hydrated `Q` + selected | hydrated `Vec<C>` | 对已选中候选做后补全 |
| 9 | `filter_post_selection` | hydrated `Q` + post-hydrated | `(kept, removed)` | 串行执行 post-selection filters |
| 10 | `truncate(result_size)` | kept | final `Vec<C>` | 结果再次裁剪；不足 result_size 时输出 `result_underfilled` 告警 |
| 11 | `finalize` | query + final candidates | 无 | 公共扩展点，默认空实现 |
| 12 | `run_side_effects` | query + final candidates | 无 | fire-and-forget |
| 13 | 组装 `PipelineResult` | 各阶段中间结果 | `PipelineResult<Q, C>` | 返回给调用方 |

## 2. 并发与串行策略

### 2.1 并发阶段

以下阶段使用 `futures::future::join_all`：

- `hydrate_query`
- `fetch_candidates`
- `run_hydrators`
- `run_side_effects`

这意味着这些阶段里的每个组件都会同时启动异步任务，但是否真正并行取决于具体运行时和组件内部是否做了 IO。

### 2.2 串行阶段

以下阶段严格按组件列表顺序执行：

- `run_filters`
- `score`
- `select`

这几类组件天然存在顺序语义：

- filter 的前一个结果会直接影响后一个 filter 的输入
- scorer 的后一个结果通常依赖前一个 scorer 写入的字段
- selector 本身就是顺序阶段

## 3. 结果合并语义

### 3.1 QueryHydrator

`hydrate_query()` 的行为是：

1. 过滤出 `enable(query)` 为真的 hydrator
2. 所有 hydrator 基于同一份原始 `query` 并发运行
3. 每个 hydrator 返回一个局部 hydrated `Q`
4. 框架按 hydrator 列表顺序调用 `update(&mut hydrated_query, partial_query)` 合并结果

关键结论：

- 同一轮 `QueryHydrator` 看不到彼此的输出
- 最终合并顺序是装配顺序，不是完成顺序

### 3.2 Source

`fetch_candidates()` 的行为是：

1. 并发执行每个 source
2. 成功返回的 `Vec<C>` 依次 append 到总结果里

关键结论：

- source 结果顺序由 source 列表顺序决定
- 不会按返回时间做交错 merge

### 3.3 Hydrator

`run_hydrators()` 的行为和 query hydrator 类似：

1. 所有启用的 hydrator 都基于同一份候选快照 `&candidates` 运行
2. 每个 hydrator 必须返回与输入相同长度、相同顺序的 `Vec<Result<C, String>>`
3. 框架再按 hydrator 列表顺序调用 `update_all()`，只合并成功候选

关键结论：

- 同 stage 的 hydrator 之间不能依赖彼此新增字段
- 单候选错误只保留该候选原值；返回长度不一致时，整份结果会转成错误并打 warning

### 3.4 Filter

`run_filters()` 对每个 filter 都会：

1. 先保存一份 `backup = candidates.clone()`
2. 通过本地 `try_run(query, candidates)` 执行 Filter；默认实现调用上游兼容的同步 `run -> filter`
3. 成功时用 `result.kept` 覆盖当前候选，并把 `result.removed` 追加到总 removed 列表
4. 只有覆盖了 `try_run` 的可失败适配器返回错误时，才记录错误并回滚到 `backup`

关键结论：

- 标准 Filter 是同步、不可失败的上游合同
- `try_run` 是 additive fail-open 扩展；其失败不会丢失前面 Filter 已经成功移除的候选

### 3.5 Scorer

`score()` 和 hydrator 类似，但按 scorer 列表串行执行：

1. scorer 基于当前候选切片返回等长同序的 `Vec<Result<C, String>>`
2. 框架调用 `update_all()`，只合并成功候选

关键结论：

- scorer 可以显式依赖前一个 scorer 写入的字段
- 单候选错误保留该候选当前字段；长度不一致时整份 scorer 输出会转成错误，不中断流水线

## 4. 错误处理矩阵

| 阶段 | 失败后行为 | 是否中断流水线 | 备注 |
| --- | --- | --- | --- |
| `QueryHydrator` | 记录 error，忽略该 hydrator 输出 | 否 | 查询保留已有字段 |
| `Source` | 记录 error，忽略该 source 输出 | 否 | 其他 source 继续 |
| `Hydrator` 单候选失败 | 记录失败数量，只忽略对应候选更新 | 否 | 其他候选正常更新 |
| `Hydrator` 长度不匹配 | 记录 warning，整份输出转为错误 | 否 | 所有候选保留原值 |
| `Filter::try_run` | 记录 error，回滚到该 Filter 执行前 | 否 | 仅本地可失败扩展；标准 Filter 不返回错误 |
| `Scorer` 单候选失败 | 记录失败数量，只忽略对应候选更新 | 否 | 保留该候选当前得分字段 |
| `Scorer` 长度不匹配 | 记录 warning，整份输出转为错误 | 否 | 所有候选保留当前字段 |
| `Selector` | 无 `Result`，无框架级错误处理 | 是，若内部 panic | 当前需业务自行保证 |
| `SideEffect` | 后台 `join_all` 执行并记录成功或错误 | 否 | 主响应不等待完成 |

这说明该框架整体是“尽量给结果”的降级风格，而不是严格的 fail-fast 风格。

## 5. 输出裁剪语义

这里有两个裁剪点：

- `selector().size()`：发生在 select 阶段
- `result_size()`：发生在 post-selection 过滤之后

这两个值可以不同。当前 `home-mixer` 的装配就是：

- selector 先保留 Top 50
- post-selection 过滤后再截断到 35

这会产生一个非常重要的行为：

- 如果 post-selection 过滤后只剩 31 条，框架不会回到 selector 之前补候选
- 最终结果允许少于 `result_size()`

## 6. `PipelineResult` 的真实含义

### `retrieved_candidates`

它保存的是：

- source 召回结果
- 再经过 pre-selection hydrators 合并后的结果

它不是 source 原始输出快照。

### `filtered_candidates`

它是两部分 removed 的拼接：

- pre-selection filters 移除的候选
- post-selection filters 移除的候选

没有额外字段标记某个候选是在哪个 filter 被移除的。

### `selected_candidates`

它保存的是：

- 经过 scorer
- 经过 selector
- 经过 post-selection hydrator/filter
- 再经过最终 truncate

之后的最终结果。

## 7. 非直观但很关键的行为

### 7.1 同 stage 的 hydrator 使用的是同一份旧快照

这意味着如果：

- `Hydrator B` 依赖 `Hydrator A` 写入的字段
- 但两者被放在同一个 hydrator 列表里

那么 `B` 看到的仍然是写入前的数据。

### 7.2 SideEffect 是 fire-and-forget

框架对 side effect 的处理是：

- `tokio::spawn`，主链路不 await
- 后台任务内 `join_all` 后逐个检查结果：成功记 `info!`、失败记 `error!`（含组件名与耗时）

所以它不会影响主链路返回；结果有 request 级日志，但不进入 metrics 维度。

### 7.3 selector 已有框架级日志

`PipelineStage` 包含 `Selector` 变体，`select()` 会输出框架级 info 日志（`request_id=... stage=Selector component=... input=N selected=K non_selected=M elapsed_ms=...`），`selector.rs` 的 `run()` 还统一调用 `stat()` 包装。只有需要更细粒度的排序行为观测时，才需要在业务 selector 内部自行打点。

### 7.4 过滤和打分阶段都允许“静默降级”

只要组件返回 `Err(String)`，框架就会继续执行后续阶段。这很适合高可用 Feed，但对问题定位提出了更高要求：

- 需要稳定的 request_id
- 需要阶段级 metrics
- 需要在业务组件里补充更具体的 error 上下文

## 8. 对实现者的约束总结

如果你要新增一个组件，至少要遵守下面这些硬约束：

1. `Hydrator` / `Scorer` 必须返回与输入一一对应、等长同序的结果。
2. `update()` / `update_all()` 只能改自己拥有的字段。
3. 同 stage 组件之间不能假设存在数据依赖。
4. 标准 `Filter` 实现同步 `filter()`；只有可失败外部适配器才覆盖 `try_run()`，并接受失败后恢复旧输入的语义。
5. `SideEffect` 不能依赖“必须成功”语义，因为框架不会等待它完成。
