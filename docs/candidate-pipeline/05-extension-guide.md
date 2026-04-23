# 扩展与接入指南

本篇面向要继续演进这条流水线的人，重点讲“新增功能时该挂在哪一层”，以及当前框架有哪些硬边界。

## 1. 先判断应该扩展哪一类组件

| 需求 | 推荐扩展点 | 原因 |
| --- | --- | --- |
| 需要补查询上下文，比如用户画像、实验参数、特征开关 | `QueryHydrator` | 输出进入整条链路，适合前置补全 |
| 需要从新数据源召回候选 | `Source` | 负责产出新的候选集合 |
| 需要补候选属性，但不该删除候选 | `Hydrator` | 负责 enrich，不改变候选数量 |
| 需要剔除不满足规则的候选 | `Filter` | 框架已提供 kept/removed 语义 |
| 需要计算一个新得分或重写已有得分 | `Scorer` | 适合串行叠加 |
| 需要重排、混排、截断 | `Selector` | 它是最终排序入口 |
| 需要异步缓存、打点、回写 | `SideEffect` | 不阻塞主链路 |

最常见的误区是把“会删除候选的逻辑”写进 `Hydrator` 或 `Scorer`。在这个框架里，这样做会破坏等长同序契约。

## 2. 新增组件时必须遵守的约束

### 2.1 Hydrator / Scorer 只能做等长同序更新

你必须保证：

- 返回结果长度和输入一致
- 结果顺序和输入一致
- 每个位置一一对应原候选

如果你想删候选，请去 `Filter`。

### 2.2 `update()` 只能改自己拥有的字段

推荐做法是：

- `hydrate()` / `score()` 返回一个只填自己字段的“局部候选”
- `update()` 只拷贝自己负责的字段

不要在一个组件里顺手重写其他组件也会写的字段，否则后续很难排查覆盖顺序问题。

### 2.3 同 stage 组件不能互相依赖

这是当前框架最重要的限制之一。

如果一个新 `Hydrator` 需要读取另一个 `Hydrator` 刚刚补出来的字段，有三种安全做法：

1. 把两步合并成一个 hydrator
2. 把依赖逻辑移到后续串行阶段，比如 `Filter` 或 `Scorer`
3. 扩展框架本身，增加一个新的阶段边界

不要简单地把两个有依赖关系的 hydrator 放在同一个 `Vec<Box<dyn Hydrator<...>>>` 里。

### 2.4 Filter 要接受 fail-open 语义

当前框架里 filter 失败会回滚到执行前的输入并继续。这意味着：

- filter 不能把“成功执行”当成主链路的强保证
- 真正必须生效的规则，不适合只靠一个会失败的异步 filter

### 2.5 SideEffect 不能承载关键路径语义

`SideEffect` 适合：

- 缓存
- 日志
- 观测
- 异步回写

不适合：

- 必须成功的计费
- 必须成功的风控写入
- 影响主链路返回内容的逻辑

## 3. 在现有 `PhoenixCandidatePipeline` 中新增组件的建议位置

### 3.1 新增召回源

适合场景：

- 热门内容召回
- 图谱召回
- 相似作者召回

建议做法：

- 放在 `sources` 列表
- 让 `served_type` 能区分来源
- 明确是否受 `in_network_only` 约束

### 3.2 新增候选补全

适合场景：

- 帖子实体扩展
- 作者特征补全
- 实时行为统计补全

建议做法：

- 如果下游 filter / scorer 依赖它，确保阶段关系正确
- 尽量把同一外部服务的多个字段放在一个 hydrator 内批量取

### 3.3 新增规则过滤

适合场景：

- 业务白名单/黑名单
- 语言、地域、设备限制
- 新的用户偏好过滤

建议做法：

- 能用本地字段判断就不要再发外部请求
- 若必须发请求，注意失败后的降级语义是否可接受

### 3.4 新增排序信号

适合场景：

- 新模型得分
- 内容质量分
- 探索性、多样性、新鲜度调节

建议做法：

- 如果是“原始模型输出”，放在独立 scorer
- 如果是“把多个信号聚成一个排序分”，放在聚合 scorer
- 如果是“对已有 score 做策略性乘法或偏移”，放在后置 scorer

## 4. 推荐的实现模板

### 4.1 QueryHydrator 模板

```rust
#[async_trait]
impl QueryHydrator<MyQuery> for MyHydrator {
    async fn hydrate(&self, query: &MyQuery) -> Result<MyQuery, String> {
        let value = self.client.fetch(query.user_id).await.map_err(|e| e.to_string())?;
        Ok(MyQuery {
            my_field: Some(value),
            ..Default::default()
        })
    }

    fn update(&self, query: &mut MyQuery, hydrated: MyQuery) {
        query.my_field = hydrated.my_field;
    }
}
```

### 4.2 Hydrator 模板

```rust
#[async_trait]
impl Hydrator<MyQuery, MyCandidate> for MyHydrator {
    async fn hydrate(
        &self,
        _query: &MyQuery,
        candidates: &[MyCandidate],
    ) -> Result<Vec<MyCandidate>, String> {
        let partials = candidates
            .iter()
            .map(|candidate| MyCandidate {
                my_field: Some(compute(candidate)),
                ..Default::default()
            })
            .collect();
        Ok(partials)
    }

    fn update(&self, candidate: &mut MyCandidate, hydrated: MyCandidate) {
        candidate.my_field = hydrated.my_field;
    }
}
```

### 4.3 Filter 模板

```rust
#[async_trait]
impl Filter<MyQuery, MyCandidate> for MyFilter {
    async fn filter(
        &self,
        query: &MyQuery,
        candidates: Vec<MyCandidate>,
    ) -> Result<FilterResult<MyCandidate>, String> {
        let (kept, removed) = candidates.into_iter().partition(|c| keep(query, c));
        Ok(FilterResult { kept, removed })
    }
}
```

## 5. 如果要把这套框架迁到别的业务

建议按下面顺序做，而不是直接复制 `PhoenixCandidatePipeline`：

1. 先定义好业务自己的 `Query` 和 `Candidate`
2. 明确每个字段由哪个组件拥有
3. 先接通最小闭环：一个 query hydrator、一个 source、一个 hydrator、一个 filter、一个 scorer、一个 selector
4. 跑通后再逐步增加更多 source / filter / scorer

原因很简单：

- 这套框架的复杂度主要不在 trait 数量，而在字段 ownership 和阶段顺序
- 一开始字段设计不清楚，后面会持续被 `update()` 覆盖问题拖住

## 6. 新增功能前的检查清单

建议每次新增组件前至少回答下面几个问题：

1. 它补字段、删候选、算分还是异步回写？
2. 它是否依赖前一个组件刚补出来的字段？
3. 失败时主链路应该 fail-open 还是 fail-fast？
4. 是否需要按请求动态 enable？
5. 是否需要批量化访问外部服务？
6. 是否需要为日志和 metrics 提供稳定组件名？

如果第 2 个问题回答为“是”，那就不要直接把它放进当前的并发 hydrator stage。
