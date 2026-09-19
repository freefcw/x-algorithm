# 当前行为、风险与路线

> **状态：`current-code` + `design`**

## 1. 当前可以确认的行为

- Home Mixer 是 Rust Feed 编排层，执行查询补全、候选召回、内容补全、过滤、打分、选择和副作用；
- 业务数据通过 Recommendation Data 和 viewer-relation 合同接入；
- Redis adapter 承载 UAS 投影与有界 served/request 状态；
- Phoenix 客户端会校验 serving metadata、响应 shape、NaN、重复和缺失候选，失败时回退到 `RuleFallbackScorer`；
- `production_ready` 在生产合同未闭合时拒绝启动；
- xrex 是 Phoenix 生产主线，但尚未与 Home Mixer 当前 Phoenix 请求合同完成适配。

## 2. 风险排序

| 优先级 | 风险 | 影响 | 关闭条件 |
| --- | --- | --- | --- |
| P1 | xrex 与 Home Mixer 协议未适配 | 模型服务不能直接接入 | ranking/retrieval contract test、版本和错误语义验收 |
| P1 | 真实行为事件与 UAS 投影合同未验收 | 个性化和训练归因不可信 | schema、认证、保留、重放、吞吐验收 |
| P1 | served persistence/exposure 端到端未验收 | 训练样本缺曝光事实 | 可靠写入、幂等、对账和告警 |
| P1 | 内容权限和可见性外部合同依赖 | 可能空 Feed 或安全错误 | 真实服务集成与 fail-closed 演练 |
| P2 | 多副本、容量和模型发布流程未验证 | 延迟、恢复和回滚风险 | 压测、灰度、版本和 rollback runbook |

## 3. 不应再出现的假设

- 本地端到端 Demo 能代表生产链路；
- 配置 Phoenix 地址就代表模型已经上线；
- 随机权重或 mock corpus 能证明推荐质量；
- 旧 gateway、整数 Thunder 或本地 fixture 可以替代 xrex/真实业务合同；
- FeedState 记录可以替代服务端曝光事件。

## 4. 推荐路线

```text
Rust/Phoenix 测试
  → 真实业务数据、权限、可见性和 Redis
  → 规则 Feed 与曝光事件
  → UAS 投影
  → xrex 合同适配与 checkpoint/index
  → 灰度、容量、监控和回滚
```

每个阶段只在有可复现的测试、指标和真实输入后推进；历史更新记录不作为当前状态依据。
