# 客户端行为事件上报说明（UAS）——已并入新文档

> **状态**：`historical`（已废弃，全文由 [user-action-collect.md](./user-action-collect.md) 取代）  
> **废弃日期**：2026-09-17  
> **配套合同**：[uas-event-contract.md](./uas-event-contract.md)

本文内容已修正并入 [user-action-collect.md](./user-action-collect.md)。并入时修正了本文与消费端合同（[uas-event-contract.md](./uas-event-contract.md) §3）不一致的三处动作归属：

1. **`15` 不感兴趣**：产品当前没有可采集入口，**不要上报**（本文误列为客户端"必须报"）；
2. **`14` 从帖子上关注作者**：由**客户端**上报并带 `tweet_id`——后端关注落库无法关联到具体帖子（本文误归为服务端发）；
3. **`18` 举报帖子**：默认由**服务端**在举报落库后上报（本文误列为客户端"必须报"）。

客户端与埋点团队请直接阅读新文档，不要按本文实现。新文档另补充了当前模型非零权重行为（`supported-actions`）、消费端处理上限与聚合规则、投递语义（at-least-once / `event_id`）和联调验收步骤。
