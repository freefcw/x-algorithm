# 训练与数据文档索引

状态：`design`

本目录描述 Phoenix 训练数据、离线产物和索引更新的**目标形态**（例行化训练、索引切版等生产设计）。基础训练能力已在仓库中落地（`phoenix/scripts/train_*.py`），操作手册见 [phoenix/docs/训练指引.md](../../phoenix/docs/训练指引.md)，入门教程见 [getting-started 第三步](../getting-started/04-第三步-训练自己的模型.md)；代码事实以 [../phoenix/README.md](../phoenix/README.md) 为准。

## 文档

| 文档 | 说明 |
| --- | --- |
| [training_data_spec.md](./training_data_spec.md) | Phoenix 训练样本字段、Tensor 形状和文件格式建议。 |
| [data_preparation.md](./data_preparation.md) | 数据对象、训练节奏、索引切版和监控降级设计。 |
