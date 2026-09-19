# Phoenix 训练与数据

> **状态：`current-code`**

本文只描述当前 xrex 生产训练链路。旧的单机 JAX、`.npz`、mock corpus、随机权重和本地 gateway 训练路径已删除，不作为训练或发布方案。

## 1. 当前数据流

```text
served-candidates + UAS 事件
  → build_training_inputs.py
  → Parquet/Kafka training input
  → xrex trainer
  → checkpoint / metadata / retrieval index
  → xrex ranking 或 retrieval serving
```

事实来源是 `phoenix/xrex/data/`、`phoenix/xrex/train/`、`phoenix/xrex/eval/` 和 `phoenix/scripts/build_training_inputs.py`。字段、时间窗口和版本 metadata 不应从旧文档或旧张量示例复制。

## 2. 训练前置条件

- 服务端曝光事件必须包含 request、候选顺序、候选 ID、时间和 serving metadata；
- UAS 行为事件必须包含用户、帖子、动作、时间和 product surface；
- 训练、验证和测试按用户或时间隔离，避免未来信息泄漏；
- 删除、权限和隐私保留策略必须先于样本生成确定；
- 训练输入中的 ID、feature schema、action 映射必须与 serving 使用同一版本。

## 3. 训练与评估

生产训练使用 Linux + NVIDIA CUDA 环境，命令和参数以 `phoenix/xrex/` 的 driver、config 和测试为准。训练完成后至少验证：

1. checkpoint 可加载且 metadata 完整；
2. ranking 输入输出 shape、action 映射和 mask 与合同一致；
3. retrieval index 与模型版本绑定；
4. 离线指标优于规则基线，并按用户/内容切片检查；
5. 固定回放集的结果、延迟和资源使用符合上线门槛。

## 4. 产物发布

发布单元至少包括代码版本、数据版本、feature schema、checkpoint、retrieval index、训练配置和评估结果。任何一个版本不匹配都应拒绝加载，而不是回退到随机参数。

详细的真实数据字段映射见 [07-real-data-integration.md](./07-real-data-integration.md)，部署和验收见 [08-production-handbook.md](./08-production-handbook.md)。
