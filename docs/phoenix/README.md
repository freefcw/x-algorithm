# Phoenix 文档入口

> **状态：`current-code`**

`phoenix/` 当前只保留 xrex 生产引擎、Rust serving crate、真实事件输入工具和相关测试。旧的单机 JAX Demo、随机权重服务、旧 gRPC gateway 及其部署清单已经删除。

## 唯一操作入口

| 目标 | 文档 | 事实来源 |
| --- | --- | --- |
| 训练数据、样本和产物 | [06-training-and-data.md](./06-training-and-data.md) | `phoenix/xrex/`、`phoenix/scripts/build_training_inputs.py` |
| 接入真实事件和特征 | [07-real-data-integration.md](./07-real-data-integration.md) | xrex 数据加载器与事件合同 |
| 训练、评估、部署和验收 | [08-production-handbook.md](./08-production-handbook.md) | xrex driver、inference、Rust engine |
| 环境、测试和保留工具 | [`phoenix/README.md`](../../phoenix/README.md) | `phoenix/pyproject.toml`、测试目录 |

## 与 Home Mixer 的边界

Home Mixer 当前的 Phoenix 客户端合同与 xrex serving 合同尚未完成适配。不要把 xrex ranking/retrieval 进程配置成旧 `phoenix-gateway`，也不要恢复已删除的 `HOME_MIXER_MODE=demo`、`run_grpc_gateway.py` 或本地随机权重链路。正式接入必须先完成协议适配、真实 checkpoint/index 配置和 contract test。

## 历史资料

旧的模型结构分析、HTTP/gRPC gateway 分析和端到端 Demo 教程不再属于当前文档集；它们已经删除。变更记录与迁移材料仍可能包含历史命令，只能用于考古，不能作为操作依据。
