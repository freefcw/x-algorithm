# Phoenix 生产引擎

`xrex/`、`crates/` 与本地 `python/` 包是生产训练、推理和协议实现。旧的单机
JAX 教学栈已删除，本目录不再提供假模型、假语料或演示 HTTP 服务。

```bash
uv sync --extra engine --dev
uv run pytest
uv run pytest tests/engine
```

保留合同：`services/model_contract.py`、`services/inference_types.py`、
`services/recsys_proto.py`；真实事件输入工具：`scripts/build_training_inputs.py`；
生产 SID/MM/checkpoint 工具位于 `reference/`。缺少真实 checkpoint、特征事件或部署
配置时必须失败，不能启动随机模型。

待实现：home-mixer 的 `.npz`/字符串 ID 到 xrex 协议适配、真实特征/事件接入、生产部署配置
及 readiness/监控/回滚验证。
