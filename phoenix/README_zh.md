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

仍待实现：真实特征/事件接入、生产部署配置及 readiness/监控/回滚验证；协议 Adapter 已有
第一版实现，但尚未达到生产切流条件。

## Home Mixer → xrex Adapter（第一版）

`scripts/run_xrex_adapter.py` 对外提供 Home Mixer 现有的
`PhoenixPredictionService` / `PhoenixRetrievalService` 合同，内部调用 xrex 的
`RecsysPredictor` / `RecsysRetrievalPredictor`。Home Mixer 不需要改用 xrex proto。

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/run_xrex_adapter.py \
  --listen '[::]:50053' \
  --xrex-address localhost:50054
```

Adapter 只接受 canonical Snowflake 数值 ID。ObjectID 到 Snowflake 的映射必须在
Home Mixer ingress、训练数据构建和 retrieval/index 发布阶段通过统一 ID Registry
完成；adapter 不再使用 MD5、截断或独立 external-ID dictionary 生成第二套 ID。

adapter 会在响应 metadata 中携带 `identity-map-version`，并在请求/响应边界校验
Snowflake 范围、候选身份、作者身份和 action taxonomy。当前版本只转换已存在的最小
UAS/candidate 字段；SID、多模态和画像字段尚未接入，不能据此宣称生产模型特征已完整对齐。
