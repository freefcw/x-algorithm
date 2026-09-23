# 上游 `8b25829`–`1b3fec2` 能力清点与吸收结果

> 清点日期：2026-09-24；比较范围：`c279172..1b3fec2`。本地按能力移植，没有整体合并或 cherry-pick。三个快照合计改动 147 个文件。

| 上游变化 | 分类 | 本地处置 |
|---|---|---|
| `8b25829`：`xrex.cuda.unique` 无 inverse 时的回退分支 | U0 | 已修复 `return_inverse=False` 解包错误，二元组返回契约不变；true/false 两路有回归测试。上游改用 `xrex_cuda_kernels` wheel 的加载方式未吸收，本地没有该包合同，保留现有 out-of-tree 扩展加载方式。 |
| `8b25829`：删除 Home Mixer dwell-regret gate | 已等价 | 本地排序始终使用 weighted 模式，不存在该 gate。 |
| `3aa0fa3`：召回来源、分数和去重后的多路归因 | U1/U4 | 已在本地 `phoenix_recsys.proto` 增加可选 `source_idx` / `dataset_type`，由 xrex adapter 透传；Home Mixer 候选保存召回分数和来源内位置，去重时合并来源；最终下发事件 v1 增加 `retrieval_sources`，旧事件缺少该字段仍可读取。数值 ID 继续在现有身份边界校验，事件仍使用 ObjectId。 |
| `3aa0fa3`：retrieval-candidates Kafka 审计和未入选候选 | U3 | 未吸收。当前 Candidate Pipeline 会主动释放未入选候选，且本地只定义最终下发事件；需要独立确定事件合同、保留策略和身份反查预算。 |
| `3aa0fa3`：copy-port 多连接条带下载 | U3 | 现有分片下载、checksum、限流仍保留；仅在 checkpoint 热切换的真实吞吐测试证明需要时移植调度与失败清理。 |
| `1b3fec2`：VM Ranker DPP embedding memo | U0/U4 | 已按本地 `SnowflakeId` 实现请求级缓存。同一原帖的转帖共用 embedding；缺失 embedding 时共用同一随机回退向量，避免重复查询和错误的多样性判断。 |
| `1b3fec2`：MoE 普通/冷启动双配额 | U3 + 本地修复 | 上游的 `0/200` 双配额不能照搬到本地单 `max_results` 合同；修复本地 MoE Source 错用通用 `1000` 上限的问题，改用专用 `PHOENIX_MOE_MAX_RESULTS=200`。 |
| `1b3fec2`：VM Ranker value model、配置同步、debias 输入 | U3 | 未吸收。需本地 proto、共享打分实现和可用配置源形成完整合同；不能只搬服务端代码。 |
| `1b3fec2`：NSFW 用户态 / safety bit | U3 | 未吸收。缺用户安全态生产者、Home Mixer 入口、模型 schema 和对应训练产物。 |
| 最近三个快照的 Visibility Filtering 服务改动 | U3 | 未导入内部服务。未来生产 VF adapter 验收应覆盖请求响应严格对应、规则声明所需 hydrator、golden corpus 中每条已装配规则均有主导用例。 |
| Grox 媒体流、Thunder 内部 Kafka mTLS 和其他安全离线链路 | 不适用 / U3 | 不属于当前 portable 推荐主链或依赖未公开合同，不整体移植。 |

## 验证

- `cargo test --workspace`：552 项通过，24 项按既有配置 ignored。
- `cargo test -p xai-vm-ranker`：14 项通过。
- `cargo test -p home-mixer --lib`：431 项通过。
- `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q`：46 项通过。
- `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q tests/test_adapter_contract.py`：24 项通过。
- `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q tests/engine`：8 项通过。
- `cargo check -p x-algorithm-proto`、`cargo fmt --all -- --check`、`git diff --check`：通过。

`retrieval_sources` 是最终下发候选的诊断数据；现有训练输入脚本忽略它。它不等同于上游的全量 retrieval-candidates 审计流，也不改变最终排序分。
