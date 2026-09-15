# 提交 `2d4a03c` 能力清点与吸收结果

> 上游范围：`6bb4594..2d4a03c`；本地选择性吸收，未整体 cherry-pick。

| 上游变化 | 分类 | 本地处置 |
|---|---|---|
| `phoenix/xrex/data/streaming/kafkaloader.py` 两处 OTel 默认周期 `30s → 120s` | U0 | 已吸收；helper 与 Dataset 默认值同步，显式配置仍可覆盖。 |
| `phoenix/xrex/configs/xrecsys_two_tower.py` H100 CuTeDSL varlen | U0/U1 | 已吸收 attention 前置配置（`qk_norm=True`、`attn_logit_cap=-1`）与 H100 实现切换；保留本地 GB300 batch/EP/remat/unroll。CPU 配置测试通过，GPU forward/backward、Kafka 和旧 checkpoint 仍待 Linux/NVIDIA 验证。 |
| `home-mixer` Response Diversity 统计 | U1/U2 | 未照搬私有 `xai_stats`、ExperimentBucket、SID、Arrow history；新增本地 sink + 日志 adapter，采样记录内层 final/top10 的作者/来源/网内比例，不改排序。 |
| `home-mixer/util/composition.rs` | U0 | 已移植纯标准库组成统计，并由本地 SideEffect 消费。 |
| `tweet_type_metrics`、`viewer_history`、semantic ID 字段 | U3/U4 | 本地无公开字段、columnar provider，且 Arrow `Int64` 无法承载 96-bit ObjectId；未引入。 |
| purchase-value proto/helper | U3 | 仅有协议片段和自测，没有请求生产者、训练标签或 serving 消费者；未引入。 |
| `grox/flows/reply_spam/task_filter.py` 阈值 `10000 → 30000` | 不适用 | 本地 Grox 为独立骨架，不存在该上游 flow。 |

## 验证

- `cargo test --workspace`：431 passed。
- `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- `cargo fmt --all -- --check`、`git diff --check`：通过。
- `phoenix` Python 全量测试：115 passed，2 个既有 JAX 弃用警告。
- Home Mixer 多样性测试：8 passed。
- 配置与 segment-id 测试：6 passed；`compileall` 通过。
- `./scripts/run_demo.sh`：端到端返回 35 条（网内 8、网外 27），通过。

## 未完成的外部验收

本机无 H100/GB300、真实 Kafka/OTel collector 或生产 checkpoint，未宣称 CuTeDSL kernel、Kafka 数据面和旧 checkpoint 续训已完成。
