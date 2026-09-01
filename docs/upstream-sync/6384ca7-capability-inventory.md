# 提交 `bc8e5f0` / `6384ca7` 能力清点与吸收结果

> 清点日期：2026-09-01
> 上游范围：`24c60942..6384ca7`
> 本地目标分支：`feature/migrate-20260515`
> 结果：Phoenix 基础设施与多模态缓存能力选择性吸收；其余能力按外部合同延期或不适用

## 1. 范围概览

两个上游提交共涉及 79 个文件，增加 4064 行、删除 1777 行。真正有本地落点的内容集中在 Phoenix Rust 引擎、Phoenix 多模态 embedding server 和 xrex driver 命名；上游 visibility-filtering、广告、Kafka mTLS、Grox 生产安全链路及其他内部服务不属于当前可独立验证的本地运行目标。

## 2. 已吸收

### P1：Phoenix copy-port / parser / embedding table 基础设施

吸收文件：

- `phoenix/crates/serving/xai-recsys-engine/src/copy_port_client.rs`
- `phoenix/crates/serving/xai-recsys-engine/src/emb_table.rs`
- `phoenix/crates/serving/xai-recsys-engine/src/proto_parser.rs`

吸收内容：

- 统一 copy-port transfer task join 和错误转换。
- 使用 `Bytes` callback 支持 gRPC body 分块消费。
- embedding table gRPC 拷贝使用有界分块 channel 和 blocking worker，保留 checksum 校验与本地调度语义。
- 保留本地已有的 shard schedule、测试裁剪和其他运行时改造，不覆盖本地非上游行为。

### P2：Phoenix 多模态 embedding 配置与缓存

吸收文件：

- `phoenix/crates/common/xai-recsys/Cargo.toml`
- `phoenix/crates/common/xai-recsys/src/model_config.rs`
- `phoenix/crates/common/xai-recsys/src/util.rs`
- `phoenix/crates/serving/xai-recsys-engine/src/python.rs`
- `phoenix/crates/serving/xai-recsys-mm-server/Cargo.toml`
- `phoenix/crates/serving/xai-recsys-mm-server/src/mm_embedding_client.rs`
- `phoenix/crates/serving/xai-recsys-mm-server/src/snapshot.rs`

吸收内容：

- 集中定义 `MultimodalEmbeddingType`、维度、解析和 trainer override。
- 统一从 `ModelConfig` 读取 search query embedding dimension。
- MM cache 增加同步插入、parquet bytes ingest 和同步 fetch 接口。
- 使用 Rayon 并行读取 embedding cache，增加 in-process cache 命中/缺失回归测试。
- 保留本地模型和 published/demo 兼容路径。

### P3：xrex driver 命名对齐

- `phoenix/xrex/driver/driver.py` 重命名为 `core.py`。
- `phoenix/xrex/driver/driver_local.py` 重命名为 `local.py`。
- 更新 `config_factory.py` 与 `local.py` 内部 import。

## 3. 延期或不吸收

| 能力 | 分类 | 处置 |
|---|---|---|
| `reranker_head_tag` Home Mixer/Phoenix 字段链路 | U3 | 当前本地协议和模型没有消费者，不引入无效字段 |
| `PageDecode` 请求/响应协议 | U3 | 没有本地 Page Decode 模型和调用方 |
| delayed conversion / ads heads | U3 | Ads Source、广告样本、训练和评估合同均未接入 |
| visibility-filtering `RuleContext`、golden corpus 和 safety labels | U3 | 本地使用 fail-closed VF 适配器，无上游 VF 服务和 safety label store |
| ScoreInfo 去预测分数、score stats bucket | U1/U3 | 上游实现依赖不同的 cache/stats 合同，保留设计参考，不直接覆盖本地实现 |
| 加密 checkpoint helper | U3 | 无 KMS、认证、密钥轮换、审计和恢复合同 |
| Thunder Kafka mTLS | U3 | 无真实 Phoenix Kafka 集群、证书和 zone 部署合同 |
| Grox special video workflow | U3 | 本地 Grox 是无生产模型的中立 Demo |
| abuse-enforcement-service、SimClusters offline 改动 | U3 | 不属于当前推荐链运行域 |
| Brazil 2026 election 名单更新 | 产品专项 | 不与通用同步混合；只有业务要求严格追齐上游名单时单独更新 |

## 4. 验证

- 根 workspace：`cargo fmt --all -- --check` 通过。
- 根 workspace：`cargo test --workspace`，255 通过。
- Phoenix：`cargo fmt --all -- --check` 通过。
- Phoenix：`PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`，125 通过、3 ignored。
- `git diff --check` 通过。
- 系统默认 `python` 不存在；`python3` 版本低于项目语法要求，未用它作为 Python 测试结论。

## 5. 锚点结论

本轮只吸收了具备本地代码落点且能通过现有测试验证的 Phoenix 基础设施能力。同步锚点从 `24c60942` 推进到 `6384ca7`；这不表示本地已经启用上游的 VF、广告、Page Decode、Kafka mTLS、加密 checkpoint 或 Grox 生产能力。
