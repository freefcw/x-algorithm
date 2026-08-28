# 提交 24c60942 能力清点与 U0–U3 分类（2026-08-28 上游快照）

> 文档状态：清点完成，当前有业务落点的能力已落地
> 上游范围：`45b48ba6baa40e212f6dcbaf8fe9fdc8d9da722e..24c60942c5c5fdad3a6addffb4c6e6d2f228f04f`
> 此前已吸收锚点：`45b48ba6baa40e212f6dcbaf8fe9fdc8d9da722e`
> 本地目标分支：`feature/migrate-20260515`
> 迁移规则：[`upstream-first-maintenance.md`](./upstream-first-maintenance.md)
> 迁移结果：[`../update/20260828.md`](../update/20260828.md)

## 1. 范围概览

上游提交涉及 41 个文件，增加 2071 行、删除 265 行。改动可归为六组：

1. Home Mixer 多风险广告混排、请求上下文、指标和专项过滤规则。
2. Phoenix 协议、异步 gRPC 压缩、copy-port、checkpoint 与训练配置。
3. visibility-filtering 缓存预热。
4. Grox reply-spam 分类与写入流程。
5. abuse-enforcement-service AIS 合同。
6. SimClusters 离线数据读取。

本地当前业务目标仍是可独立运行的 Home Mixer 推荐链和 Phoenix 演示/服务链。迁移以行为或合同是否能执行、验证为准，不按文件数量整体复制。

## 2. 已落地能力

| 编号 | 能力 | 分类 | 本地处置 |
|---|---|---|---|
| P1 | Phoenix `recsys.proto`：视频 6 秒观看动作、profile long dwell 聚合名称、`SlateContext.reconCountAbove/reconGapAbove` | **U0** | Rust/Python 两份 proto 同步采用；字段编号和枚举值保持不变或向后追加，不启用对应训练、广告或 served SlateContext 行为 |
| P2 | 异步 zstd 压缩后保留 gRPC trailers | **U0** | 在现有 `GrpcCompressionService` 中保留并重建 trailers；覆盖压缩响应、小响应和非 OK `grpc-status/grpc-message` |
| D1 | Phoenix 干净 CUDA 容器安装依赖 | **U0 文档** | 安装命令加入 `ca-certificates` 和 `curl`，与后续 protoc 下载步骤一致 |
| H1 | CJK 静音关键词整词回归 | **已存在** | 当前分支提交 `f72e345` 已包含同等测试和整词匹配行为，不重复迁移 |

## 3. 延期能力与重入条件

| 编号 | 能力 | 分类 | 不立即落地的业务原因与重入条件 |
|---|---|---|---|
| A1 | `MultiRiskAdsBlender`、BSR_HIGH 对 MediumRisk 的差异化邻接规则及指标 | **U3** | Ads Source 仍显式关闭，且缺少真实广告和 VF 安全合同。重入条件：广告 Provider、风险字段来源、失败语义、启用策略和端到端样本齐全；届时基于本地域模型迁移纯混排规则，指标留在外层 |
| H2 | 从 `stratocontext`/`stratocontext-bin` 提取 polling 状态 | **U3** | 当前公开调用方只使用请求体字段，没有该内部 metadata 合同。重入条件：真实调用方提供编码合同、优先级和兼容性样本；解析只放在 gRPC 边界 |
| O1 | Home Mixer 按 client/version/polling/request context 打点 | **U1，延期** | 当前无对应指标消费和内部 client 分类依赖。重入条件：先定义本地统计字段、sink 和 dashboard，再通过 `FeedStatsSink` 接入 |
| E1 | copy-port shard 下载按 pod/tensor 打散并恢复校验顺序 | **N/A（当前故障面）** | copy-port 默认关闭，暂无多实例下载热点或启动耗时证据。重入条件：基准确认热点后整体迁移并在多实例环境对拍 checksum |
| C1 | KMS envelope 加密 checkpoint 读取 | **U3** | 本地没有 KMS、认证和加密存储合同。重入条件：明确密钥服务、权限、超时、审计和恢复责任后，以 Adapter 接入 |
| C2 | `restore_streamed` 和窗口化 checkpoint 恢复 | **N/A（当前运行目标）** | 延续既有结论：没有大 checkpoint 加载 OOM 或峰值内存证据。重入条件：真实产物基准证明整状态 staging 成为问题 |
| O2 | Phoenix 网络延迟指标增加 `src_dc`，压缩耗时指标 | **N/A（当前观测需求）** | 当前没有按机房拆分或压缩队列耗时的监控需求；有 dashboard 和告警消费者时再采用 |
| X1 | `concat_history_bridge_prob` 配置透传 | **U3** | 模型字段存在，但当前配置没有启用方和对应数据验收。重入条件：训练配置实际使用 bridge probability 并有特征形状/效果测试 |

## 4. 不适用能力

| 编号 | 能力 | 原因 |
|---|---|---|
| B1 | Brazil 2026 election filter 名单维护 | 延续既有产品决定：本地不迁移国家/选举专项规则 |
| V1 | visibility-filtering L2 cache warmer 和客户端 ID 配置 | 本地已剥离 visibility-filtering，不能复制无服务入口的缓存预热器 |
| AE1 | abuse-enforcement-service AIS bounce 合同和 schema 测试 | 本地无该服务；处罚执行不属于 Feed 推荐域 |
| G1 | Grox PTOS/reply-spam 模型、Prompt 和 Manhattan/Kafka 写入 | 本地 Grox 仍是中立 Demo，真实模型、策略和 Sink 合同未具备，维持 P6-B U3 |
| S1 | SimClusters Scala fallback snapshot | 本地无 Scalding/DAL 执行链和数据集 |
| P3 | Phoenix 参数同步时间戳、无行为变化的格式调整 | 不产生本地合同或业务行为变化 |

## 5. 设计判断

本轮没有把上游的协议对象、全局指标或内部服务依赖直接带入 Home Mixer 领域层。广告、请求 metadata 和统计能力继续遵循“外部合同先闭合，再装配”的规则。

异步压缩属于基础设施边界，通过响应重建函数修复 trailers，不改变预测服务和模型逻辑。Proto 只同步合同，不提前实现没有生产者或消费者的 SlateContext 行为。

## 6. 验证

- `cd phoenix && cargo test -p xai-recsys-proto`：6 通过
- `cd phoenix && PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test -p xai-recsys-engine grpc_compression`：5 通过
- Rust/Python 两份 Phoenix `recsys.proto` 字节一致
- 根目录和 `phoenix/` 的 `cargo fmt --all -- --check` 通过
- `cargo test --workspace`：255 通过
- `cd phoenix && PYO3_PYTHON="$PWD/.venv/bin/python3" cargo test --workspace`：122 通过、3 ignored
- `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run pytest`：92 通过
- `git diff --check` 通过
- `cargo clippy -p xai-recsys-proto --all-targets -- -D warnings` 通过
- `xai-recsys-engine` 在仅放行未改动文件中的既有 `collapsible_if` 和 `unnecessary_min_or_max` 后通过 Clippy；严格 `-D warnings` 仍被这两个既有告警拦住，本轮不修改无关代码

## 7. 锚点结论

范围内每项能力均已有落地、延期或不适用结论，本轮改动的测试、格式和定向静态检查通过，上游同步锚点从 `45b48ba` 前移至 `24c60942`；这不表示广告、Strato metadata、copy-port、KMS、VF 或 Grox 生产能力已经启用。
