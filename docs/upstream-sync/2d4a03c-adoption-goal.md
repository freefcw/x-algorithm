# 上游 2d4a03c 选择性吸收目标

## Go / No-Go

Go。用户已批准上一轮审查建议；本轮完成可在本地验证的实现与同步台账。GPU 运行验收单独保留，不将 CPU 测试当作 H100 性能或训练正确性的证明。

## 目标与边界

- 类型：技术交付；基线为 `mp-trunk@a731066`，上游增量为 `6bb4594..2d4a03c`。
- Kafka 指标默认上报间隔同步为 120 秒。
- Home Mixer 增加本地候选多样性 sink 和上游同名 SideEffect，统计 final/top10 的作者、来源、网内比例；使用真实 ObjectId，无外部私有依赖。
- 对齐 H100 combined attention 与 CuTeDSL 的前置配置，修复 GB300 combined 继承的不合法配置；保留本地 GB300 batch/EP/remat/unroll 调整。
- 不引入 purchase-value、SID bitset、Arrow viewer history、实验分桶和 Grox 阈值；不记录缺少可靠基线的 pre_heuristic。
- 不修改推荐排序/过滤语义、不部署；本目标文档完成后按拆分方案提交 Git commit。
- 验收：针对性测试、根 workspace 测试、格式检查、端到端 demo 通过；实现与台账一致。由执行代理根据命令证据判定源码交付完成，GPU 运维/训练负责人另行验收硬件路径。

## 当前状态与优先级

实现前锚点为 `6bb4594`。Home Mixer 有作者降权但无候选组成遥测；FeedStatsSink 只接受最终 FeedItem 类型统计。fallback 会清空 weighted_score，不能用它伪造重排基线。

Kafka 两处默认均为 30 秒。H100 combined 用 Pallas；H100/GB300 combined 共同继承 `qk_norm=False`、softcap=80，与 CuTeDSL 断言冲突。

先确定目标与测试边界，再并行实现互不相交的 Home Mixer/Phoenix 变更，最后统一审查并验收。

## 假设与待验事项

| 事项 | 状态 | 处理 |
|---|---|---|
| 统计后端 | 本地已有日志机制 | 新 port 默认使用日志 adapter，采样策略显式注入；不引入新服务 |
| GPU | 本机 macOS，无 H100 | 配置兼容性自动校验；forward/backward、短训练、旧 checkpoint 验收列为外部待验 |
| Kafka | 无真实 collector/集群验收 | 同步默认值并验证语法；真实上报不宣称已验证 |

## 阶段与 Todo

### P1：可观测的候选多样性

入口：本目标已写入；边界：只新增 SideEffect，不改 scorer/selector，不复用不匹配的 FeedStatsSink。

- [x] 先写指标行为与装配测试，记录目标行为缺失的 RED 证据。
- [x] 移植纯 std Composition，并实现 final/top10 统计、可注入 sink、采样和日志 adapter。
- [x] 装配真实消费路径；验证空响应、少于 10 条、ObjectId、fallback、sink 失败隔离。
- [x] 跑通过受影响测试后审查并同步 Home Mixer 可观测性文档。

退出证据：统计值与装配测试通过；停止条件：实现要求新增业务协议或修改排序行为。

### P2：Phoenix 默认配置与 attention 兼容性

入口：P1 可并行执行；边界：两处 Kafka 默认一起改；不覆盖本地 GB300 定制项。

- [x] 同步 Kafka 120 秒默认值；此低影响配置变更仅做必要检查，不为常量写镜像测试。
- [x] 先添加最终 combined 配置与 attention 要求的兼容性回归，记录 RED。
- [x] 修正 qk_norm/softcap 前置项并切换 H100 attention，验证 GREEN。
- [x] 记录 GPU / 旧 checkpoint / 实际 Kafka 数据面验收限制。

退出证据：配置兼容性测试、相关 Python 测试通过；停止条件：发现必须迁移 checkpoint 或模型参数而无法保持源码边界。

### P3：集成与台账

入口：P1/P2 完成；边界：只对齐本次能力与受影响文档。

- [x] review diff，运行根 `cargo test --workspace` 与 `cargo clippy --workspace --all-targets`。
- [x] 运行相关 Python 测试与端到端 demo。
- [x] 新增能力清单和 RED/GREEN/REFACTOR 证据，按实际结果更新同步锚点及 GPU 待验状态。

退出证据：测试、demo 和文档一致；停止条件：出现与本次变更有关的回归，修复后重验。

## Dry-run

Home Mixer 的数据字段与 SideEffect 框架齐备，唯一新增契约是本地统计 port。Phoenix 配置断言可以在 CPU 检查，但不证明 kernel 可运行。上述边界避免恢复已延期协议，两个代码阶段可由独立代理处理。

## TDD 与验证证据

- Home Mixer RED：统计测试先于实现运行时因缺少目标模块而无法通过；GREEN 后多样性测试从 7 项扩展到 8 项并通过。覆盖空 slate、5% 采样、final/top10、同分稳定排序、无效分数、ObjectId、未知来源、fallback 和 sink 失败隔离。
- Phoenix RED：`test_combined_presets_satisfy_cutedsl_varlen_attention_prerequisites` 在旧配置下失败（`qk_norm` 为 `False`）；GREEN 后配置与 segment-id 测试 6 项通过。
- 根验证：`cargo test --workspace` 431 passed；`cargo clippy --workspace --all-targets -- -D warnings` 通过；Phoenix 全量 Python 测试 115 passed（2 个既有 JAX 弃用警告）；`./scripts/run_demo.sh` 返回 35 条推荐。
- REFACTOR：修正未知 `served_type` 的统计归属、NaN/Infinity 分数排序和不可 Clone 的 SideEffect 注入方式；格式、diff 检查和回归测试再次通过。

## 最终验证与首步

首步：代码代理按以上范围先写行为回归并验证失败；主代理并行准备集成验证和文档更新。最终附真实命令结果，不以补丁应用成功替代功能验证。
