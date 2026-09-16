# Repository Guidelines

## Agent 使用说明
本文件描述仓库通用约定。若任务涉及本机工具链、权限、缓存目录、Homebrew 路径或沙箱行为，请按需同时查看 `AGENTS_local.md`；它只记录当前本地环境事实，不应当作跨机器通用规范。遇到命令异常时，先区分是代码问题还是本地环境问题，再决定是否修改源码。

## 项目结构与模块组织
根目录是一个 Rust workspace，成员包括：`home-mixer/`（Feed 编排主服务）、`thunder/`（实时帖子缓存与 Kafka 消费）、`candidate-pipeline/`（候选流水线抽象）、`proto/`（`proto/definitions/*.proto` 与 Rust 桩代码生成）、`vm-ranker/`（可选二次重排，默认关闭）。`phoenix/` 是独立子项目（演示用 Python/JAX 链路 + 生产用 `xrex/`/`crates/` 引擎，不并入根 workspace），入口脚本在 `phoenix/scripts/`，测试在 `phoenix/tests/`，中文操作文档在 `phoenix/docs/`。`grox/` 是独立 Python 包，不进主推荐链。仓库级文档在 `docs/`（跑通教程在 `docs/getting-started/`，历史记录在 `docs/archive/`）。`target/`、`*/target/` 都是构建产物，不要直接编辑。

## 构建、测试与开发命令
- `cargo build --workspace`：编译所有 Rust crate。
- `cargo test --workspace`：运行 Rust 测试；当前更常用于保证改动至少可编译、可链接。
- `cargo run -p thunder -- --demo-seed-posts 200 --grpc-port 50052`：以演示模式启动 `thunder`（无 Kafka）；不带参数则进入 Kafka 消费模式。
- `cargo run -p home-mixer`：启动 `home-mixer` 二进制（演示模式需设 `HOME_MIXER_MODE=demo` 等环境变量，见 `docs/getting-started/05-第四步-跑通完整推荐链路.md`）。
- `cargo run -p home-mixer --bin demo-client`：请求一次推荐 Feed 并打印结果。
- `cargo run -p home-mixer --features kafka --bin uas-worker`：启动 UAS 行为序列投影 job（Kafka 消费需 `kafka` feature；不带 feature 时只支持 stdin 换行 JSON；`kafka-ssl` feature 额外链接 OpenSSL，SSL / SASL_SSL 才可用）。
- `docker build -f deploy/docker/home-mixer.Dockerfile -t home-mixer:dev .` 与 `docker build -f deploy/docker/phoenix-gateway.Dockerfile -t phoenix-gateway:dev phoenix`：构建两份容器镜像（分别以仓库根和 `phoenix/` 为上下文）。
- `cargo test -p home-mixer --test redis_feed_state --test redis_uas -- --ignored`：需要本机 `redis-server` 的 Redis 适配器集成测试。
- `./scripts/run_demo.sh`：一键跑通端到端演示链路。
- `cargo fmt --all` 和 `cargo clippy --workspace --all-targets`：格式化与静态检查。
- `cd phoenix && uv sync --dev --group service`：安装演示推理、测试与 gRPC 服务依赖。
- `cd phoenix && uv run scripts/run_ranker.py`：运行排序模型示例。
- `cd phoenix && uv run scripts/run_retrieval.py`：运行检索模型示例。
- `cd phoenix && uv run scripts/train_ranker.py`：训练精排模型。
- `cd phoenix && uv run scripts/train_retrieval.py`：训练召回模型。
- `cd phoenix && uv run scripts/run_grpc_gateway.py`：启动供 home-mixer 调用的 gRPC 模型服务。
- `cd phoenix && uv run pytest`：运行 Python 测试（测试位于 `phoenix/tests/`）。

## 编码风格与命名约定
Rust 使用 Edition 2021，遵循 `rustfmt` 默认风格，使用 4 空格缩进。模块、文件、函数使用 `snake_case`，类型和 trait 使用 `CamelCase`，例如 `PhoenixScorer`、`Source`。Python 代码遵循 `phoenix/pyproject.toml`：4 空格缩进、行宽 100，优先使用 `ruff` 兼容写法。新增文件名应与现有目录保持一致，例如过滤器放在 `filters/*_filter.rs`。

## 测试指南
`phoenix/` 使用 `pytest`，现有测试以 `test_*.py` 命名，覆盖张量形状、attention mask、检索、gRPC 契约和策略装配。`home-mixer`、`candidate-pipeline`、`vm-ranker` 已有成体系 Rust 单测；`thunder` 仍偏少。修改 pipeline、proto 或服务装配时，至少运行 `cargo test --workspace`。如果改动影响排序、过滤或协议字段，补充最接近变更点的单测或回归测试。

## 提交与 Pull Request 规范
提交历史已采用稳定的简洁 scope 前缀（`home-mixer: ...`、`phoenix: ...`、`thunder: ...`、`docs: ...`），新提交请沿用，例如 `home-mixer: add author diversity scorer`、`phoenix: fix retrieval normalization`。PR 需说明影响的模块、行为变化、验证命令和结果；若改动涉及接口、排序输出或文档图示，附示例输出或截图，并链接相关 issue / 设计文档。

## 配置与安全提示
协议定义优先修改 `proto/definitions/`，不要手改生成代码。涉及 Kafka、gRPC 或外部依赖的改动，应把配置入口放在显式参数或配置结构中，避免把环境相关值硬编码到源码里。
