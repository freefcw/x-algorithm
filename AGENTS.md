# Repository Guidelines

## Agent 使用说明
本文件描述仓库通用约定。若任务涉及本机工具链、权限、缓存目录、Homebrew 路径或沙箱行为，请按需同时查看 `AGENTS_local.md`；它只记录当前本地环境事实，不应当作跨机器通用规范。遇到命令异常时，先区分是代码问题还是本地环境问题，再决定是否修改源码。

## 项目结构与模块组织
根目录是一个 Rust workspace，核心 crate 包括：`home-mixer/`（Feed 编排主服务）、`thunder/`（实时帖子缓存与 Kafka 消费）、`candidate-pipeline/`（候选流水线抽象）、`proto/`（`proto/definitions/*.proto` 与 Rust 桩代码生成）。`phoenix/` 是独立的 Python 3.11/JAX 模型子项目，包含检索、排序与对应测试。设计说明和迁移记录放在 `docs/`。`target/`、`*/target/` 都是构建产物，不要直接编辑。

## 构建、测试与开发命令
- `cargo build --workspace`：编译所有 Rust crate。
- `cargo test --workspace`：运行 Rust 测试；当前更常用于保证改动至少可编译、可链接。
- `cargo run -p thunder`：启动 `thunder` 二进制。
- `cargo run -p home-mixer`：启动 `home-mixer` 二进制。
- `cargo fmt --all` 和 `cargo clippy --workspace --all-targets`：格式化与静态检查。
- `cd phoenix && uv run run_ranker.py`：运行排序模型示例。
- `cd phoenix && uv run run_retrieval.py`：运行检索模型示例。
- `cd phoenix && uv run pytest test_recsys_model.py test_recsys_retrieval_model.py`：运行 Python 测试。

## 编码风格与命名约定
Rust 使用 Edition 2021，遵循 `rustfmt` 默认风格，使用 4 空格缩进。模块、文件、函数使用 `snake_case`，类型和 trait 使用 `CamelCase`，例如 `PhoenixScorer`、`Source`。Python 代码遵循 `phoenix/pyproject.toml`：4 空格缩进、行宽 100，优先使用 `ruff` 兼容写法。新增文件名应与现有目录保持一致，例如过滤器放在 `filters/*_filter.rs`。

## 测试指南
`phoenix/` 使用 `pytest`，现有测试以 `test_*.py` 命名，重点覆盖张量形状、attention mask 和检索行为。Rust 侧暂未看到成体系单测；修改 pipeline、proto 或服务装配时，至少运行 `cargo test --workspace`。如果改动影响排序、过滤或协议字段，补充最接近变更点的单测或回归测试。

## 提交与 Pull Request 规范
当前 Git 历史几乎只有初始化提交，尚未形成稳定提交规范。建议采用简洁的 scope 前缀：`home-mixer: add author diversity scorer`、`phoenix: fix retrieval normalization`。PR 需说明影响的模块、行为变化、验证命令和结果；若改动涉及接口、排序输出或文档图示，附示例输出或截图，并链接相关 issue / 设计文档。

## 配置与安全提示
协议定义优先修改 `proto/definitions/`，不要手改生成代码。涉及 Kafka、gRPC 或外部依赖的改动，应把配置入口放在显式参数或配置结构中，避免把环境相关值硬编码到源码里。
