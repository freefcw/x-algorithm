# Repository Guidelines

## Agent 使用说明
本文件描述仓库通用约定。若任务涉及本机工具链、权限、缓存目录、Homebrew 路径或沙箱行为，请按需同时查看 `AGENTS_local.md`；它只记录当前本地环境事实，不应当作跨机器通用规范。遇到命令异常时，先区分是代码问题还是本地环境问题，再决定是否修改源码。

## 项目结构与模块组织
`phoenix/` 里有两套代码，不要混用依赖组。

- **演示链路**（getting-started / recommendation-service）：根目录 `recsys_model.py`、`recsys_retrieval_model.py`、`grok.py`、`runners.py`、`data_preprocessor.py`，入口在 `scripts/`，服务在 `services/`。Python ≥ 3.11。安装：`uv sync --dev --group service`。架构说明见 `ARCHITECTURE.md`。
- **生产引擎**（Linux + CUDA）：`xrex/`、`crates/`。安装：`uv sync --extra engine`。入口见 `README.md` 和 `QUICKSTART.md`。

中文操作文档在 `docs/`，中英文背景说明见 `README.md` 与 `README_zh.md`。测试在 `tests/`，示例在 `examples/`。

## 构建、测试与开发命令
使用 `uv` 管理环境与依赖。

- `uv sync --dev --group service`：安装运行、测试与服务依赖（含 grpcio）。
- `uv run scripts/run_ranker.py`：运行精排模型示例。
- `uv run scripts/run_retrieval.py`：运行召回模型示例。
- `uv run scripts/train_ranker.py`：训练精排模型。
- `uv run scripts/train_retrieval.py`：训练召回模型。
- `uv run scripts/run_services.py all`：启动精排/召回 HTTP 服务（8081/8082）。
- `uv run scripts/run_grpc_gateway.py`：启动供 recommendation-service 调用的 gRPC 网关（50053）。
- `uv run pytest`：运行演示链路全部 Python 测试（默认不收集 `tests/engine/`）。
- `uv run pytest tests/engine`：运行生产引擎（xrex）侧测试；这些测试会导入 `xai_proto`，与演示链路分进程跑。
- `uv run pytest tests/test_recsys_model.py`：仅验证精排相关改动。
- `uv run ty check`：做一次基础静态类型检查。

## 编码风格与命名约定
遵循 `pyproject.toml` 中的约定：4 空格缩进，行宽 100。模块、函数、变量使用 `snake_case`，类名使用 `CamelCase`。新增入口脚本放在 `scripts/`（如 `scripts/run_*.py`），新增测试放在 `tests/`（如 `tests/test_*.py`），新增示例/工具放在 `examples/`。优先写清晰的张量维度与掩码语义，只有在复杂逻辑前添加简短注释。

## 测试指南
测试框架为 `pytest`，测试文件统一放在 `tests/` 目录。演示链路测试直接放 `tests/`；任何会导入 `xrex` 模型/数据模块（进而顶层导入 `xai_proto`）的测试放 `tests/engine/`，或像 `tests/test_checkpoint_imports.py` 那样在子进程里跑。演示链路与 `xai_proto` 的隔离由 `tests/test_chain_isolation.py` 守门。涉及模型结构、mask、shape 或检索打分逻辑的改动时，至少补充对应 `tests/test_*.py` 用例。新增测试应覆盖输入张量形状、前向输出维度以及关键行为约束，例如"候选之间不可互相注意"。提交前至少运行受影响测试，较大改动运行 `uv run pytest`。

## 提交与 Pull Request 规范
根仓库提交历史已采用稳定的简洁 scope 前缀（phoenix 相关改动统一用 `phoenix: ...`），新提交请沿用，例如 `phoenix: fix candidate mask`、`phoenix: normalize item embeddings`。PR 需要说明修改动机、影响模块、验证命令及结果；若改动影响模型输出或流程图，请同步更新 `README.md` 或 `ARCHITECTURE.md`。

## 配置与依赖提示
依赖版本由 `pyproject.toml` 与 `uv.lock` 管理，不要手动漂移核心库版本，尤其是 `jax` 与 `dm-haiku`。新增配置优先通过显式参数传递，避免把环境相关常量硬编码进模型代码。
