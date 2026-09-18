# Repository Guidelines

## Agent 使用说明
本文件描述仓库通用约定。若任务涉及本机工具链、权限、缓存目录、Homebrew 路径或沙箱行为，请按需同时查看 `AGENTS_local.md`；它只记录当前本地环境事实，不应当作跨机器通用规范。遇到命令异常时，先区分是代码问题还是本地环境问题，再决定是否修改源码。

## 项目结构与模块组织
`phoenix/` 只保留生产引擎（Linux + CUDA）：`xrex/`、`crates/`、本地 `python/` 包，以及真实事件输入和 SID/MM 工具。旧演示链路已删除。

中文操作文档在 `docs/`，中英文背景说明见 `README.md` 与 `README_zh.md`。测试在 `tests/`，示例在 `examples/`。

## 构建、测试与开发命令
使用 `uv` 管理环境与依赖。

- `uv sync --extra engine --dev`：安装生产引擎与测试依赖。
- `uv run pytest`：运行全部保留的生产与合同测试。
- `uv run pytest tests/engine`：运行 xrex 模型/配置测试。
- `uv run scripts/build_training_inputs.py ...`：将真实 served-candidates/UAS 事件写成训练输入。
- `uv run ty check`：做一次基础静态类型检查。

## 编码风格与命名约定
遵循 `pyproject.toml` 中的约定：4 空格缩进，行宽 100。模块、函数、变量使用 `snake_case`，类名使用 `CamelCase`。新增入口脚本放在 `scripts/`（如 `scripts/run_*.py`），新增测试放在 `tests/`（如 `tests/test_*.py`），新增示例/工具放在 `examples/`。优先写清晰的张量维度与掩码语义，只有在复杂逻辑前添加简短注释。

## 测试指南
测试框架为 `pytest`，测试文件统一放在 `tests/`；xrex 导入和协议耦合测试放 `tests/engine/`。生产模型改动应覆盖输入形状、输出维度和关键协议约束。

## 提交与 Pull Request 规范
根仓库提交历史已采用稳定的简洁 scope 前缀（phoenix 相关改动统一用 `phoenix: ...`），新提交请沿用，例如 `phoenix: fix candidate mask`、`phoenix: normalize item embeddings`。PR 需要说明修改动机、影响模块、验证命令及结果；若改动影响模型输出或流程图，请同步更新 `README.md` 或 `ARCHITECTURE.md`。

## 配置与依赖提示
依赖版本由 `pyproject.toml` 与 `uv.lock` 管理，不要手动漂移核心库版本，尤其是 `jax` 与 `dm-haiku`。新增配置优先通过显式参数传递，避免把环境相关常量硬编码进模型代码。
