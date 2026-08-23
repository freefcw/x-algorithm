# Local Environment Notes

## 适用范围
这个文件只描述当前这台机器和当前 Codex 工作环境的事实，用来帮助 agent 快速判断“命令失败是否由本地环境导致”。仓库通用规范仍以 `AGENTS.md` 为准；只有在执行命令、安装依赖、排查权限/路径/版本问题时才需要引入本文件。

## 本机基础环境
- 系统：macOS `arm64`，`Darwin 25.x`
- Shell：`zsh 5.9`
- Rust：`rustc 1.94.1`，`cargo 1.94.1`
- `protoc` 已安装：`/opt/homebrew/bin/protoc`

## Python 与 `uv`
- 系统 `python3` 是 `3.9.6`，不要假设它满足 `phoenix/pyproject.toml` 的 `>=3.11` 要求。
- `phoenix` 相关命令优先使用 `uv run ...`，不要直接用根环境的 `python3` 跑脚本或测试。
- 当前环境下 `UV_CACHE_DIR=/tmp/uv-cache uv run python --version` 可得到 `Python 3.12.13`，说明 `uv` 能提供满足要求的解释器。

## 当前已知坑
- 在当前 Codex 沙箱里，直接运行 `uv run ...` 可能因默认缓存目录 `~/.cache/uv` 权限不足而失败，典型报错是 `Operation not permitted`。
- 出现这个问题时，优先改用：
  - `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run pytest ...`
  - `cd phoenix && UV_CACHE_DIR=/tmp/uv-cache uv run scripts/run_ranker.py`
- 这属于本地权限/沙箱问题，不是仓库代码本身的问题。

## 已验证事实
- `cargo build -p x-algorithm-proto` 在当前环境可直接成功。
- `proto/build.rs` 依赖本机 `protoc`；当前机器已满足该前提。
- `phoenix` Rust workspace 的 pyo3 crate（xai-recsys-engine、xai-recsys-mm-server）默认链接系统 Python 3.9 framework 桩，测试二进制在 macOS 上因 `no LC_RPATH` 无法执行（dyld 加载期失败）。解决办法：`cd phoenix && PYO3_PYTHON=$PWD/.venv/bin/python3 cargo test --workspace`，重链到 uv 管理的 Python 3.12 后 116 项测试全部通过（2026-08-21 验证）。
- `xai-recsys-engine` 的 Python 绑定可在本机构建：`cd phoenix/crates/serving/xai-recsys-engine && VIRTUAL_ENV=<repo>/phoenix/.venv PYO3_PYTHON=<repo>/phoenix/.venv/bin/python3 uvx maturin develop`（release 构建约 2-3 分钟）。构建后 `import xai_recsys_engine` 及 `xrex.train.trainer_recsys`、`xrex.configs.xrecsys`、`xrex.inference.model_runner` 整条导入链全部打通（2026-08-23 验证）。绑定只装进本地 venv（editable 安装），不写入仓库；重建 venv 后需重跑。

## 额外说明
- 如果上层指令提到 `@RTK.md`，不要默认认为仓库根目录一定存在这个文件；当前仓库内未找到 `RTK.md`。若引用来自外层会话配置，以外层注入内容为准，不要在仓库里反复查找。
