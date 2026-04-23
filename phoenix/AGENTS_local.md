# Local Environment Notes

## 适用范围
本文件只记录当前机器和当前代理运行环境的事实，用来帮助排查“命令跑不通是否只是本地环境问题”。不要把这里的路径、权限或工具位置当作仓库规范提交到别的机器上复用。

## 当前环境事实
- 操作系统：`Darwin arm64`（Apple Silicon macOS）。
- Shell：`zsh`。
- 当前仓库路径：`/Users/hejun/work/mp/x-algorithm/phoenix`。
- 当前时区：`Asia/Shanghai`。

## Python 与工具链
- 仓库内已存在虚拟环境，当前 `python3` 解析到 `./.venv/bin/python3`。
- `python3 --version` 当前为 `3.12.13`；仓库声明的最低版本是 `>=3.11`。
- `uv` 路径为 `/opt/homebrew/bin/uv`。
- `brew` 路径为 `/opt/homebrew/bin/brew`，说明 Homebrew 前缀是 `/opt/homebrew`，不是 Intel 机器常见的 `/usr/local`。
- 运行项目命令优先使用 `uv run ...`，不要假设系统 Python 或全局包可用。

## 沙箱与权限
- 当前代理文件系统沙箱为 `workspace-write`：可读仓库，可写仓库与少量允许目录。
- 网络默认受限；需要下载依赖、访问外网或写出工作区外路径时，通常需要额外授权。
- 可写根目录包括当前仓库、`/tmp`、`/var/folders/.../T` 以及 `/Users/hejun/.codex/memories`。

## 排查建议
- 命令失败时，先确认是不是路径、权限、网络或虚拟环境问题，再判断是否需要改代码。
- 如果命令里写死 `/usr/local/...`、全局 `python`、全局 `pip`，优先改成 `uv run` 或 `/opt/homebrew/...` 兼容写法。
