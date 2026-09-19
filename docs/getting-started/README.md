# 当前代码验证入口

> **状态：`current-code`**

本目录不再提供本地 Demo、随机权重模型、旧 gRPC gateway 或端到端启动脚本。旧章节已删除，避免把不存在的命令当成当前操作手册。

## Phoenix 生产引擎

在 `phoenix/` 目录执行：

```bash
uv sync --extra engine --dev
uv run pytest
uv run pytest tests/engine
```

生产训练、真实数据、checkpoint、ranking/retrieval 服务和上线边界以以下文档为准：

- [训练与数据](../phoenix/06-training-and-data.md)
- [真实数据接入](../phoenix/07-real-data-integration.md)
- [生产上线手册](../phoenix/08-production-handbook.md)
- [Phoenix 项目 README](../../phoenix/README.md)

这些入口要求真实事件输入、checkpoint 和部署配置；没有配置时应失败，不能启动随机模型。

## 根 Rust workspace

在仓库根目录执行：

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
```

Home Mixer 的外部依赖、配置合同和部署边界见 [bootstrap](../bootstrap/)、[home-mixer 文档](../home-mixer/) 和 [Kubernetes 说明](../../deploy/k8s/README.md)。当前没有可执行的“完整推荐 Demo”入口。
