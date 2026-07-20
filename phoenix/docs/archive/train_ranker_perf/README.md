# 精排训练脚本性能优化记录

> 本目录沉淀一次针对 `scripts/train_ranker.py` 的性能治理：把原本**每步数据侧 O(文件大小)**、**每步阻塞同步**、**无法跑全量数据**的训练脚本，改造为**稳态 24 ms/step、内存与数据集大小脱钩、可在 29 M 样本上训练**的形态。
>
> 目标读者：后续维护 `train_ranker.py`、排查训练吞吐 / 内存问题、或想了解 JAX 训练循环性能工程套路的工程师。
>
> 版本：1.0（2026-04-24）

---

## TL;DR

- **发现**：`scripts/train_ranker.py` 在真实数据上又慢又容易无故"静默退出"。
- **根因**：一是数据加载路径极度低效（每文件只取 `batch_size` 行 + `to_pydict` + Python 嵌套 list → numpy）；二是每步 `float(loss_val)` 阻塞同步；三是 CPU 上做嵌入表查表然后再 host→device 拷贝；四是 29 M 样本的全量数据根本塞不进内存（~109 GB），导致 macOS 内核 OOM 静默杀进程（exit code 仍为 0）。
- **落地**：
  1. Parquet 加载改成"一次读目录 + flatten/reshape"，并把查表放进 `@jax.jit` 的 `train_step` 内部；
  2. loss 改为在 device 上累加，只在日志步同步；
  3. 向量化 `make_simulated_batch`；
  4. 新增 `--streaming` 模式，用 shuffle buffer 支持任意大小数据集；
  5. 新增 `--benchmark` 开关给出稳态 step 时间。
- **实测**（batch_size=64，macOS aarch64）：三种数据源（模拟 / in-memory / streaming）稳态 step 都在 **24–26 ms**，`train_step` 自身成为新瓶颈，数据侧开销接近零。

---

## 文档导航

| 文档 | 主题 |
|---|---|
| [01-背景与瓶颈分析.md](./01-背景与瓶颈分析.md) | 起因、现象、对原脚本的逐点代码级瓶颈审计 |
| [02-优化方案与实现.md](./02-优化方案与实现.md) | 方案选型、关键代码改动、为什么这么改 |
| [03-OOM与流式训练.md](./03-OOM与流式训练.md) | 真实数据 OOM 事故复盘、`--streaming` 模式设计 |
| [04-基准测试与效果.md](./04-基准测试与效果.md) | `--benchmark` 开关、实测数据、下一步建议 |
| [05-使用指南.md](./05-使用指南.md) | CLI 速查、场景示例、FAQ |

## 推荐阅读顺序

- **想快速上手**：先读 [05-使用指南](./05-使用指南.md)。
- **想搞懂为什么这么改**：按 01 → 02 → 03 → 04 顺序看。
- **做后续性能工作**：重点看 [04-基准测试与效果.md](./04-基准测试与效果.md) 末尾的 "下一步建议"。

## 主要代码落点

- `@/Users/hejun/work/mp/x-algorithm/phoenix/scripts/train_ranker.py` — 所有改动集中在这一个文件里。
- 没有新增依赖，`pyproject.toml` / `uv.lock` 不变。
