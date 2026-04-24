# 03 — OOM 事故与流式训练

> 这一篇是本次优化中**第一档改造落地后**才暴露的问题。
> 关键词：进程静默退出、macOS OOM、全量 29 M 样本、shuffle buffer。

---

## 1. 现象

第一档优化（`load_parquet_dataset` 一次读入 + flatten/reshape）跑模拟模式 OK，但用真实数据跑起来后日志总是断档在同一个位置：

```
2026-04-24 15:33:41,247 INFO 找到 9 个 Parquet 文件，开始加载...
---DONE---
       0 /tmp/train_smoke.log
```

特征：
- **没有任何异常栈**；
- `exit code = 0`；
- 没有后续的 "共加载 N 条样本" / "开始训练循环"。

第一反应是"log flush 问题"，但切换了 `-u` / 显式 `flush=True` 现象依旧。

---

## 2. 根因

两步排查：

### 2.1 Parquet schema 有老文件遗留

先跑了一遍 schema 探测：

| 文件 | 行数 | schema |
|---|---|---|
| `train_20240101.parquet` | 780 | **int64 / double**（旧版） |
| `train_20260413.parquet` | 276,219 | int32 / float32 |
| ... | ... | ... |
| `train_20260419.parquet` | 6,039,864 | int32 / float32 |

老文件的 schema 与 `data_preprocessor.py:491-505` 里声明的不一致。对加载路径的影响是 `astype(np.int32)` 会做一次 copy，内存瞬间 ×2。不是主因，但放大了主因。

### 2.2 数据量远超内存

所有文件合计约 **28.7 M 行**。紧凑 numpy 下每行 ≈ 3.8 KB：

```
28.7 M × 3.8 KB ≈ 109 GB
```

一次性 `pa.concat_tables` 再展成 numpy 无论如何都装不下。

macOS 内核在 OOM 时会直接 SIGKILL，**Python 进程 exit code 仍然是 0**，日志里看不到任何错误，只能看到最后一条 log 恰好卡在 "开始加载..." 这种给"表示即将大分配"的位置。

> 这是 macOS + Python 下非常容易误诊的一种失败模式。**只要 exit 0 + 日志突然断档 + 后续步骤没跑**，先怀疑 OOM。

---

## 3. 第一轮修复：`--max-samples` + 分块流式读取（仍然 in-memory）

先把 in-memory 路径自身改得更稳健：

### 3.1 用 `iter_batches` 取代一次性 `read_table`

```@/Users/hejun/work/mp/x-algorithm/phoenix/scripts/train_ranker.py:337-354
        for rb in pf.iter_batches(
            batch_size=row_group_batch_size,
            columns=list(_PARQUET_COLUMN_SPEC.keys()),
        ):
            take = rb.num_rows
            if max_samples is not None:
                remain = max_samples - total_rows
                if remain <= 0:
                    break
                if take > remain:
                    rb = rb.slice(0, remain)
                    take = remain
            tbl = pa.Table.from_batches([rb])
            for name, (tail_shape, dtype) in _PARQUET_COLUMN_SPEC.items():
                chunks[name].append(_col_to_numpy(tbl[name], tail_shape, dtype))
            total_rows += take
            del tbl, rb
        logger.info(f"  已加载 {f.name}，累计 {total_rows} 行")
```

要点：
- 每次只解压 `row_group_batch_size=8192` 行到内存；
- 立刻 `_col_to_numpy` 压成紧凑 int32/float32；
- `del tbl, rb` 主动释放 arrow 引用；
- peak memory 稳定在一个 chunk 的规模。

### 3.2 新增 `--max-samples` / `--max-files`

```@/Users/hejun/work/mp/x-algorithm/phoenix/scripts/train_ranker.py:679-686
    parser.add_argument(
        "--max-samples", type=int, default=None,
        help="in-memory 模式下 Parquet 最多加载的样本行数（防 OOM / 快速试跡）",
    )
    parser.add_argument(
        "--max-files", type=int, default=None,
        help="只读 Parquet 目录下排序后前 N 个文件",
    )
```

现在即便用全量目录做实验也不怕了：`--max-samples 100000` 就截到 10 万行为止。

---

## 4. 第二轮修复：streaming 模式，真正支持全量

即便 in-memory 分块读取，仍然"最后全部装入内存"，不适合 29 M × 3.8 KB = 109 GB 的场景。下一步必须上**流式训练**。

### 4.1 设计目标

| 项目 | 要求 |
|---|---|
| 内存占用 | `O(shuffle_buffer)`，与数据集总大小无关 |
| Shuffle 粒度 | 至少 10 k 样本级别，接近 TF `tf.data.shuffle(buffer_size)` |
| 支持多 epoch | 读完目录自动回到开头 |
| 支持 `--max-files` | 小范围调试时能限制 |
| 数据类型 | 仍然产出与 `iterate_parquet_dataset` 形状一致的 `(RecsysBatch, labels)` |

### 4.2 策略：chunk → shuffle buffer → batch

```
parquet 文件 → iter_batches(row_group_batch_size=8192) → _col_to_numpy → chunk (np)
   累积 chunks 到 shuffle_buffer_size (默认 16 384) →
     np.random.permutation → 按 batch_size 切 → yield batch (每轮都是全局打乱过的)
   不满 batch 的残留行转入下一轮 buffer，避免丢数据
```

### 4.3 核心实现

```@/Users/hejun/work/mp/x-algorithm/phoenix/scripts/train_ranker.py:441-488
    def _chunk_iter():
        """无限产出每个 chunk 的紧凑 numpy 字典。"""
        epoch = 0
        while True:
            epoch += 1
            for f in files:
                try:
                    pf = pq.ParquetFile(str(f))
                except Exception as e:
                    logger.warning(f"跳过 {f}：{e}")
                    continue
                for rb in pf.iter_batches(
                    batch_size=row_group_batch_size,
                    columns=list(_PARQUET_COLUMN_SPEC.keys()),
                ):
                    tbl = pa.Table.from_batches([rb])
                    chunk = {
                        name: _col_to_numpy(tbl[name], tail, dtype)
                        for name, (tail, dtype) in _PARQUET_COLUMN_SPEC.items()
                    }
                    del tbl, rb
                    yield chunk
            logger.info(f"[streaming] 完成第 {epoch} 轮遍历，继续下一轮 epoch")

    chunks: dict[str, list[np.ndarray]] = {name: [] for name in _PARQUET_COLUMN_SPEC}
    in_buffer = 0
    for chunk in _chunk_iter():
        size = len(chunk["user_hashes"])
        for name in chunks:
            chunks[name].append(chunk[name])
        in_buffer += size
        if in_buffer < shuffle_buffer_size:
            continue
        # buffer 满，一次性全排列 + 切 batch
        merged = {name: np.concatenate(arrs, axis=0) for name, arrs in chunks.items()}
        perm = rng.permutation(in_buffer)
        n_full = (in_buffer // batch_size) * batch_size
        for start in range(0, n_full, batch_size):
            yield _make_batch_labels(merged, perm[start:start + batch_size])
        # 不满 batch 的残留行留给下一轮，避免丢数据
        leftover = in_buffer - n_full
        if leftover > 0:
            tail_sl = perm[n_full:]
            chunks = {name: [merged[name][tail_sl]] for name in merged}
            in_buffer = leftover
        else:
            chunks = {name: [] for name in chunks}
            in_buffer = 0
```

### 4.4 内存占用

默认 `shuffle_buffer_size=16384`：
- 16384 × 3.8 KB ≈ **62 MB** 的 shuffle buffer；
- 加上 pyarrow 的 chunk + 少量临时数组，整体稳定在 **~150 MB** 以内。

即便你把 shuffle_buffer 调到 1 M 行也只是 ~3.8 GB，仍然远低于 109 GB。

### 4.5 权衡：shuffle 粒度 vs 内存

| Shuffle 模式 | 粒度 | 内存 | 对收敛的影响 |
|---|---|---|---|
| 全局（in-memory） | N = 数据集总行数 | O(dataset) | 最好 |
| streaming 16k buffer | 16 k 行 | 62 MB | 接近全局，实践中 Transformer 训练基本无感 |
| streaming 128k buffer | 128 k 行 | 500 MB | 更接近全局 |
| 文件内 / chunk 内 | 8 k | 几十 MB | 明显不如上面（有时间相关性） |

默认 16 384 是一个"内存便宜 + shuffle 足够"的平衡点。需要时可以用 `--shuffle-buffer` 调。

---

## 5. 两种模式并存

改造完成后脚本提供两条数据路径：

```@/Users/hejun/work/mp/x-algorithm/phoenix/scripts/train_ranker.py:569-585
    # 6. 数据源：根据 --streaming 选择 in-memory 或流式迭代器
    if args.data_dir is not None:
        if args.streaming:
            data_iter = stream_parquet_batches(
                args.data_dir,
                batch_size=args.batch_size,
                shuffle_buffer_size=args.shuffle_buffer,
                max_files=args.max_files,
                seed=0,
            )
        else:
            dataset = load_parquet_dataset(
                args.data_dir,
                max_samples=args.max_samples,
                max_files=args.max_files,
            )
            data_iter = iterate_parquet_dataset(dataset, args.batch_size, shuffle=True, seed=0)
```

选择规则：

| 场景 | 建议 |
|---|---|
| 调试、跑 benchmark、样本 ≤ 100 k | in-memory + `--max-samples` |
| 全量训练（29 M 行） | `--streaming` |
| 不确定数据规模 | 默认 `--streaming` 总是安全 |

---

## 6. 数据治理建议（不在本次代码改动范围）

- `train_20240101.parquet` 已经在目录中被移除（格式为老 int64 schema，仅 780 行）。
- 如果再出现老格式文件，建议：
  - 用最新版 `data_preprocessor.py` 重新生成；
  - 或者以 schema 校验脚本把这类文件过滤掉。
- `_col_to_numpy` 的 `astype(..., copy=False)` 能兼容 int64 老格式，但会多一次 copy，不是长久之计。

---

下一篇 → [04-基准测试与效果.md](./04-基准测试与效果.md)
