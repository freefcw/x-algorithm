# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
数据预处理脚本（多进程并行版）

按天分片并行处理行为日志，复用 `data_preprocessor.py` 中的全部核心函数。
与原版（单进程）相比只改调度层，采样/编码/样本结构完全一致。

使用示例：
    # 自动扫描所有 dt=* 目录，8 进程并行
    uv run data_preprocessor_mp.py \
        --behavior-dir data/real_data/behavior_logs \
        --post-meta data/real_data/post_metadata.parquet \
        --output-dir data/training_samples \
        --workers 8

    # 只处理指定若干天
    uv run data_preprocessor_mp.py \
        --behavior-dir data/real_data/behavior_logs \
        --post-meta data/real_data/post_metadata.parquet \
        --output-dir data/training_samples \
        --dates 2026-04-13,2026-04-14 \
        --workers 2

输出：
    output_dir/train_YYYYMMDD.parquet（每天一个文件）

对比方法：
    # 原版顺序处理
    time uv run data_preprocessor.py --behavior-dir ... --output-dir /tmp/seq/

    # 多进程处理
    time uv run data_preprocessor_mp.py --behavior-dir ... --output-dir /tmp/mp/ --workers 8

    # 对比输出一致性（样本数、schema、正样本 label 等）
"""

import argparse
import logging
import multiprocessing as mp
import os
import time
from pathlib import Path

# 复用原版的全部核心函数
from data_preprocessor import (
    MAX_AGE_DAYS,
    load_behavior_logs,
    load_post_metadata,
    process_single_day,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("preprocess_mp")


def discover_dates(behavior_dir: str) -> list[str]:
    """扫描 `dt=YYYY-MM-DD` 子目录，返回排序后的日期列表。"""
    base = Path(behavior_dir)
    dates: list[str] = []
    for child in base.iterdir():
        if child.is_dir() and child.name.startswith("dt="):
            dates.append(child.name[len("dt=") :])
    dates.sort()
    return dates


def _worker(args):
    """
    单个 worker 进程的入口：处理某一天的数据。

    必须是 module-level 函数（而非 lambda/嵌套）才能被 pickle 传给子进程。
    每个进程独立加载 post_meta 与 behavior_df，避免跨进程传输大 DataFrame。
    """
    date_str, behavior_dir, post_meta_path, output_dir, neg_ratio, seed, max_age_days = args

    # 子进程独立 logger 前缀，便于观察并行进度
    worker_logger = logging.getLogger(f"worker[{date_str}]")
    t_start = time.perf_counter()
    worker_logger.info(f"pid={os.getpid()} 开始处理")

    try:
        post_meta_df = load_post_metadata(post_meta_path)
        behavior_df = load_behavior_logs(behavior_dir, date_str)

        output_path = Path(output_dir) / f"train_{date_str.replace('-', '')}.parquet"

        process_single_day(
            behavior_df=behavior_df,
            post_meta_df=post_meta_df,
            output_path=str(output_path),
            neg_sample_ratio=neg_ratio,
            seed=seed,
            max_age_days=max_age_days,
        )

        elapsed = time.perf_counter() - t_start
        worker_logger.info(
            f"完成，耗时 {elapsed:.1f}s，输出 {output_path}"
        )
        return {"date": date_str, "ok": True, "elapsed": elapsed, "output": str(output_path)}
    except Exception as e:
        elapsed = time.perf_counter() - t_start
        worker_logger.exception("失败")
        return {"date": date_str, "ok": False, "elapsed": elapsed, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="数据预处理（多进程并行版）")
    parser.add_argument(
        "--behavior-dir",
        type=str,
        required=True,
        help="行为日志根目录（包含 dt=YYYY-MM-DD 子目录）",
    )
    parser.add_argument(
        "--post-meta",
        type=str,
        required=True,
        help="帖子元数据 Parquet 文件路径",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/training_samples",
        help="输出目录",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="逗号分隔的日期列表，如 2026-04-13,2026-04-14。默认自动扫描所有 dt=* 目录",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行进程数。0 表示 min(天数, cpu_count())",
    )
    parser.add_argument(
        "--neg-ratio",
        type=int,
        default=7,
        help="负样本比例（默认 7，即 1:7）",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="随机种子（每天 + 每用户会派生确定性子种子）",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_AGE_DAYS,
        help="负样本只从事件时刻前 N 天内发布的帖子中采样（默认 7，与线上过滤一致）",
    )

    args = parser.parse_args()

    # 确定待处理日期
    if args.dates:
        dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    else:
        dates = discover_dates(args.behavior_dir)

    if not dates:
        logger.error(f"在 {args.behavior_dir} 下未找到任何 dt=* 目录")
        return

    # 确定并行度
    if args.workers <= 0:
        workers = min(len(dates), os.cpu_count() or 4)
    else:
        workers = min(args.workers, len(dates))

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    logger.info("=== 数据预处理（多进程）开始 ===")
    logger.info(f"待处理日期: {dates}")
    logger.info(f"并行进程数: {workers}")

    # 组装 worker 参数
    task_args = [
        (
            d,
            args.behavior_dir,
            args.post_meta,
            args.output_dir,
            args.neg_ratio,
            args.seed,
            args.max_age_days,
        )
        for d in dates
    ]

    t_start = time.perf_counter()

    # 使用 spawn 启动方法避免 macOS fork 在有 numpy/pandas 后的潜在死锁
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=workers) as pool:
        results = pool.map(_worker, task_args)

    total_elapsed = time.perf_counter() - t_start

    # 汇总
    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    logger.info("=== 数据预处理（多进程）完成 ===")
    logger.info(
        f"总耗时（wall clock）: {total_elapsed:.1f}s，"
        f"成功 {ok_count}/{len(results)}，失败 {fail_count}"
    )
    # 打印每天耗时明细，便于对比最慢一天和并行效率
    logger.info("—— 各日耗时明细 ——")
    for r in sorted(results, key=lambda x: x["date"]):
        status = "OK  " if r["ok"] else "FAIL"
        extra = r.get("output", r.get("error", ""))
        logger.info(f"  [{status}] {r['date']}: {r['elapsed']:.1f}s  {extra}")

    cpu_time_sum = sum(r["elapsed"] for r in results)
    if total_elapsed > 0:
        speedup = cpu_time_sum / total_elapsed
        logger.info(
            f"CPU 时间总和: {cpu_time_sum:.1f}s，并行加速比: {speedup:.2f}x "
            f"(理论上限 = workers = {workers})"
        )

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
