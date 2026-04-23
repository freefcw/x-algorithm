# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
将 Kafka 导出的 feed 事件 CSV 转换为 phoenix 训练管线所需的 parquet 格式。

输入：
    feed_event.csv — 列: account_id, feed_id, author_id, event_time, event_type, event_source

输出（与 generate_example_data.py / data_preprocessor.py 保持一致）：
    - behavior_logs/dt=YYYY-MM-DD/*.parquet
    - post_metadata.parquet
    - user_metadata.parquet

设计说明：
    - 每条 CSV 行保留为一条独立事件行（不跨时间聚合），以保留完整时间线供
      data_preprocessor 构造历史序列。
    - 仅对同一秒内的同 (user, post) 事件做合并，模拟"同一次曝光/交互"。
    - vqv / dwell_time 等连续值字段：CSV 中仅有触发信号无实际数值，
      对应行为列设为 0（避免编造 label），仅在二值 flag 列标记触发。

使用示例：
    uv run feed_csv_importer.py --csv /path/to/feed_event.csv --output-dir data/real_data
    # 然后运行已有预处理:
    uv run data_preprocessor.py \\
        --behavior-dir data/real_data/behavior_logs \\
        --post-meta data/real_data/post_metadata.parquet \\
        --output-dir data/training_samples
"""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("feed_csv_importer")


# ── event_type → 行为字段映射 ──────────────────────────────────────────────────
# 顺序严格对齐 data_preprocessor.BEHAVIOR_FIELDS / runners.ACTIONS

EVENT_TYPE_TO_FIELD: Dict[int, str] = {
    0: "favorite",
    1: "reply",
    2: "repost",
    3: "photo_expand",
    4: "click",
    5: "profile_click",
    6: "vqv",
    7: "share",
    8: "share_via_dm",
    9: "share_via_copy_link",
    10: "dwell",
    11: "quote",
    12: "quoted_click",
    13: "follow_author",
    14: "not_interested",
    15: "block_author",
    16: "mute_author",
    17: "report",
    18: "dwell_time",
}

# 二值行为字段（int8 0/1）
BINARY_FIELDS: List[str] = [
    "favorite", "reply", "repost", "photo_expand", "click",
    "profile_click", "share", "share_via_dm", "share_via_copy_link",
    "dwell", "quote", "quoted_click", "follow_author",
    "not_interested", "block_author", "mute_author", "report",
]

# 连续值字段：CSV 中无实际数值，仅标记触发；值保持 0，避免编造 label
CONTINUOUS_FIELDS: List[str] = ["vqv", "dwell_time"]

ALL_BEHAVIOR_FIELDS: List[str] = BINARY_FIELDS + CONTINUOUS_FIELDS

# 列顺序与 generate_example_data.py 一致
OUTPUT_COLUMNS: List[str] = [
    "event_time", "user_id", "post_id", "author_id", "product_surface",
] + BINARY_FIELDS + CONTINUOUS_FIELDS

DEFAULT_CHUNK_SIZE = 500_000

# 预构建聚合字典（process_chunk 和跨 chunk 合并共用）
_AGG_DICT: Dict[str, str] = {"product_surface": "first"}
for _f in BINARY_FIELDS:
    _AGG_DICT[_f] = "max"
for _f in CONTINUOUS_FIELDS:
    _AGG_DICT[_f] = "max"

_GROUP_COLS: List[str] = ["user_id", "post_id", "author_id", "event_time"]


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def ms_to_seconds(ms_series: pd.Series) -> pd.Series:
    """将毫秒时间戳转为秒级 int64（向下取整）。"""
    return (ms_series // 1000).astype(np.int64)


def ts_to_date_str(ts_seconds: int) -> str:
    """秒级时间戳 → 'YYYY-MM-DD' 字符串（UTC）。"""
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime("%Y-%m-%d")


def vectorized_date_column(event_time_series: pd.Series) -> pd.Series:
    """向量化：秒级时间戳 Series → 'YYYY-MM-DD' 字符串 Series（UTC）。"""
    return pd.to_datetime(event_time_series, unit="s", utc=True).dt.strftime("%Y-%m-%d")


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """写出 parquet，不含 pandas index。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path))


# ── 核心转换逻辑 ──────────────────────────────────────────────────────────────

def expand_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    将 event_type 列展开为 19 个行为列。

    每行仅有对应 event_type 的列被置为 1（二值）或 0（连续值字段无实际数值）。
    """
    for etype, field in EVENT_TYPE_TO_FIELD.items():
        mask = df["event_type"] == etype
        if field in CONTINUOUS_FIELDS:
            df[field] = np.float32(0.0)
        else:
            df[field] = np.where(mask, np.int8(1), np.int8(0))
    return df


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    处理一个 CSV chunk：列重命名 → 时间转换 → 展开行为列 → 同秒聚合。

    同秒聚合：同一 (user_id, post_id, event_time_seconds) 的事件合并为一行，
    模拟同一次曝光/交互中触发的多个行为。跨秒事件保持为独立行。
    """
    df = chunk.rename(columns={
        "account_id": "user_id",
        "feed_id": "post_id",
        "event_source": "product_surface",
    })

    df["event_time"] = ms_to_seconds(df["event_time"])
    df = expand_event_columns(df)

    # 同秒 (user, post, author) 聚合
    grouped = df.groupby(_GROUP_COLS, sort=False).agg(_AGG_DICT).reset_index()
    return grouped


def enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """确保输出 DataFrame 的 dtype 与 generate_example_data.py 一致。"""
    df["event_time"] = df["event_time"].astype(np.int64)
    df["user_id"] = df["user_id"].astype("string")
    df["post_id"] = df["post_id"].astype("string")
    df["author_id"] = df["author_id"].astype("string")
    df["product_surface"] = df["product_surface"].astype(np.int8)
    for field in BINARY_FIELDS:
        df[field] = df[field].astype(np.int8)
    df["vqv"] = df["vqv"].astype(np.float32)
    df["dwell_time"] = df["dwell_time"].astype(np.int32)
    return df


# ── 主流程 ────────────────────────────────────────────────────────────────────

def import_csv(
    csv_path: str,
    output_dir: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    tz_name: str = "UTC",
) -> None:
    """
    读取 feed_event.csv 并输出 behavior_logs + 元数据 parquet。

    流式处理：每个 chunk 独立转换后按日期追加到对应分区文件列表，
    最后按日期合并写出，避免一次性全量加载到内存。
    """
    output = Path(output_dir)
    logger.info(f"开始导入: {csv_path}")
    logger.info(f"日期分区时区: {tz_name}")

    reader = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype={
            "account_id": str,
            "feed_id": str,
            "author_id": str,
            "event_time": np.int64,
            "event_type": np.int8,
            "event_source": np.int8,
        },
    )

    # 按日期分桶：date_str → [chunk_df, ...]
    date_buckets: Dict[str, List[pd.DataFrame]] = defaultdict(list)
    total_csv_rows = 0
    total_event_rows = 0

    for i, raw_chunk in enumerate(reader):
        total_csv_rows += len(raw_chunk)
        processed = process_chunk(raw_chunk)
        total_event_rows += len(processed)

        # 按日期分桶（向量化）
        _dates = vectorized_date_column(processed["event_time"])
        for date_str, day_df in processed.groupby(_dates):
            date_buckets[str(date_str)].append(day_df)

        if (i + 1) % 10 == 0:
            logger.info(f"  已处理 {total_csv_rows:,} 行 CSV ({i + 1} 个 chunk)")

    logger.info(f"CSV 读取完成: {total_csv_rows:,} 行 → {total_event_rows:,} 条事件")

    # ── 按日期写出 behavior_logs，同时增量聚合元数据 ─────────────────────
    # 增量聚合：避免在内存中保留全量 behavior DataFrame
    post_meta_frames: List[pd.DataFrame] = []  # 每天 post 元数据，最后统一去重
    user_agg: Dict[str, int] = {}            # user_id → min_event_time
    # 行为触发计数
    behavior_counts: Dict[str, int] = {f: 0 for f in BINARY_FIELDS + CONTINUOUS_FIELDS}
    total_merged_rows = 0

    sorted_dates = sorted(date_buckets.keys())
    logger.info(
        f"数据跨越 {len(sorted_dates)} 天: "
        f"{sorted_dates[:5]}{'...' if len(sorted_dates) > 5 else ''}"
    )

    for date_str in sorted_dates:
        day_df = pd.concat(date_buckets.pop(date_str), ignore_index=True)

        # 同秒事件可能跨 chunk 被拆开，再做一次合并
        day_df = day_df.groupby(_GROUP_COLS, sort=False).agg(_AGG_DICT).reset_index()

        day_df = enforce_dtypes(day_df)
        day_df = day_df.sort_values(
            ["user_id", "event_time"], kind="stable"
        ).reset_index(drop=True)

        day_df = day_df[OUTPUT_COLUMNS]

        part_path = output / "behavior_logs" / f"dt={date_str}" / "part-00000.parquet"
        write_parquet(day_df, part_path)
        logger.info(f"  behavior_logs dt={date_str}: {len(day_df):,} 条")

        # 增量聚合元数据和统计信息
        total_merged_rows += len(day_df)
        for field in BINARY_FIELDS:
            behavior_counts[field] += int(day_df[field].sum())
        for field in CONTINUOUS_FIELDS:
            behavior_counts[field] += int((day_df[field] > 0).sum())

        # 向量化聚合当天的 post/user 元数据
        day_post = (
            day_df.groupby("post_id", as_index=False)
            .agg(author_id=("author_id", "first"), create_time=("event_time", "min"))
        )
        post_meta_frames.append(day_post)

        day_user = day_df.groupby("user_id")["event_time"].min()
        for uid, et in zip(day_user.index, day_user.values):
            et = int(et)
            if uid not in user_agg or et < user_agg[uid]:
                user_agg[uid] = et

        del day_df

    del date_buckets

    # ── 生成 post_metadata.parquet ────────────────────────────────────────
    if post_meta_frames:
        post_meta = pd.concat(post_meta_frames, ignore_index=True)
        # 显式去重：同一 post_id 出现多次时保留最早的 create_time 和第一个 author_id
        post_meta = post_meta.groupby("post_id", as_index=False).agg(
            author_id=("author_id", "first"),
            create_time=("create_time", "min"),
        )
    else:
        post_meta = pd.DataFrame(columns=["post_id", "author_id", "create_time"])

    post_meta["is_active"] = np.int8(1)
    post_meta["post_id"] = post_meta["post_id"].astype("string")
    post_meta["author_id"] = post_meta["author_id"].astype("string")
    post_meta["create_time"] = post_meta["create_time"].astype(np.int64)

    post_meta_path = output / "post_metadata.parquet"
    write_parquet(post_meta, post_meta_path)
    logger.info(f"post_metadata: {len(post_meta):,} 个帖子 → {post_meta_path}")

    # ── 生成 user_metadata.parquet ────────────────────────────────────────
    user_meta = pd.DataFrame([
        {"user_id": uid, "register_time": et}
        for uid, et in user_agg.items()
    ])
    del user_agg
    user_meta["is_active"] = np.int8(1)
    user_meta["user_id"] = user_meta["user_id"].astype("string")
    user_meta["register_time"] = user_meta["register_time"].astype(np.int64)

    user_meta_path = output / "user_metadata.parquet"
    write_parquet(user_meta, user_meta_path)
    logger.info(f"user_metadata: {len(user_meta):,} 个用户 → {user_meta_path}")

    # ── 总结 ──────────────────────────────────────────────────────────────
    logger.info("=== 导入完成 ===")
    logger.info(f"输出目录: {output}")
    logger.info(f"CSV 原始行数: {total_csv_rows:,}")
    logger.info(f"事件记录数（同秒合并后）: {total_merged_rows:,}")
    logger.info(f"用户数: {len(user_meta):,}")
    logger.info(f"帖子数: {len(post_meta):,}")

    # 行为分布
    logger.info("行为触发分布:")
    for field in BINARY_FIELDS + CONTINUOUS_FIELDS:
        count = behavior_counts[field]
        if count > 0:
            logger.info(f"  {field}: {count:,} ({count / total_merged_rows * 100:.2f}%)")

    logger.info("\n后续步骤:")
    logger.info(
        f"  uv run data_preprocessor.py \\\n"
        f"    --behavior-dir {output / 'behavior_logs'} \\\n"
        f"    --post-meta {post_meta_path} \\\n"
        f"    --output-dir data/training_samples"
    )


def main():
    parser = argparse.ArgumentParser(
        description="将 Kafka 导出的 feed 事件 CSV 转换为 phoenix 训练所需的 parquet 格式"
    )
    parser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="feed_event.csv 文件路径",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/real_data",
        help="输出目录（默认 data/real_data）",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"CSV 分块读取大小（默认 {DEFAULT_CHUNK_SIZE:,}）",
    )
    parser.add_argument(
        "--tz",
        type=str,
        default="UTC",
        help="日期分区使用的时区（默认 UTC）",
    )
    args = parser.parse_args()

    import_csv(
        csv_path=args.csv,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        tz_name=args.tz,
    )


if __name__ == "__main__":
    main()
