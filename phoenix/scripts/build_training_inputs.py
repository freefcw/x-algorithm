#!/usr/bin/env python3
"""
把线上两条事件流（服务端曝光 + 用户行为）整理成生产训练输入表。

这是训练数据链路的第一步：

    served-candidates 事件（home-mixer SE-11）─┐
                                              ├─▶ 本脚本 ─▶ behavior_logs/ impressions/ post_metadata
    UAS 行为事件（uas-event-contract.md）──────┘

输入（都是 JSON Lines，可以给文件或目录，目录内递归读取 *.jsonl / *.json / *.ndjson）：
    --served-events     served-candidates-event-contract.md 的事件，一行一次请求
    --behavior-events   uas-event-contract.md 的事件，一行一次行为

输出（写到 --output-dir）：
    behavior_logs/dt=YYYY-MM-DD/part-0.parquet
        一行一个行为事件（与 UAS 事件一一对应）：`event_time` 是该行为的秒级时间，
        `action_type` 是 proto 枚举值，19 个行为列里只有这一位为 1。这里**不**按帖子预聚合：
        线上 `DefaultAggregator` 只合并请求时刻之前、7 天窗口内的行为，预聚合会把请求之后的
        行为（往往正是这次曝光的标签）混进历史 mask。生产训练读取器须按请求时间聚合。
    impressions/dt=YYYY-MM-DD/part-0.parquet
        一行一条下发候选，带归因后的 19 列标签：行为发生在 [request_time, request_time + 窗口]
        内、且该帖在窗口内被多次下发时归到最近的一次下发。没有任何行为的候选就是负样本。
    post_metadata.parquet
        从两类事件里抽出的 post_id / author_id / create_time / is_active=1。这只是兜底：有
        mrpyq 的帖子导出时用导出的（含删除状态）覆盖它。

未做的事（有意留给上游合同）：
    - dwell_time 连续值：UAS 事件没有停留秒数，列固定为 0，只有 dwell（是否停留）二值列。
    - 客户端真实曝光：这里的曝光是服务端下发；客户端曝光回传接入后用它替换 impressions 的行集。
    - product_surface：曝光事件没有该字段，impressions 里固定 0；行为事件里的值原样保留。

用法:
    uv run scripts/build_training_inputs.py \
        --served-events data/raw/served/ \
        --behavior-events data/raw/uas/ \
        --output-dir data/ \
        --attribution-window-minutes 30
"""

import _setup_path  # noqa: F401

import argparse
import json
import logging
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from services.model_contract import ACTION_IDX_TO_ENUM

BEHAVIOR_FIELDS = [
    "favorite", "reply", "repost", "photo_expand", "click", "profile_click", "vqv",
    "share", "share_via_dm", "share_via_copy_link", "dwell", "quote", "quoted_click",
    "follow_author", "not_interested", "block_author", "mute_author", "report", "dwell_time",
]


def is_object_id(value: str) -> bool:
    return len(value) == 24 and all(character in "0123456789abcdef" for character in value)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_training_inputs")

# proto ActionName 枚举值 → data_preprocessor 行为列名（dwell_time 是连续值，没有枚举）。
ENUM_TO_FIELD = {enum: BEHAVIOR_FIELDS[idx] for idx, enum in enumerate(ACTION_IDX_TO_ENUM)}
DISCRETE_FIELDS = [field for field in BEHAVIOR_FIELDS if field != "dwell_time"]

MAX_PRODUCT_SURFACE = 15


# ── 读取 ───────────────────────────────────────────────────────────────────────


def iter_json_lines(paths: Iterable[str]) -> Iterator[dict]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(
                sorted(p for p in path.rglob("*") if p.suffix in {".jsonl", ".json", ".ndjson"})
            )
        elif path.exists():
            files.append(path)
        else:
            raise SystemExit(f"输入不存在：{path}")
    if not files:
        raise SystemExit(f"在 {list(paths)} 下没有找到 JSON Lines 文件")
    for file in files:
        with file.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning("跳过 %s:%d 非法 JSON：%s", file, line_number, exc)


def load_served_events(paths: Iterable[str], include_shadow: bool) -> pd.DataFrame:
    """把每次请求的事件展平成一行一条候选。"""
    rows: list[dict] = []
    dropped = {"schema": 0, "shadow": 0, "invalid_id": 0}
    for event in iter_json_lines(paths):
        if event.get("schema_version") != 1:
            dropped["schema"] += 1
            continue
        if event.get("is_shadow_traffic") and not include_shadow:
            dropped["shadow"] += 1
            continue
        user_id = str(event.get("viewer_id", ""))
        request_id = str(event.get("request_id", ""))
        request_time_ms = event.get("request_time_ms")
        if not is_object_id(user_id) or not request_id or not isinstance(request_time_ms, int):
            dropped["invalid_id"] += 1
            continue
        for candidate in event.get("candidates", []):
            post_id = str(candidate.get("post_id", ""))
            author_id = str(candidate.get("author_id", ""))
            if not is_object_id(post_id) or not is_object_id(author_id):
                dropped["invalid_id"] += 1
                continue
            rows.append(
                {
                    "user_id": user_id,
                    "request_id": request_id,
                    "request_time_ms": int(request_time_ms),
                    "position": int(candidate.get("position", 0)),
                    "post_id": post_id,
                    "author_id": author_id,
                    "served_type": candidate.get("served_type") or "",
                    "degraded_reason": candidate.get("degraded_reason") or "",
                    "created_at_ms": candidate.get("created_at_ms"),
                }
            )
    logger.info(
        "曝光事件：%d 条候选；丢弃 schema=%d shadow=%d invalid_id=%d",
        len(rows),
        dropped["schema"],
        dropped["shadow"],
        dropped["invalid_id"],
    )
    df = pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "request_id",
            "request_time_ms",
            "position",
            "post_id",
            "author_id",
            "served_type",
            "degraded_reason",
            "created_at_ms",
        ],
    )
    # 同一 request_id 重复投递（Kafka 重放）按整条覆盖：保留最后一次出现的候选集合。
    if not df.empty:
        df = df.drop_duplicates(subset=["request_id", "post_id"], keep="last")
    return df.reset_index(drop=True)


def load_behavior_events(paths: Iterable[str]) -> pd.DataFrame:
    rows: list[dict] = []
    dropped = {"invalid": 0}
    for event in iter_json_lines(paths):
        user_id = str(event.get("user_id", ""))
        post_id = str(event.get("tweet_id", ""))
        author_id = str(event.get("author_id", ""))
        action_time_ms = event.get("action_time_ms")
        action_type = event.get("action_type")
        surface = event.get("product_surface", 0)
        if (
            not is_object_id(user_id)
            or not is_object_id(post_id)
            or not is_object_id(author_id)
            or not isinstance(action_time_ms, int)
            or action_time_ms <= 0
            or action_type not in ENUM_TO_FIELD
            or not isinstance(surface, int)
            or not 0 <= surface <= MAX_PRODUCT_SURFACE
        ):
            dropped["invalid"] += 1
            continue
        rows.append(
            {
                "user_id": user_id,
                "post_id": post_id,
                "author_id": author_id,
                "action_time_ms": int(action_time_ms),
                "action_type": int(action_type),
                "product_surface": int(surface),
            }
        )
    logger.info("行为事件：%d 条；丢弃 invalid=%d", len(rows), dropped["invalid"])
    df = pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "post_id",
            "author_id",
            "action_time_ms",
            "action_type",
            "product_surface",
        ],
    )
    if not df.empty:
        df = df.drop_duplicates(subset=["user_id", "post_id", "action_type", "action_time_ms"])
    return df.reset_index(drop=True)


# ── 变换 ───────────────────────────────────────────────────────────────────────


BEHAVIOR_LOG_COLUMNS = [
    "user_id",
    "post_id",
    "author_id",
    "event_time",
    "action_time_ms",
    "action_type",
    "product_surface",
] + BEHAVIOR_FIELDS


def behavior_log_rows(behaviors: pd.DataFrame) -> pd.DataFrame:
    """把校验过的行为事件整理成 behavior_logs 行：一行一个事件，行为列 one-hot。

    不在这里按帖子合并。生产读取器必须以请求时间为截止聚合，否则请求之后的行为会进入历史。
    """
    if behaviors.empty:
        return pd.DataFrame(columns=BEHAVIOR_LOG_COLUMNS)
    ordered = behaviors.sort_values(["user_id", "action_time_ms", "post_id"], kind="stable")
    rows = pd.DataFrame(
        {
            "user_id": ordered["user_id"].to_numpy(),
            "post_id": ordered["post_id"].to_numpy(),
            "author_id": ordered["author_id"].to_numpy(),
            "event_time": (ordered["action_time_ms"] // 1000).astype(np.int64).to_numpy(),
            "action_time_ms": ordered["action_time_ms"].astype(np.int64).to_numpy(),
            "action_type": ordered["action_type"].astype(np.int32).to_numpy(),
            "product_surface": ordered["product_surface"].astype(np.int32).to_numpy(),
        }
    )
    one_hot = np.zeros((len(rows), len(BEHAVIOR_FIELDS)), dtype=np.float32)
    column_index = {field: j for j, field in enumerate(BEHAVIOR_FIELDS)}
    for i, action_type in enumerate(rows["action_type"].tolist()):
        one_hot[i, column_index[ENUM_TO_FIELD[int(action_type)]]] = 1.0
    for j, field in enumerate(BEHAVIOR_FIELDS):
        rows[field] = one_hot[:, j]
    return rows[BEHAVIOR_LOG_COLUMNS].reset_index(drop=True)


def attribute_impressions(
    exposures: pd.DataFrame, behaviors: pd.DataFrame, window_ms: int
) -> pd.DataFrame:
    """给每条下发候选贴上归因窗口内的行为标签。

    规则：行为 (user, post, t) 归到满足 request_time <= t <= request_time + window 的、
    request_time 最大（最近）的那次下发；窗口外或没有下发记录的行为不产生标签。
    """
    labels = pd.DataFrame(
        0.0, index=exposures.index, columns=BEHAVIOR_FIELDS, dtype=np.float32
    )
    out = pd.concat([exposures, labels], axis=1)
    if exposures.empty or behaviors.empty:
        return out

    keyed = exposures[["user_id", "post_id", "request_id", "request_time_ms"]].reset_index()
    keyed = keyed.rename(columns={"index": "exposure_row"})
    joined = behaviors.reset_index().rename(columns={"index": "behavior_row"}).merge(
        keyed, on=["user_id", "post_id"], how="inner"
    )
    joined = joined[
        (joined["action_time_ms"] >= joined["request_time_ms"])
        & (joined["action_time_ms"] <= joined["request_time_ms"] + window_ms)
    ]
    if joined.empty:
        return out
    nearest = (
        joined.sort_values(["behavior_row", "request_time_ms"], ascending=[True, False])
        .drop_duplicates("behavior_row")
    )
    for exposure_row, group in nearest.groupby("exposure_row"):
        for action_type in group["action_type"]:
            out.at[exposure_row, ENUM_TO_FIELD[int(action_type)]] = 1.0
    return out


def impressions_table(attributed: pd.DataFrame) -> pd.DataFrame:
    """整理成生产训练读取器消费的曝光合同。"""
    table = attributed.copy()
    table["event_time"] = (table["request_time_ms"] // 1000).astype(np.int64)
    table["product_surface"] = np.int32(0)
    columns = [
        "user_id",
        "request_id",
        "event_time",
        "position",
        "post_id",
        "author_id",
        "product_surface",
        "served_type",
        "degraded_reason",
    ] + BEHAVIOR_FIELDS
    ordered = table[columns].sort_values(["user_id", "event_time", "request_id", "position"])
    return ordered.reset_index(drop=True)


def post_metadata(exposures: pd.DataFrame, behaviors: pd.DataFrame) -> pd.DataFrame:
    """兜底的帖子元数据：作者取最先出现的，发布时间优先用曝光事件里的 created_at_ms。"""
    frames = []
    if not exposures.empty:
        created = pd.to_numeric(exposures["created_at_ms"], errors="coerce")
        frames.append(
            pd.DataFrame(
                {
                    "post_id": exposures["post_id"],
                    "author_id": exposures["author_id"],
                    "create_time": (created // 1000).astype("Int64"),
                    "seen_time": exposures["request_time_ms"] // 1000,
                }
            )
        )
    if not behaviors.empty:
        frames.append(
            pd.DataFrame(
                {
                    "post_id": behaviors["post_id"],
                    "author_id": behaviors["author_id"],
                    "create_time": pd.array([pd.NA] * len(behaviors), dtype="Int64"),
                    "seen_time": behaviors["action_time_ms"] // 1000,
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["post_id", "author_id", "create_time", "is_active"])
    combined = pd.concat(frames, ignore_index=True)
    grouped = combined.groupby("post_id", sort=True)
    meta = pd.DataFrame(
        {
            "post_id": grouped["author_id"].first().index,
            "author_id": grouped["author_id"].first().to_numpy(),
            # 没有发布时间时退化为最早观察到的时间（只会让负采样窗口更保守）。
            "create_time": grouped["create_time"]
            .min()
            .fillna(grouped["seen_time"].min())
            .astype(np.int64)
            .to_numpy(),
        }
    )
    meta["is_active"] = np.int8(1)
    return meta.reset_index(drop=True)


# ── 写出 ───────────────────────────────────────────────────────────────────────


def write_partitioned(df: pd.DataFrame, root: Path, time_column: str) -> int:
    """按 UTC 日期分区写 parquet（`dt=YYYY-MM-DD/part-0.parquet`），返回分区数。"""
    if df.empty:
        logger.warning("%s 没有数据，不写出", root)
        return 0
    dates = pd.to_datetime(df[time_column], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    count = 0
    for date, part in df.groupby(dates, sort=True):
        target = root / f"dt={date}" / "part-0.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False)
        pq.write_table(table, target)
        count += 1
    logger.info("%s：%d 行，%d 个日期分区", root, len(df), count)
    return count


def build(
    served_paths: list[str],
    behavior_paths: list[str],
    output_dir: str,
    window_minutes: float,
    include_shadow: bool,
) -> dict[str, int]:
    exposures = load_served_events(served_paths, include_shadow)
    behaviors = load_behavior_events(behavior_paths)
    window_ms = int(window_minutes * 60 * 1000)

    logs = behavior_log_rows(behaviors)
    attributed = attribute_impressions(exposures, behaviors, window_ms)
    impressions = impressions_table(attributed)
    meta = post_metadata(exposures, behaviors)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_partitioned(logs, out / "behavior_logs", "event_time")
    write_partitioned(impressions, out / "impressions", "event_time")
    meta_path = out / "post_metadata.parquet"
    pq.write_table(pa.Table.from_pandas(meta, preserve_index=False), meta_path)

    positives = 0
    if not impressions.empty:
        positives = int((impressions[DISCRETE_FIELDS].sum(axis=1) > 0).sum())
    stats = {
        "behavior_events": len(behaviors),
        "behavior_log_rows": len(logs),
        "impressions": len(impressions),
        "impressions_with_action": positives,
        "posts": len(meta),
    }
    logger.info(
        "完成：行为事件 %d → behavior_logs %d 行；曝光候选 %d，其中有行为 %d（%.2f%%）；帖子 %d",
        stats["behavior_events"],
        stats["behavior_log_rows"],
        stats["impressions"],
        stats["impressions_with_action"],
        100.0 * positives / max(len(impressions), 1),
        stats["posts"],
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="把曝光 + 行为事件整理成生产训练输入表"
    )
    parser.add_argument(
        "--served-events", nargs="+", required=True, help="served-candidates JSONL 文件或目录"
    )
    parser.add_argument(
        "--behavior-events", nargs="+", required=True, help="UAS 行为事件 JSONL 文件或目录"
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--attribution-window-minutes",
        type=float,
        default=30.0,
        help="曝光后多少分钟内的行为算这次曝光的标签（默认 30）",
    )
    parser.add_argument(
        "--include-shadow",
        action="store_true",
        help="把 is_shadow_traffic=true 的曝光也纳入（默认排除）",
    )
    args = parser.parse_args()
    if args.attribution_window_minutes <= 0:
        parser.error("--attribution-window-minutes 必须为正数")

    stats = build(
        args.served_events,
        args.behavior_events,
        args.output_dir,
        args.attribution_window_minutes,
        args.include_shadow,
    )
    if stats["impressions"] == 0 and stats["behavior_log_rows"] == 0:
        logger.error("两类事件都为空，没有产出")
        sys.exit(2)


if __name__ == "__main__":
    main()
