# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from __future__ import annotations

import bisect
import json
import logging
import os
from datetime import datetime
from typing import TypedDict

import pyarrow.parquet as pq

logger = logging.getLogger("rank")

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

BATCHES_PER_SUBDIR = 2000


class ValidBatchesMetadata(TypedDict):
    min_valid_batch: int
    max_valid_batch: int
    num_partitions: int


class DataPosition(TypedDict):
    last_batch_id: int
    rows_read_in_batch: int
    batch_size: int | None


def batch_path(topic_dir: str, partition_id: int, batch_id: int) -> str:
    sub_dir = str(batch_id // BATCHES_PER_SUBDIR)
    return os.path.join(
        topic_dir, f"partition={partition_id}", sub_dir, f"batch_{batch_id}.parquet"
    )


def load_valid_batches_metadata(path: str) -> ValidBatchesMetadata | None:
    try:
        with open(path) as f:
            data = json.load(f)
        return ValidBatchesMetadata(
            min_valid_batch=int(data["min_valid_batch"]),
            max_valid_batch=int(data["max_valid_batch"]),
            num_partitions=int(data["num_partitions"]),
        )
    except (OSError, KeyError, ValueError, json.JSONDecodeError) as e:
        logger.warning("Failed to load valid batches metadata from %s: %s", path, e)
        return None


def parse_date_bound(s: str) -> int | None:
    s = s.strip().strip("()")
    if s.lower() == "none":
        return None
    return int(datetime.strptime(s, DATE_TIME_FORMAT).timestamp() * 1000)


class _FooterTimestamps:
    def __init__(self, topic_dir: str, min_batch: int, max_batch: int):
        self._topic_dir = topic_dir
        self._min_batch = min_batch
        self._max_batch = max_batch

    def __len__(self) -> int:
        return self._max_batch - self._min_batch + 1

    def __getitem__(self, idx: int) -> int:
        bid = self._min_batch + idx
        path = batch_path(self._topic_dir, 0, bid)
        try:
            pf = pq.ParquetFile(path)
            meta = pf.metadata.metadata or {}
            return int(meta[b"min_kafka_timestamp_ms"])
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(
                f"Parquet file {path} does not contain min_kafka_timestamp_ms footer metadata. "
                f"Time-range filtering requires this footer key on "
                f"Hive-partitioned files. Error: {e}"
            ) from e
        except Exception as e:
            raise ValueError(f"Cannot read footer metadata from {path}: {e}") from e


def resolve_time_range(
    topic_dir: str,
    min_batch: int,
    max_batch: int,
    min_timestamp_ms: int | None,
    max_timestamp_ms: int | None,
) -> tuple[int, int]:
    ts = _FooterTimestamps(topic_dir, min_batch, max_batch)
    n = len(ts)

    if n == 0:
        raise ValueError("No valid batches — cannot resolve time range")
    data_min_ts = ts[0]
    data_max_ts = ts[n - 1]
    if min_timestamp_ms is not None and min_timestamp_ms > data_max_ts:
        raise ValueError(
            f"min_timestamp_ms={min_timestamp_ms} is after all data "
            f"(last batch min_ts={data_max_ts})"
        )
    if max_timestamp_ms is not None and max_timestamp_ms <= data_min_ts:
        raise ValueError(
            f"max_timestamp_ms={max_timestamp_ms} is before all data "
            f"(first batch min_ts={data_min_ts})"
        )

    start_idx = bisect.bisect_left(ts, min_timestamp_ms) if min_timestamp_ms is not None else 0
    end_idx = bisect.bisect_left(ts, max_timestamp_ms) if max_timestamp_ms is not None else n

    start_batch_id = min_batch + start_idx
    end_batch_id = min_batch + end_idx

    if start_batch_id >= end_batch_id:
        raise ValueError(
            f"Time range [{min_timestamp_ms}, {max_timestamp_ms}) "
            f"resolves to empty batch range [{start_batch_id}, {end_batch_id})"
        )

    logger.info(
        "Time range [%s, %s) → batch_ids [%d, %d) (%d batches)",
        min_timestamp_ms,
        max_timestamp_ms,
        start_batch_id,
        end_batch_id,
        end_batch_id - start_batch_id,
    )
    return start_batch_id, end_batch_id
