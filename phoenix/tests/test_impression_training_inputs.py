"""曝光 + 行为事件 → 训练输入 → 曝光模式样本的链路约束。

这条链路决定了第一版模型的标签：归因窗口、最近一次下发、正样本排在候选位 0、历史只看
请求之前。任何一处错位都不会报错，只会训出错误的模型，所以逐条锁住。
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from data_preprocessor import (
    BEHAVIOR_FIELDS,
    CANDIDATE_SEQ_LEN,
    TrainingSampleBuilder,
    encode_actions_vectorized,
    hash_id_to_ints,
    load_impressions,
    process_single_day,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import build_training_inputs as etl  # noqa: E402

MINUTE_MS = 60_000
USER = f"{7:024x}"
AUTHOR = f"{99:024x}"
FAV, REPLY, CLICK = 1, 2, 6  # proto ActionName 枚举值


def oid(index: int) -> str:
    return f"{index:024x}"


def served_event(request_id, request_time_ms, post_ids, shadow=False, viewer=USER):
    return {
        "schema_version": 1,
        "request_id": request_id,
        "prediction_request_id": 1,
        "viewer_id": viewer,
        "request_time_ms": request_time_ms,
        "is_shadow_traffic": shadow,
        "in_network_only": False,
        "is_bottom_request": False,
        "client_app_id": 0,
        "candidates": [
            {
                "position": position,
                "post_id": post_id,
                "author_id": AUTHOR,
                "retweeted_post_id": None,
                "served_type": "FOR_YOU_PHOENIX_RETRIEVAL",
                "in_network": False,
                "score": 0.5,
                "weighted_score": None,
                "degraded_reason": None,
                "created_at_ms": request_time_ms - 3_600_000,
            }
            for position, post_id in enumerate(post_ids)
        ],
    }


def behavior_event(post_id, action_time_ms, action_type, user=USER, surface=0):
    return {
        "user_id": user,
        "tweet_id": post_id,
        "author_id": AUTHOR,
        "action_time_ms": action_time_ms,
        "action_type": action_type,
        "product_surface": surface,
    }


def write_jsonl(path: Path, events) -> Path:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return path


# ==================== 1. ETL：归因 ====================


def test_attribution_uses_window_and_nearest_exposure(tmp_path):
    t0 = 1_700_000_000_000
    served = write_jsonl(
        tmp_path / "served.jsonl",
        [
            served_event("req-a", t0, [oid(1), oid(2), oid(3)]),
            served_event("req-b", t0 + 10 * MINUTE_MS, [oid(1), oid(4)]),  # 帖 1 再次下发
            served_event("req-shadow", t0, [oid(9)], shadow=True),
        ],
    )
    behaviors = write_jsonl(
        tmp_path / "uas.jsonl",
        [
            behavior_event(oid(1), t0 + 12 * MINUTE_MS, FAV),  # 落在 req-b 之后 → 归 req-b
            behavior_event(oid(2), t0 + 5 * MINUTE_MS, CLICK),  # 归 req-a
            behavior_event(oid(2), t0 + 5 * MINUTE_MS, CLICK),  # Kafka 重放，去重
            behavior_event(oid(3), t0 + 45 * MINUTE_MS, REPLY),  # 超出 30 分钟窗口 → 负样本
            behavior_event(oid(3), t0 - MINUTE_MS, REPLY),  # 下发之前的行为不算
            behavior_event(oid(9), t0 + MINUTE_MS, FAV),  # 只在影子流量里下发过
        ],
    )

    stats = etl.build([str(served)], [str(behaviors)], str(tmp_path / "out"), 30.0, False)
    assert stats["impressions"] == 5, "影子流量默认排除"
    assert stats["impressions_with_action"] == 2

    impressions = pd.concat(
        pd.read_parquet(p) for p in (tmp_path / "out" / "impressions").rglob("*.parquet")
    )
    by_key = impressions.set_index(["request_id", "post_id"])
    assert by_key.loc[("req-b", oid(1)), "favorite"] == 1.0
    assert by_key.loc[("req-a", oid(1)), "favorite"] == 0.0, "归到最近一次下发，不重复计"
    assert by_key.loc[("req-a", oid(2)), "click"] == 1.0
    assert by_key.loc[("req-a", oid(3)), BEHAVIOR_FIELDS].sum() == 0.0
    assert (by_key["event_time"] == by_key["event_time"].astype(np.int64)).all()
    assert by_key.loc[("req-a", oid(2)), "event_time"] == t0 // 1000

    logs = pd.concat(
        pd.read_parquet(p) for p in (tmp_path / "out" / "behavior_logs").rglob("*.parquet")
    )
    # 行为日志按 (user, post) 聚合，mask 取或、时间取最早，与线上 DefaultAggregator 同构。
    post3 = logs[logs["post_id"] == oid(3)].iloc[0]
    assert post3["reply"] == 1.0 and post3["event_time"] == (t0 - MINUTE_MS) // 1000
    assert len(logs[logs["post_id"] == oid(2)]) == 1

    meta = pd.read_parquet(tmp_path / "out" / "post_metadata.parquet")
    assert set(meta["post_id"]) == {oid(1), oid(2), oid(3), oid(4), oid(9)}
    assert (meta["is_active"] == 1).all()
    assert meta.set_index("post_id").loc[oid(1), "create_time"] == (t0 - 3_600_000) // 1000


def test_etl_rejects_malformed_events_but_keeps_valid_ones(tmp_path):
    t0 = 1_700_000_000_000
    served = write_jsonl(
        tmp_path / "served.jsonl",
        [
            served_event("req-a", t0, [oid(1), "not-an-id"]),
            {"schema_version": 2, "request_id": "future", "viewer_id": USER, "candidates": []},
        ],
    )
    behaviors = write_jsonl(
        tmp_path / "uas.jsonl",
        [
            behavior_event(oid(1), t0 + MINUTE_MS, FAV),
            behavior_event(oid(1), t0 + MINUTE_MS, 99),  # 枚举越界
            behavior_event(oid(1), t0 + MINUTE_MS, FAV, surface=16),  # surface 越界
            {"user_id": USER, "tweet_id": oid(1)},  # 缺字段
        ],
    )
    exposures = etl.load_served_events([str(served)], include_shadow=False)
    assert exposures["post_id"].tolist() == [oid(1)]
    events = etl.load_behavior_events([str(behaviors)])
    assert len(events) == 1


# ==================== 2. 曝光模式样本 ====================


def impressions_frame(rows):
    df = pd.DataFrame(rows)
    for field in BEHAVIOR_FIELDS:
        df[field] = df[field].fillna(0.0) if field in df.columns else 0.0
    return df


def test_impression_samples_put_positives_first_and_history_before_request():
    t_req = 1_700_000_000
    behaviors = pd.DataFrame(
        {
            "user_id": [USER] * 3,
            "post_id": [oid(101), oid(102), oid(2)],
            "author_id": [AUTHOR] * 3,
            "event_time": [t_req - 20 * 86400, t_req - 100, t_req + 60],  # 太老 / 有效 / 请求之后
            "product_surface": [0, 1, 0],
            "favorite": [1.0, 1.0, 1.0],
        }
    )
    for field in BEHAVIOR_FIELDS:
        if field not in behaviors.columns:
            behaviors[field] = 0.0
    impressions = impressions_frame(
        [
            {
                "user_id": USER,
                "request_id": "req-1",
                "event_time": t_req,
                "position": position,
                "post_id": oid(post),
                "author_id": AUTHOR,
                "product_surface": 0,
                "favorite": 1.0 if post == 2 else 0.0,
            }
            for position, post in enumerate([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        ]
        + [
            {
                "user_id": USER,
                "request_id": "req-2",
                "event_time": t_req + 10,
                "position": 0,
                "post_id": oid(11),
                "author_id": AUTHOR,
                "product_surface": 0,
            }
        ]
    )
    builder = TrainingSampleBuilder(post_to_author={}, active_posts=None)
    samples = builder.build_impression_samples(
        USER,
        behaviors,
        encode_actions_vectorized(behaviors),
        impressions,
        encode_actions_vectorized(impressions),
    )

    assert len(samples) == 1, "没有正样本的 req-2 默认丢弃"
    sample = samples[0]
    assert sample["event_time"] == t_req
    assert sample["positive_post"] == oid(2)
    # 候选位 0 是正样本，其余按下发位置排列并截到 8 个。
    assert sample["candidate_post_hashes"][0] == hash_id_to_ints(oid(2))
    assert sample["candidate_post_hashes"][1] == hash_id_to_ints(oid(1))
    assert len(sample["candidate_post_hashes"]) == CANDIDATE_SEQ_LEN
    assert sample["negative_posts"] == [oid(i) for i in [1, 3, 4, 5, 6, 7, 8]]
    assert sample["labels"][0][BEHAVIOR_FIELDS.index("favorite")] == 1.0
    assert sum(sum(row) for row in sample["labels"][1:]) == 0.0
    # 历史只包含请求之前、7 天窗口内的行为：post 102（含其 surface），不含太老的 101 和之后的 2。
    assert sample["history_post_hashes"][0] == hash_id_to_ints(oid(102))
    assert sample["history_post_hashes"][1] == [0, 0]
    assert sample["history_product_surface"][0] == 1

    with_negatives = builder.build_impression_samples(
        USER,
        behaviors,
        encode_actions_vectorized(behaviors),
        impressions,
        encode_actions_vectorized(impressions),
        include_negative_only=True,
    )
    assert [s["positive_post"] for s in with_negatives] == [oid(2), ""]
    assert with_negatives[1]["negative_posts"] == [oid(11)]


def test_impression_mode_end_to_end_and_retrieval_loader_skips_negative_only(tmp_path):
    t_req = 1_700_000_000
    behaviors = pd.DataFrame(
        {
            "user_id": [USER, USER, oid(8)],
            "post_id": [oid(50), oid(51), oid(52)],
            "author_id": [AUTHOR] * 3,
            "event_time": [t_req - 300, t_req - 200, t_req - 100],
            "product_surface": [0, 0, 0],
            "click": [1.0, 1.0, 1.0],
        }
    )
    impressions = impressions_frame(
        [
            {"user_id": USER, "request_id": "r1", "event_time": t_req, "position": 0,
             "post_id": oid(1), "author_id": AUTHOR, "product_surface": 0, "favorite": 1.0},
            {"user_id": USER, "request_id": "r1", "event_time": t_req, "position": 1,
             "post_id": oid(2), "author_id": AUTHOR, "product_surface": 0},
            {"user_id": USER, "request_id": "r2", "event_time": t_req + 5, "position": 0,
             "post_id": oid(3), "author_id": AUTHOR, "product_surface": 0},
            # 用户 oid(9) 没有任何行为记录 → 无历史 → 不构造样本
            {"user_id": oid(9), "request_id": "r3", "event_time": t_req, "position": 0,
             "post_id": oid(4), "author_id": AUTHOR, "product_surface": 0, "favorite": 1.0},
        ]
    )
    impressions_dir = tmp_path / "impressions" / "dt=2023-11-14"
    impressions_dir.mkdir(parents=True)
    impressions.to_parquet(impressions_dir / "part-0.parquet", index=False)

    loaded = load_impressions(str(tmp_path / "impressions"))
    assert len(loaded) == 4

    output = tmp_path / "train.parquet"
    process_single_day(
        behavior_df=behaviors,
        post_meta_df=None,
        output_path=str(output),
        impressions_df=loaded,
        include_negative_only=True,
    )
    table = pq.read_table(output)
    assert table.num_rows == 2, "r1（有正样本）+ r2（仅负样本）；无历史用户被跳过"
    assert table.column("positive_post").to_pylist() == [oid(1), ""]
    assert table.column("labels")[0].as_py()[0][BEHAVIOR_FIELDS.index("favorite")] == 1.0

    import train_retrieval as tr

    batch = tr.load_parquet_batch(str(output), batch_size=8)
    assert batch.user_hashes.shape[0] == 1, "召回训练必须跳过 positive_post 为空的行"

    process_single_day(
        behavior_df=behaviors,
        post_meta_df=None,
        output_path=str(tmp_path / "positives_only.parquet"),
        impressions_df=loaded,
        include_negative_only=False,
    )
    assert pq.read_table(tmp_path / "positives_only.parquet").num_rows == 1

    with pytest.raises(ValueError, match="帖子元数据"):
        process_single_day(behaviors, None, str(tmp_path / "x.parquet"))
