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
    # 行为日志一行一个事件，不预聚合：帖 3 的两次 reply 是两行，各带自己的时间；
    # 按帖合并留给 data_preprocessor 在知道请求时间的地方做（否则请求之后的行为会进历史）。
    post3 = logs[logs["post_id"] == oid(3)].sort_values("event_time")
    assert post3["event_time"].tolist() == [(t0 - MINUTE_MS) // 1000, (t0 + 45 * MINUTE_MS) // 1000]
    assert (post3["reply"] == 1.0).all() and (post3["action_type"] == REPLY).all()
    assert post3[[f for f in BEHAVIOR_FIELDS if f != "reply"]].to_numpy().sum() == 0.0
    assert len(logs[logs["post_id"] == oid(2)]) == 1, "Kafka 重放在事件层去重"

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


def event_rows(rows):
    """事件表（build_training_inputs 的 behavior_logs 形状）：一行一个行为，one-hot + action_type。"""
    df = pd.DataFrame(rows)
    for field in BEHAVIOR_FIELDS:
        df[field] = 0.0
    for i, action_type in enumerate(df["action_type"].tolist()):
        df.loc[i, etl.ENUM_TO_FIELD[action_type]] = 1.0
    return df


def test_history_mask_only_contains_actions_before_the_request():
    """历史按帖合并时必须以请求时间为截止；请求之后的行为（往往就是这次曝光的标签）不能进历史。

    线上 `DefaultAggregator` 只看 [请求 - 7 天, 请求] 内的行为，所以：
      - 帖 A：请求前点赞、请求后评论 → 历史里 A 只有点赞位；
      - 帖 B：请求前先点击（surface 2）再点赞 → 历史里 B 一条，两位都为 1，surface 取最早的 2；
      - 排序按最早行为时间：B 在 A 之前；
      - 请求之后才第一次出现的帖 C 不在历史里。
    """
    t_req = 1_700_000_000
    a, b, c = oid(201), oid(202), oid(203)
    behaviors = event_rows(
        [
            {"user_id": USER, "post_id": b, "author_id": AUTHOR, "event_time": t_req - 300,
             "product_surface": 2, "action_type": CLICK},
            {"user_id": USER, "post_id": b, "author_id": AUTHOR, "event_time": t_req - 200,
             "product_surface": 0, "action_type": FAV},
            {"user_id": USER, "post_id": a, "author_id": AUTHOR, "event_time": t_req - 100,
             "product_surface": 0, "action_type": FAV},
            {"user_id": USER, "post_id": a, "author_id": AUTHOR, "event_time": t_req + 60,
             "product_surface": 0, "action_type": REPLY},
            {"user_id": USER, "post_id": c, "author_id": AUTHOR, "event_time": t_req + 120,
             "product_surface": 0, "action_type": FAV},
        ]
    )
    impressions = impressions_frame(
        [
            {"user_id": USER, "request_id": "req-1", "event_time": t_req, "position": 0,
             "post_id": a, "author_id": AUTHOR, "product_surface": 0, "reply": 1.0},
            {"user_id": USER, "request_id": "req-1", "event_time": t_req, "position": 1,
             "post_id": c, "author_id": AUTHOR, "product_surface": 0, "favorite": 1.0},
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
    assert len(samples) == 1
    history = samples[0]
    fav_idx, reply_idx, click_idx = (
        BEHAVIOR_FIELDS.index("favorite"),
        BEHAVIOR_FIELDS.index("reply"),
        BEHAVIOR_FIELDS.index("click"),
    )
    assert history["history_post_hashes"][:2] == [hash_id_to_ints(b), hash_id_to_ints(a)]
    assert history["history_post_hashes"][2] == [0, 0], "帖 C 首次行为在请求之后，不进历史"
    b_mask, a_mask = history["history_actions"][0], history["history_actions"][1]
    assert b_mask[click_idx] == 1.0 and b_mask[fav_idx] == 1.0
    assert history["history_product_surface"][0] == 2, "surface 取窗口内最早一次行为"
    assert a_mask[fav_idx] == 1.0
    assert a_mask[reply_idx] == 0.0, "请求之后的评论是这次曝光的标签，不能泄漏进历史"
    assert history["labels"][0][reply_idx] == 1.0


def test_legacy_mode_merges_event_rows_per_post():
    """行为模式把事件表按 (user, post) 合并成一条多热正样本，不把同一帖的两位拆成互为负样本。"""
    from data_preprocessor import aggregate_action_events, is_action_event_table

    behaviors = event_rows(
        [
            {"user_id": USER, "post_id": oid(1), "author_id": AUTHOR, "event_time": 100,
             "product_surface": 3, "action_type": FAV},
            {"user_id": USER, "post_id": oid(1), "author_id": AUTHOR, "event_time": 250,
             "product_surface": 0, "action_type": REPLY},
            {"user_id": USER, "post_id": oid(2), "author_id": AUTHOR, "event_time": 300,
             "product_surface": 0, "action_type": CLICK},
        ]
    )
    assert is_action_event_table(behaviors)
    merged = aggregate_action_events(behaviors)
    assert len(merged) == 2 and "action_type" not in merged.columns
    post1 = merged[merged["post_id"] == oid(1)].iloc[0]
    assert post1["event_time"] == 100 and post1["product_surface"] == 3
    assert post1["favorite"] == 1.0 and post1["reply"] == 1.0 and post1["click"] == 0.0

    plain = pd.DataFrame({"user_id": [USER], "post_id": [oid(1)], "event_time": [1], "favorite": [1.0]})
    assert not is_action_event_table(plain), "多热日志表没有 action_type 列，保持逐行语义"


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
