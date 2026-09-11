"""负样本必须是事件时刻线上真的可能出现的帖子：已发布、且未超过最大帖龄。"""

import random

import pandas as pd

from data_preprocessor import TrainingSampleBuilder, build_negative_pool

DAY = 86400


def _builder(meta: pd.DataFrame, max_age_days: int = 7) -> TrainingSampleBuilder:
    return TrainingSampleBuilder(
        post_to_author=dict(zip(meta["post_id"], meta["author_id"])),
        active_posts=set(meta["post_id"]),
        negative_pool=build_negative_pool(meta),
        max_age_seconds=max_age_days * DAY,
    )


def test_negatives_exclude_future_and_expired_posts():
    t0 = 1_000 * DAY
    meta = pd.DataFrame(
        {
            "post_id": ["expired", "old_ok", "fresh", "future"],
            "author_id": ["a", "a", "a", "a"],
            "create_time": [t0 - 8 * DAY, t0 - 6 * DAY, t0 - 1, t0 + 1],
            "is_active": [1, 1, 1, 1],
        }
    )
    builder = _builder(meta)
    for seed in range(20):
        picked = builder._sample_negative_posts(
            frozenset(), "positive", 3, random.Random(seed), event_time=t0
        )
        assert set(picked) == {"old_ok", "fresh"}, picked


def test_negatives_fall_back_to_full_pool_without_create_time():
    meta = pd.DataFrame(
        {"post_id": ["p1", "p2", "p3"], "author_id": ["a", "a", "a"], "is_active": [1, 1, 1]}
    )
    builder = _builder(meta)
    picked = builder._sample_negative_posts(frozenset(), "positive", 3, random.Random(0), 5)
    assert set(picked) == {"p1", "p2", "p3"}


def test_negatives_skip_interacted_and_positive_posts():
    t0 = 1_000 * DAY
    meta = pd.DataFrame(
        {
            "post_id": ["seen", "pos", "n1", "n2"],
            "author_id": ["a"] * 4,
            "create_time": [t0 - 1] * 4,
            "is_active": [1] * 4,
        }
    )
    builder = _builder(meta)
    picked = builder._sample_negative_posts(
        frozenset({"seen"}), "pos", 7, random.Random(1), event_time=t0
    )
    assert set(picked) == {"n1", "n2"}


def test_empty_window_yields_no_negatives():
    t0 = 1_000 * DAY
    meta = pd.DataFrame(
        {"post_id": ["future"], "author_id": ["a"], "create_time": [t0 + DAY], "is_active": [1]}
    )
    builder = _builder(meta)
    assert builder._sample_negative_posts(frozenset(), "pos", 3, random.Random(0), t0) == []
