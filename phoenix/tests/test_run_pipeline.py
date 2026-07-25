import numpy as np

from run_pipeline import (
    TWITTER_EPOCH_MS,
    build_candidate_post_age_timestamps,
    infer_corpus_impression_timestamp,
)


def _snowflake_id(timestamp_ms: int, sequence: int = 0) -> int:
    return ((timestamp_ms - TWITTER_EPOCH_MS) << 22) | sequence


def test_candidate_post_age_timestamps_use_snowflake_creation_and_fixed_snapshot():
    snapshot_seconds = 1_800_000_000
    older_post = _snowflake_id((snapshot_seconds - 2 * 60 * 60) * 1000, 1)
    newer_post = _snowflake_id((snapshot_seconds - 30 * 60) * 1000, 2)

    impression, creation = build_candidate_post_age_timestamps(
        [older_post, newer_post], candidate_len=3, impression_seconds=snapshot_seconds
    )

    np.testing.assert_array_equal(
        impression, [[snapshot_seconds, snapshot_seconds, snapshot_seconds]]
    )
    np.testing.assert_array_equal(
        creation,
        [[snapshot_seconds - 2 * 60 * 60, snapshot_seconds - 30 * 60, 0]],
    )


def test_corpus_snapshot_uses_latest_valid_snowflake_timestamp():
    earlier = _snowflake_id(1_700_000_000_000, 1)
    latest = _snowflake_id(1_700_000_300_000, 2)

    assert (
        infer_corpus_impression_timestamp(np.array([0, earlier, latest], dtype=np.uint64))
        == 1_700_000_300
    )
