import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import build_training_inputs as etl  # noqa: E402


def oid(value: int) -> str:
    return f"{value:024x}"


def test_training_input_builder_attributes_to_nearest_exposure(tmp_path):
    user, author, post = oid(1), oid(2), oid(3)
    t0 = 1_700_000_000_000
    served = {
        "schema_version": 1,
        "request_id": "r1",
        "viewer_id": user,
        "request_time_ms": t0,
        "candidates": [{"position": 0, "post_id": post, "author_id": author}],
    }
    behavior = {
        "user_id": user,
        "tweet_id": post,
        "author_id": author,
        "action_time_ms": t0 + 1_000,
        "action_type": 1,
        "product_surface": 0,
    }
    served_path = tmp_path / "served.jsonl"
    behavior_path = tmp_path / "behavior.jsonl"
    served_path.write_text(json.dumps(served) + "\n")
    behavior_path.write_text(json.dumps(behavior) + "\n")

    stats = etl.build([str(served_path)], [str(behavior_path)], str(tmp_path / "out"), 30, False)

    assert stats["impressions_with_action"] == 1
    assert stats["behavior_log_rows"] == 1
    assert (tmp_path / "out" / "impressions" / "dt=2023-11-14" / "part-0.parquet").exists()


def test_training_input_builder_rejects_non_contract_ids():
    assert not etl.is_object_id("not-an-object-id")
    assert etl.is_object_id("0123456789abcdef01234567")
