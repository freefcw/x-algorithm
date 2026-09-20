import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import build_training_inputs as etl  # noqa: E402
from services.recsys_proto import load_proto_modules  # noqa: E402
from services.xrex_adapter import PhoenixXrexTranslator  # noqa: E402


recsys_pb2, _ = load_proto_modules()


def oid(value: int) -> str:
    return f"{value:024x}"


class StaticIdentityResolver:
    def resolve_batch(self, ids):
        return {
            (kind, object_id): int(object_id, 16)
            for object_id, kind in set(ids)
        }


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

    stats = etl.build(
        [str(served_path)],
        [str(behavior_path)],
        str(tmp_path / "out"),
        30,
        False,
        identity_resolver=StaticIdentityResolver(),
    )

    assert stats["impressions_with_action"] == 1
    assert stats["behavior_log_rows"] == 1
    output = tmp_path / "out" / "impressions" / "dt=2023-11-14" / "part-0.parquet"
    assert output.exists()
    table = pq.read_table(output)
    assert str(table.schema.field("user_id").type) == "uint64"
    assert str(table.schema.field("post_id").type) == "uint64"
    assert str(table.schema.field("author_id").type) == "uint64"
    row = table.to_pylist()[0]
    translated = PhoenixXrexTranslator().prediction_request(
        recsys_pb2.PredictNextActionsRequest(
            user_id=row["user_id"],
            candidates=[
                recsys_pb2.TweetInfo(
                    tweet_id=row["post_id"],
                    author_id=row["author_id"],
                )
            ],
        )
    )
    assert translated.candidateSets[0].userId == 1
    assert translated.candidateSets[0].candidates[0].tweetId == 3
    assert translated.candidateSets[0].candidates[0].authorId == 2
    identity_mapping = json.loads((tmp_path / "out" / "identity_mapping.json").read_text())
    assert identity_mapping == {
        "mapping_version": 2,
        "entries": [
            {"entity_kind": "Post", "object_id": post, "snowflake_id": 3},
            {"entity_kind": "User", "object_id": user, "snowflake_id": 1},
            {"entity_kind": "User", "object_id": author, "snowflake_id": 2},
        ],
    }
    metadata = json.loads((tmp_path / "out" / "training_input_metadata.json").read_text())
    assert metadata["feature_schema"] == "phoenix-snowflake-id-actions-v3"
    assert metadata["identity_mapping_version"] == 2
    assert metadata["identity_count"] == 3
    assert len(metadata["identity_mapping_sha256"]) == 64


def test_training_input_builder_rejects_non_contract_ids():
    assert not etl.is_object_id("not-an-object-id")
    assert etl.is_object_id("0123456789abcdef01234567")
