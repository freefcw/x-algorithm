import json

import numpy as np
import pytest

pytest.importorskip("grpc_tools", reason="requires generated recsys proto modules")

from services.grpc_gateway import TWITTER_EPOCH_MS, snowflake_id
from services.published_gateway import PublishedFeatureAdapter
from services.recsys_proto import load_proto_modules

recsys_pb2, _ = load_proto_modules()


def _write_tiny_artifact(path):
    config = {
        "emb_size": 4,
        "history_seq_len": 3,
        "candidate_seq_len": 2,
        "num_actions": 19,
        "product_surface_vocab_size": 16,
        "user_vocab_size": 5,
        "item_vocab_size": 6,
        "author_vocab_size": 7,
        "num_user_hashes": 2,
        "num_item_hashes": 2,
        "num_author_hashes": 2,
        "hash_params": {
            "user_hash_scales": [3, 5],
            "user_biases": [1, 2],
            "user_modulus": 97,
            "item_hash_scales": [7, 11],
            "item_biases": [2, 3],
            "item_modulus": 101,
            "author_hash_scales": [13, 17],
            "author_biases": [3, 4],
            "author_modulus": 103,
        },
    }
    path.mkdir()
    (path / "config.json").write_text(json.dumps(config), encoding="utf-8")
    np.savez(
        path / "embedding_tables.npz",
        user_embeddings=np.ones((5, 4), dtype=np.float32),
        item_embeddings=np.full((6, 4), 2.0, dtype=np.float32),
        author_embeddings=np.full((7, 4), 3.0, dtype=np.float32),
    )


def _make_uas():
    action_mask = [False] * 19
    action_mask[1] = True
    return recsys_pb2.UserActionSequence(
        user_id=42,
        user_actions_data=recsys_pb2.UserActionSequenceDataContainer(
            ordered_aggregated_user_actions_list=recsys_pb2.AggregatedUserActionList(
                aggregated_user_actions=[
                    recsys_pb2.AggregatedUserAction(
                        tweet_id=100,
                        author_id=200,
                        action_mask=action_mask,
                        product_surface=1,
                    )
                ]
            )
        ),
    )


def test_published_feature_adapter_uses_exported_shapes_and_features(tmp_path):
    artifact = tmp_path / "ranker"
    _write_tiny_artifact(artifact)
    adapter = PublishedFeatureAdapter(artifact)
    history = adapter.history_from_uas(_make_uas())
    created_ms = 1_800_000_000_000
    post_id = snowflake_id(created_ms, 1)

    batch = adapter.build_batch(
        42,
        history,
        [post_id],
        [300],
        ranker_features=True,
    )
    embeddings = adapter.lookup(batch)

    assert batch.history_post_hashes.shape == (1, 3, 2)
    assert batch.candidate_post_hashes.shape == (1, 2, 2)
    assert batch.history_continuous_actions.shape == (1, 3, 8)
    assert batch.candidate_post_creation_ts[0, 0] == created_ms // 1000
    assert batch.candidate_post_creation_ts[0, 1] == 0
    assert (post_id >> 22) + TWITTER_EPOCH_MS == created_ms
    assert embeddings.user_embeddings.shape == (1, 2, 4)
    assert embeddings.candidate_post_embeddings.shape == (1, 2, 2, 4)
    assert history.actions[0, 0, 0] == 1.0
