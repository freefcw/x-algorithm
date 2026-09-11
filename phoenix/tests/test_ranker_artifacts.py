"""精排 checkpoint bundle 的完整性和恢复契约。"""

import json
import sys
from pathlib import Path

import jax
import jax.numpy as jnp
import numpy as np
import optax

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import train_ranker as tr
import train_retrieval as retrieval_train
from services.grpc_gateway import resolve_ranker_checkpoint


def _small_embedding_state():
    return {
        name: {
            "table": jnp.full((3, 2), index, dtype=jnp.float32),
            "acc": jnp.full((3,), index + 0.5, dtype=jnp.float32),
        }
        for index, name in enumerate(("user", "post", "author"))
    }


def test_checkpoint_bundle_round_trip(tmp_path):
    params = {"module": {"w": np.ones((2, 2), dtype=np.float32)}}
    optimizer = optax.adam(1e-4)
    opt_state = optimizer.init(params)
    paths = tr.save_artifacts(
        str(tmp_path), params, _small_embedding_state(), opt_state, 7, np.ones(19)
    )

    bundle = tmp_path / "step-000007"
    assert Path(paths["checkpoint_dir"]) == bundle
    assert (bundle / "model_params.npz").exists()
    assert (bundle / "embedding_tables.npz").exists()
    assert (bundle / "optimizer_state.npz").exists()
    assert json.loads((tmp_path / "latest.json").read_text())["checkpoint_dir"] == "step-000007"

    restored = tr.load_embedding_state(str(bundle / "embedding_tables.npz"))
    np.testing.assert_array_equal(restored["post"]["acc"], [1.5, 1.5, 1.5])
    restored_opt = tr.load_optimizer_state(str(bundle / "optimizer_state.npz"), optimizer.init(params))
    assert len(jax.tree_util.tree_leaves(restored_opt)) == len(jax.tree_util.tree_leaves(opt_state))


def test_gateway_rejects_mixed_bundle_inputs(tmp_path):
    bundle = tmp_path / "step-000007"
    bundle.mkdir()
    (bundle / "model_params.npz").write_bytes(b"placeholder")
    (bundle / "embedding_tables.npz").write_bytes(b"placeholder")
    (bundle / "metadata.json").write_text(
        json.dumps(
            {"model_params": "model_params.npz", "embedding_tables": "embedding_tables.npz"}
        ),
        encoding="utf-8",
    )

    with np.testing.assert_raises(ValueError):
        resolve_ranker_checkpoint(str(bundle), str(tmp_path / "other.npz"))

    assert resolve_ranker_checkpoint(str(bundle), None) == (
        str(bundle / "model_params.npz"),
        str(bundle / "embedding_tables.npz"),
    )


def test_retrieval_training_finds_latest_ranker_embedding(tmp_path):
    bundle = tmp_path / "step-000007"
    bundle.mkdir()
    embedding = bundle / "embedding_tables.npz"
    embedding.write_bytes(b"placeholder")
    (tmp_path / "latest.json").write_text(
        json.dumps({"checkpoint_dir": bundle.name}), encoding="utf-8"
    )

    assert retrieval_train.resolve_resume_embedding_path(str(tmp_path)) == str(embedding)
