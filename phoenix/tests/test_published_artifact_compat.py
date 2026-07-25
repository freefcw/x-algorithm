# Copyright 2026 X.AI Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig
from runners import (
    ModelRunner,
    RecsysInferenceRunner,
    create_dummy_batch_from_config,
    create_dummy_embeddings_from_config,
    create_example_batch,
    load_embedding_table,
    load_model_params,
)


def test_load_model_params_reconstructs_haiku_parameter_tree(tmp_path):
    checkpoint = tmp_path / "model_params.npz"
    np.savez(
        checkpoint,
        **{
            "phoenix/linear/w": np.arange(6, dtype=np.float32).reshape(2, 3),
            "phoenix/linear/b": np.ones(3, dtype=np.float32),
            "head/w": np.full((3, 1), 2.0, dtype=np.float32),
        },
    )

    params = load_model_params(checkpoint)

    np.testing.assert_array_equal(
        np.asarray(params["phoenix/linear"]["w"]),
        np.arange(6, dtype=np.float32).reshape(2, 3),
    )
    np.testing.assert_array_equal(
        np.asarray(params["phoenix/linear"]["b"]), np.ones(3, dtype=np.float32)
    )
    assert params["head"]["w"].dtype == jnp.float32


def test_legacy_dummy_inputs_leave_published_features_disabled():
    hash_config = HashConfig()

    batch = create_dummy_batch_from_config(
        hash_config=hash_config,
        history_len=4,
        num_candidates=2,
        num_actions=19,
    )
    embeddings = create_dummy_embeddings_from_config(
        hash_config=hash_config,
        emb_size=8,
        history_len=4,
        num_candidates=2,
    )

    assert batch.history_continuous_actions is None
    assert batch.candidate_impr_ts is None
    assert batch.candidate_post_creation_ts is None
    assert batch.user_ip_hashes is None
    assert embeddings.user_ip_embeddings is None


def _tiny_model_config(**feature_overrides):
    return PhoenixModelConfig(
        model=TransformerConfig(
            emb_size=8,
            key_size=4,
            num_q_heads=2,
            num_kv_heads=2,
            num_layers=1,
            widening_factor=2,
            attn_output_multiplier=0.125,
        ),
        emb_size=8,
        num_actions=19,
        history_seq_len=4,
        candidate_seq_len=2,
        hash_config=HashConfig(),
        fprop_dtype=jnp.float32,
        **feature_overrides,
    )


def _tiny_model_inputs():
    batch, embeddings = create_example_batch(
        batch_size=1,
        emb_size=8,
        history_len=4,
        num_candidates=2,
        num_actions=19,
        num_user_embeddings=100,
        num_post_embeddings=100,
        num_author_embeddings=100,
    )
    return batch, embeddings


def _run_tiny_model(config, batch, embeddings):
    forward = hk.without_apply_rng(
        hk.transform(lambda inputs, looked_up: config.make()(inputs, looked_up))
    )
    rng = jax.random.PRNGKey(7)
    params = forward.init(rng, batch, embeddings)
    return forward.apply(params, batch, embeddings), params


def _parameter_names(params):
    return {
        f"{module_name}/{parameter_name}"
        for module_name, module_params in params.items()
        for parameter_name in module_params
    }


def test_legacy_model_profile_keeps_continuous_head_disabled():
    config = _tiny_model_config()
    batch, embeddings = _tiny_model_inputs()

    output, params = _run_tiny_model(config, batch, embeddings)

    parameter_names = _parameter_names(params)
    assert output.logits.shape == (1, 2, 19)
    assert output.continuous_preds is None
    assert not any("continuous_unembeddings" in name for name in parameter_names)
    assert not any("post_age_embedding_table" in name for name in parameter_names)


def test_published_model_profile_enables_new_model_semantics():
    config = _tiny_model_config(
        enable_post_age=True,
        enable_continuous_actions=True,
        enable_continuous_predictions=True,
        right_anchored_rope=True,
    )
    batch, embeddings = _tiny_model_inputs()
    batch = batch._replace(
        history_continuous_actions=np.ones((1, 4, 8), dtype=np.float32),
        candidate_impr_ts=np.full((1, 2), 10_000, dtype=np.int64),
        candidate_post_creation_ts=np.array([[9_000, 8_000]], dtype=np.int64),
    )

    output, params = _run_tiny_model(config, batch, embeddings)

    parameter_names = _parameter_names(params)
    assert output.logits.shape == (1, 2, 19)
    assert output.continuous_preds is not None
    assert output.continuous_preds.shape == (1, 2, 8)
    assert any("continuous_unembeddings" in name for name in parameter_names)
    assert any("post_age_embedding_table" in name for name in parameter_names)


def test_inference_runner_loads_published_checkpoint_directly(tmp_path):
    config = _tiny_model_config(
        enable_post_age=True,
        enable_continuous_actions=True,
        enable_continuous_predictions=True,
    )
    batch, embeddings = _tiny_model_inputs()
    _, expected_params = _run_tiny_model(config, batch, embeddings)
    checkpoint = tmp_path / "model_params.npz"
    np.savez(
        checkpoint,
        **{
            f"{module_name}/{parameter_name}": np.asarray(value)
            for module_name, module_params in expected_params.items()
            for parameter_name, value in module_params.items()
        },
    )
    runner = RecsysInferenceRunner(
        runner=ModelRunner(model=config, bs_per_device=0.125),
        name="published-checkpoint-test",
    )

    runner.initialize(checkpoint_path=checkpoint)

    assert _parameter_names(runner.params) == _parameter_names(expected_params)
    for module_name, module_params in expected_params.items():
        for parameter_name, expected in module_params.items():
            np.testing.assert_array_equal(
                np.asarray(runner.params[module_name][parameter_name]),
                np.asarray(expected),
            )


def test_load_embedding_table_preserves_named_tables(tmp_path):
    checkpoint = tmp_path / "embedding_tables.npz"
    expected = {
        "user_embeddings": np.ones((2, 4), dtype=np.float32),
        "item_embeddings": np.full((3, 4), 2.0, dtype=np.float32),
        "author_embeddings": np.full((5, 4), 3.0, dtype=np.float32),
    }
    np.savez(checkpoint, **expected)

    tables = load_embedding_table(checkpoint)

    assert set(tables) == set(expected)
    for name, values in expected.items():
        np.testing.assert_array_equal(tables[name], values)
