"""从零训练的可行性约束：初始化必须打破对称、嵌入表必须被更新、掩码必须生效。"""

import sys
from pathlib import Path

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import train_ranker as tr


@pytest.fixture(scope="module")
def setup():
    loss_transform = hk.without_apply_rng(hk.transform(tr.loss_fn))
    emb_state = tr.init_embedding_state(*tr.init_embedding_tables(seed=0))
    rng = np.random.default_rng(0)
    batch, labels = tr.make_simulated_batch_fast(4, rng)
    batch = jax.tree_util.tree_map(jnp.asarray, batch)
    labels = jnp.asarray(labels)
    head_mask = jnp.asarray(tr.resolve_head_mask("favorite,reply"))
    params = loss_transform.init(
        jax.random.PRNGKey(0),
        batch,
        tr.lookup_embeddings_jax(emb_state, batch),
        labels,
        head_mask,
    )
    return loss_transform, params, emb_state, batch, labels, head_mask


def test_transformer_is_randomly_initialized_not_zero(setup):
    _, params, *_ = setup
    flat = tr.flatten_dict(params)
    transformer_weights = [k for k in flat if k.startswith("transformer/") and k.endswith("/w")]
    assert transformer_weights
    for key in transformer_weights:
        assert np.abs(flat[key]).mean() > 0, key
    for key in (k for k in flat if k.endswith("rms_norm/scale") or "/rms_norm_" in k):
        assert np.allclose(flat[key], 1.0), key


def test_train_step_moves_transformer_and_touched_embedding_rows(setup):
    loss_transform, params, emb_state, batch, labels, head_mask = setup

    def loss(p, embeddings):
        return loss_transform.apply(p, batch, embeddings, labels, head_mask)

    embeddings = tr.lookup_embeddings_jax(emb_state, batch)
    _, (param_grads, emb_grads) = jax.value_and_grad(loss, argnums=(0, 1))(params, embeddings)

    flat_grads = tr.flatten_dict(param_grads)
    assert np.abs(flat_grads["transformer/decoder_layer_0/multi_head_attention/query/w"]).sum() > 0
    assert np.abs(flat_grads["transformer/decoder_layer_0/linear/w"]).sum() > 0

    new_state = tr.apply_embedding_grads(emb_state, batch, emb_grads, lr=0.05)
    old_post = np.asarray(emb_state["post"]["table"])
    new_post = np.asarray(new_state["post"]["table"])
    touched = np.unique(
        np.concatenate(
            [
                np.asarray(batch.history_post_hashes).reshape(-1),
                np.asarray(batch.candidate_post_hashes).reshape(-1),
            ]
        )
    )
    touched = touched[touched != 0]
    untouched = np.setdiff1d(np.arange(old_post.shape[0]), np.append(touched, 0))

    assert np.abs(new_post[touched] - old_post[touched]).sum() > 0
    assert np.array_equal(new_post[untouched], old_post[untouched])
    assert np.all(new_post[0] == 0)


def test_loss_ignores_padding_candidates_and_unobserved_heads(setup):
    loss_transform, params, emb_state, batch, labels, head_mask = setup
    embeddings = tr.lookup_embeddings_jax(emb_state, batch)

    # 把最后一个候选打成 padding（哈希 0），其标签怎么改都不应影响损失
    padded = batch._replace(
        candidate_post_hashes=batch.candidate_post_hashes.at[:, -1, :].set(0)
    )
    padded_embeddings = tr.lookup_embeddings_jax(emb_state, padded)
    base = loss_transform.apply(params, padded, padded_embeddings, labels, head_mask)
    flipped = labels.at[:, -1, :].set(1.0 - labels[:, -1, :])
    assert np.allclose(
        base, loss_transform.apply(params, padded, padded_embeddings, flipped, head_mask)
    )

    # 未观测头（repost = 下标 2）的标签变化不影响损失
    base = loss_transform.apply(params, batch, embeddings, labels, head_mask)
    flipped = labels.at[:, :, 2].set(1.0 - labels[:, :, 2])
    assert np.allclose(base, loss_transform.apply(params, batch, embeddings, flipped, head_mask))

    # 观测头（favorite = 下标 0）的标签变化必须影响损失
    flipped = labels.at[:, :, 0].set(1.0 - labels[:, :, 0])
    assert not np.allclose(
        base, loss_transform.apply(params, batch, embeddings, flipped, head_mask)
    )


def test_resolve_head_mask_and_supported_enums():
    mask = tr.resolve_head_mask("favorite, reply_score")
    assert mask.sum() == 2 and mask[0] == 1 and mask[1] == 1
    assert tr.observed_action_names(mask) == ["favorite_score", "reply_score"]
    assert tr.supported_action_enums(mask) == [1, 2]
    assert tr.resolve_head_mask(None).sum() == tr.NUM_ACTIONS
    with pytest.raises(ValueError):
        tr.resolve_head_mask("nonexistent")


def test_ranking_metrics_do_not_reward_constant_scores():
    valid = np.ones((3, 8), dtype=bool)
    positive = np.zeros((3, 8), dtype=bool)
    positive[:, 0] = True
    constant = tr.ranking_metrics(np.zeros((3, 8)), valid, positive)
    assert constant["hr@1"] == pytest.approx(1 / 8)
    assert constant["random_hr@1"] == pytest.approx(1 / 8)

    perfect = np.zeros((3, 8))
    perfect[:, 0] = 1.0
    assert tr.ranking_metrics(perfect, valid, positive)["hr@1"] == 1.0
    assert tr.ranking_metrics(perfect, valid, positive)["mrr"] == 1.0

    second = perfect.copy()
    second[:, 1] = 2.0
    assert tr.ranking_metrics(second, valid, positive)["mrr"] == pytest.approx(0.5)


def test_shuffle_candidates_moves_positive_and_keeps_rows_aligned():
    rng = np.random.default_rng(0)
    batch, labels = tr.make_simulated_batch_fast(64, rng)
    labels[:] = 0.0
    labels[:, 0, 0] = 1.0  # 正例固定在第 0 槽
    marker = np.asarray(batch.candidate_post_hashes)[:, 0, :].copy()

    shuffled, shuffled_labels = tr.shuffle_candidates(batch, labels, np.random.default_rng(1))

    positive_slot = np.argmax(shuffled_labels[:, :, 0], axis=1)
    assert len(np.unique(positive_slot)) > 1, "正例槽位必须被打乱"
    # 标签和候选特征必须一起移动：正例槽位上的哈希仍是原第 0 槽的哈希
    rows = np.arange(64)
    assert np.array_equal(
        np.asarray(shuffled.candidate_post_hashes)[rows, positive_slot], marker
    )
    assert np.array_equal(
        np.sort(shuffled.candidate_author_hashes, axis=1),
        np.sort(batch.candidate_author_hashes, axis=1),
    )
    assert np.array_equal(shuffled.history_post_hashes, batch.history_post_hashes)


def test_binary_auc_matches_known_values():
    assert tr.binary_auc(np.array([0.1, 0.9]), np.array([0, 1])) == 1.0
    assert tr.binary_auc(np.array([0.9, 0.1]), np.array([0, 1])) == 0.0
    assert tr.binary_auc(np.array([0.5, 0.5]), np.array([0, 1])) == 0.5
    assert tr.binary_auc(np.array([0.5, 0.5]), np.array([1, 1])) is None


def test_optimizer_state_round_trip(tmp_path):
    import optax

    params = {"weight": jnp.ones((2,), dtype=jnp.float32)}
    optimizer = optax.adam(1e-4)
    state = optimizer.init(params)
    _, state = optimizer.update({"weight": jnp.full((2,), 0.5)}, state, params)
    path = tmp_path / "optimizer_state.npz"

    tr.save_optimizer_state(str(path), state)
    restored = tr.load_optimizer_state(str(path), optimizer.init(params))
    assert all(
        np.array_equal(np.asarray(expected), np.asarray(actual))
        for expected, actual in zip(
            jax.tree_util.tree_leaves(state), jax.tree_util.tree_leaves(restored)
        )
    )
