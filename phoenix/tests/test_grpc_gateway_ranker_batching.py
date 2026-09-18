"""精排折行批处理 + jit 的正确性约束。

折行只是把原来"每 32 条候选一次 B=1 前向"合成一次多行前向；每条候选的分数必须与
原路径（eager、B=1、同一 32 槽位切块）一致，并且不受同批其他行、批次桶 padding 行的
影响。这些性质一旦被破坏，请求照常返回、分数悄悄变化，所以必须用对拍锁住。

注意：演示配置 `right_anchored_rope=False` 下 RoPE 位置按 token 下标递增，同一候选落在
32 槽位中的不同下标会得到不同分数（训练也是如此）。因此对拍的基准必须保持相同切块，
这里不断言"候选位置无关"。
"""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import numpy as np
import pytest

from services.grpc_gateway import (
    RANK_BATCH_BUCKETS,
    RANK_CHUNK,
    EmbeddingTables,
    RankerEngine,
    build_batch,
    build_batch_rows,
    empty_history,
    rank_bucket,
    uas_to_history,
)

# bfloat16 前向在不同 batch 形状下可能有最后一位的舍入差异，容差按 bf16 精度给。
BF16_ATOL = 2e-2


def oid(index: int) -> str:
    return f"{index:024x}"


def candidates(count: int, offset: int = 0):
    return [(oid(1000 + offset + i), oid(5000 + (i % 7))) for i in range(count)]


@pytest.fixture(scope="module")
def tables():
    return EmbeddingTables(None)


@pytest.fixture(scope="module")
def engine(tables):
    # 只预热最小桶，测试里更大的桶在首个命中的请求上编译。
    return RankerEngine(tables, None, warmup_max_bucket=1)


def legacy_chunked_probs(engine, tables, user_id, cands):
    """原网关路径：每 32 条一个 B=1 batch，eager 前向。作为折行 + jit 的对拍基准。"""
    history = uas_to_history(None)
    probs = []
    for start in range(0, len(cands), RANK_CHUNK):
        chunk = cands[start : start + RANK_CHUNK]
        batch = build_batch(
            user_id, history, [c[0] for c in chunk], [c[1] for c in chunk], RANK_CHUNK
        )
        output = engine._runner.rank(batch, tables.lookup(batch))
        probs.extend(np.asarray(output.scores[0], dtype=np.float64)[: len(chunk)])
    return probs


def test_rank_bucket_rounds_up_and_rejects_overflow():
    assert rank_bucket(1) == 1
    assert rank_bucket(3) == 4
    assert rank_bucket(RANK_BATCH_BUCKETS[-1]) == RANK_BATCH_BUCKETS[-1]
    with pytest.raises(ValueError):
        rank_bucket(RANK_BATCH_BUCKETS[-1] + 1)


def test_build_batch_rows_replicates_history_and_pads_rows():
    history = empty_history()
    history.post_hashes[0, 0] = [3, 4]
    rows = [candidates(2), candidates(RANK_CHUNK, offset=10)]
    batch = build_batch_rows("5506dd82fbe78e7de77976ca", history, rows, RANK_CHUNK, num_rows=4)

    assert batch.candidate_post_hashes.shape == (4, RANK_CHUNK, 2)
    assert batch.history_post_hashes.shape == (4, 32, 2)
    assert batch.user_hashes.shape == (4, 2)
    # 每一行都拿到同一份用户 / 历史特征。
    assert (batch.history_post_hashes[:, 0] == [3, 4]).all()
    assert (batch.user_hashes == batch.user_hashes[0]).all()
    # 第 0 行只有 2 条候选，其余槽位是 padding；第 2、3 行整行 padding。
    assert batch.candidate_post_hashes[0, :2].min() > 0
    assert batch.candidate_post_hashes[0, 2:].sum() == 0
    assert batch.candidate_post_hashes[1].min() > 0
    assert batch.candidate_post_hashes[2:].sum() == 0

    # 单行入口与多行入口对同一候选给出同一哈希。
    single = build_batch("u", history, [oid(1)], [oid(2)], RANK_CHUNK)
    multi = build_batch_rows("u", history, [[(oid(1), oid(2))]], RANK_CHUNK)
    np.testing.assert_array_equal(single.candidate_post_hashes, multi.candidate_post_hashes)

    with pytest.raises(ValueError):
        build_batch_rows("u", history, rows, RANK_CHUNK, num_rows=1)
    with pytest.raises(ValueError):
        build_batch_rows("u", history, [candidates(RANK_CHUNK + 1)], RANK_CHUNK)


def test_predict_preserves_order_and_length(engine):
    cands = candidates(70)  # 3 行 → 桶 4
    predictions = engine.predict("5506dd82fbe78e7de77976ca", None, cands)
    assert len(predictions) == 70
    assert all(p.action_probs.shape == (19,) for p in predictions)
    assert all(np.isfinite(p.action_probs).all() for p in predictions)
    assert engine.predict("u", None, []) == []


def test_batched_scores_match_legacy_chunked_scores(engine, tables):
    """70 条候选（3 行 → 桶 4，含 1 个 padding 行）的折行 + jit 结果 = 原逐块 B=1 eager 结果。"""
    user = "5506dd82fbe78e7de77976ca"
    cands = candidates(70)
    batched = engine.predict(user, None, cands)
    reference = legacy_chunked_probs(engine, tables, user, cands)

    assert len(batched) == len(reference) == 70
    for got, expected in zip(batched, reference):
        np.testing.assert_allclose(got.action_probs, expected, atol=BF16_ATOL)


def test_padding_rows_and_other_rows_do_not_leak(engine):
    """行之间互不注意：同一行候选不变时，追加更多行或 padding 行不改变它们的分数。"""
    user = "5506dd82fbe78e7de77976ca"
    base = candidates(RANK_CHUNK)  # 恰好占满第 0 行，槽位不变
    only_one_row = engine.predict(user, None, base)  # 桶 1
    with_more_rows = engine.predict(user, None, base + candidates(70, offset=100))  # 桶 4
    for index in range(RANK_CHUNK):
        np.testing.assert_allclose(
            only_one_row[index].action_probs,
            with_more_rows[index].action_probs,
            atol=BF16_ATOL,
        )


def test_requests_larger_than_the_biggest_bucket_are_split(engine, tables):
    max_per_pass = RANK_BATCH_BUCKETS[-1] * RANK_CHUNK
    cands = candidates(max_per_pass + 3)
    user = "5506dd82fbe78e7de77976ca"
    predictions = engine.predict(user, None, cands)
    assert len(predictions) == max_per_pass + 3

    # 第二个 pass 只有 3 条候选（1 行，桶 1），与原逐块结果一致。
    tail = cands[max_per_pass:]
    reference = legacy_chunked_probs(engine, tables, user, tail)
    for got, expected in zip(predictions[max_per_pass:], reference):
        np.testing.assert_allclose(got.action_probs, expected, atol=BF16_ATOL)


def test_ranker_allows_concurrent_requests_on_one_engine(engine, monkeypatch):
    """两个 gRPC worker 可以同时进入同一个 ranker，而不是被全局锁串行化。"""
    user = "5506dd82fbe78e7de77976ca"
    cands = candidates(RANK_CHUNK)
    barrier = Barrier(2)
    original_rank_rows = engine._rank_rows

    def synchronized_rank_rows(*args, **kwargs):
        barrier.wait(timeout=5)
        return original_rank_rows(*args, **kwargs)

    monkeypatch.setattr(engine, "_rank_rows", synchronized_rank_rows)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(engine.predict, user, None, cands) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]

    assert len(results[0]) == len(results[1]) == len(cands)
    for left, right in zip(results[0], results[1]):
        np.testing.assert_allclose(left.action_probs, right.action_probs, atol=BF16_ATOL)
