# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
方案 A（规则兜底策略）的单元测试。

测试分两层：
1. `_RuleScorer` 的四个信号分别正确；
2. `RuleStrategy` 通过 FakeFeatureStore 端到端调用后，返回形状 / 排序 / 版本字段符合约定。
"""

from __future__ import annotations

import asyncio
from typing import List, Tuple

import numpy as np

from recsys_model import RecsysBatch, RecsysEmbeddings
from runners import ACTIONS
from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies.rule_strategy import (
    RuleScorerConfig,
    RuleStrategy,
    _RuleScorer,
)


# ---------------------------------------------------------------- fixtures


def _make_batch(
    hist_author_hashes: np.ndarray,
    hist_surface: np.ndarray,
    cand_author_hashes: np.ndarray,
    cand_surface: np.ndarray,
) -> RecsysBatch:
    """用尽量小的哑数据构造 RecsysBatch，只填测试关心的字段。"""
    batch_size = hist_author_hashes.shape[0]
    history_len = hist_author_hashes.shape[1]
    num_cands = cand_author_hashes.shape[1]
    num_item_hashes = 2
    num_actions = len(ACTIONS)
    num_user_hashes = 2

    return RecsysBatch(
        user_hashes=np.zeros((batch_size, num_user_hashes), dtype=np.int32),
        history_post_hashes=np.zeros(
            (batch_size, history_len, num_item_hashes), dtype=np.int32
        ),
        history_author_hashes=hist_author_hashes.astype(np.int32),
        history_actions=np.zeros(
            (batch_size, history_len, num_actions), dtype=np.float32
        ),
        history_product_surface=hist_surface.astype(np.int32),
        candidate_post_hashes=np.zeros(
            (batch_size, num_cands, num_item_hashes), dtype=np.int32
        ),
        candidate_author_hashes=cand_author_hashes.astype(np.int32),
        candidate_product_surface=cand_surface.astype(np.int32),
    )


class FakeFeatureStore(FeatureStore):
    """给 RuleStrategy 注入可控 batch 的 FakeStore。"""

    def __init__(self, batch: RecsysBatch):
        self._batch = batch

    async def get_user_features(self, *args, **kwargs):  # pragma: no cover - 未用
        raise NotImplementedError

    async def get_candidate_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_user_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_item_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_author_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def build_recsys_batch(self, *args, **kwargs) -> Tuple[RecsysBatch, RecsysEmbeddings]:
        # RuleStrategy 只消费 batch，embeddings 传空占位即可
        empty_emb = RecsysEmbeddings(
            user_embeddings=np.zeros((1, 2, 8), dtype=np.float32),
            history_post_embeddings=np.zeros((1, 4, 2, 8), dtype=np.float32),
            history_author_embeddings=np.zeros((1, 4, 2, 8), dtype=np.float32),
            candidate_post_embeddings=np.zeros((1, 2, 2, 8), dtype=np.float32),
            candidate_author_embeddings=np.zeros((1, 2, 2, 8), dtype=np.float32),
        )
        return self._batch, empty_emb


# ================================================================ _RuleScorer


class TestAffinityScore:
    def test_hit_when_candidate_author_in_history(self):
        # 历史作者 {7, 9}, 候选作者 [7, 100] → [hit, miss]
        batch = _make_batch(
            hist_author_hashes=np.array([[[7, 0], [9, 0]]]),      # [1, 2, 2]
            hist_surface=np.array([[0, 0]]),
            cand_author_hashes=np.array([[[7, 0], [100, 0]]]),    # [1, 2, 2]
            cand_surface=np.array([[0, 0]]),
        )
        scores = _RuleScorer().affinity_score(batch)
        assert scores.shape == (1, 2)
        assert scores[0, 0] == 1.0
        assert scores[0, 1] == 0.0

    def test_padding_zero_not_matched(self):
        # 历史全是 padding（0）→ 任何候选都 miss
        batch = _make_batch(
            hist_author_hashes=np.zeros((1, 2, 2), dtype=np.int32),
            hist_surface=np.array([[0, 0]]),
            cand_author_hashes=np.array([[[0, 0], [5, 0]]]),
            cand_surface=np.array([[0, 0]]),
        )
        scores = _RuleScorer().affinity_score(batch)
        # 第 1 个候选全 0 不应被 "匹配到 padding" 误判为 hit
        assert scores[0, 0] == 0.0
        assert scores[0, 1] == 0.0


class TestSurfaceScore:
    def test_hit_when_candidate_surface_in_top_k(self):
        # 历史 surface 分布：5 出现 3 次，6 出现 1 次 → top-3 = {5, 6, 0}
        batch = _make_batch(
            hist_author_hashes=np.zeros((1, 4, 2), dtype=np.int32),
            hist_surface=np.array([[5, 5, 5, 6]]),
            cand_author_hashes=np.zeros((1, 2, 2), dtype=np.int32),
            cand_surface=np.array([[5, 99]]),
        )
        scores = _RuleScorer().surface_score(batch)
        assert scores[0, 0] == 1.0   # 5 在 top-3
        assert scores[0, 1] == 0.0   # 99 不在

    def test_custom_topk(self):
        # 只看 top-1，99 出现 1 次比 5 的 3 次少，所以 99 不命中
        cfg = RuleScorerConfig(surface_topk=1)
        batch = _make_batch(
            hist_author_hashes=np.zeros((1, 4, 2), dtype=np.int32),
            hist_surface=np.array([[5, 5, 5, 99]]),
            cand_author_hashes=np.zeros((1, 2, 2), dtype=np.int32),
            cand_surface=np.array([[5, 99]]),
        )
        scores = _RuleScorer(config=cfg).surface_score(batch)
        assert scores[0, 0] == 1.0
        assert scores[0, 1] == 0.0


class TestFreshnessScore:
    def test_returns_neutral_when_publish_time_unknown(self):
        scorer = _RuleScorer()  # 无 post_publish_time
        out = scorer.freshness_score(["p1", "p2"], now_ts=1_700_000_000)
        assert out.shape == (1, 2)
        assert np.allclose(out, 0.5)

    def test_decays_by_half_life(self):
        # 半衰期 12h，p1 发布于 12h 前 → 分数 0.5；p2 刚发布 → 分数 1.0
        now = 1_700_000_000
        publish = {"p1": now - 12 * 3600, "p2": now}
        scorer = _RuleScorer(post_publish_time=publish)
        out = scorer.freshness_score(["p1", "p2"], now_ts=now)
        assert np.isclose(out[0, 0], 0.5, atol=1e-4)
        assert np.isclose(out[0, 1], 1.0, atol=1e-4)


class TestDiversityPenalty:
    def test_same_author_repeated_penalized_progressively(self):
        # 候选作者 [7, 7, 7, 8] → 第 1 次不罚，第 2 次 0.5，第 3 次 1.0
        batch = _make_batch(
            hist_author_hashes=np.zeros((1, 2, 2), dtype=np.int32),
            hist_surface=np.array([[0, 0]]),
            cand_author_hashes=np.array([[[7, 0], [7, 0], [7, 0], [8, 0]]]),
            cand_surface=np.array([[0, 0, 0, 0]]),
        )
        penalty = _RuleScorer().diversity_penalty(batch)
        assert penalty.shape == (1, 4)
        assert penalty[0, 0] == 0.0   # 第 1 个 7，无惩罚
        assert penalty[0, 1] == 0.5
        assert penalty[0, 2] == 1.0   # 被 min(.., 1.0) 截断
        assert penalty[0, 3] == 0.0   # 首次出现的作者 8

    def test_zero_author_not_counted(self):
        batch = _make_batch(
            hist_author_hashes=np.zeros((1, 2, 2), dtype=np.int32),
            hist_surface=np.array([[0, 0]]),
            cand_author_hashes=np.array([[[0, 0], [0, 0]]]),
            cand_surface=np.array([[0, 0]]),
        )
        penalty = _RuleScorer().diversity_penalty(batch)
        assert np.all(penalty == 0.0)


class TestComposite:
    def test_score_is_bounded_and_weighted(self):
        # 设计命中 affinity + surface 但无 freshness、无 diversity 惩罚
        batch = _make_batch(
            hist_author_hashes=np.array([[[7, 0], [9, 0]]]),
            hist_surface=np.array([[5, 5]]),
            cand_author_hashes=np.array([[[7, 0], [100, 0]]]),
            cand_surface=np.array([[5, 99]]),
        )
        scorer = _RuleScorer()
        out = scorer.score(batch, ["p1", "p2"])  # [1, 2]
        assert out.shape == (1, 2)
        assert np.all(out >= 0.0) and np.all(out <= 1.0)
        # p1: affinity(1)*0.35 + surface(1)*0.20 + freshness(0.5)*0.25 - 0 = 0.675
        # p2: 0 + 0 + 0.125 = 0.125
        assert np.isclose(out[0, 0], 0.675, atol=1e-4)
        assert np.isclose(out[0, 1], 0.125, atol=1e-4)
        assert out[0, 0] > out[0, 1]


# ================================================================ RuleStrategy


def test_strategy_returns_correct_shape_and_ordering():
    batch = _make_batch(
        hist_author_hashes=np.array([[[7, 0], [9, 0]]]),
        hist_surface=np.array([[5, 5]]),
        cand_author_hashes=np.array([[[100, 0], [7, 0]]]),  # 第二个候选命中 affinity
        cand_surface=np.array([[99, 99]]),
    )
    fake_store = FakeFeatureStore(batch)
    cfg = RankerServiceConfig()  # 默认值足够

    strategy = RuleStrategy(config=cfg, feature_store=fake_store)
    result = asyncio.run(strategy.score("u1", ["p1", "p2"], history_len=2))

    # 形状符合约定：scores [C, num_actions], ranked_indices [C]
    assert result.scores.shape == (2, len(ACTIONS))
    assert result.ranked_indices.shape == (2,)

    # 策略元信息
    assert result.strategy_name == "rule"
    assert result.model_version == "rule-v1"

    # 命中 affinity 的候选应该排第一（索引 1）
    assert int(result.ranked_indices[0]) == 1
    # 所有 action 维度都是同一主分数的复制
    primary = result.scores[:, 0]
    for i in range(len(ACTIONS)):
        assert np.allclose(result.scores[:, i], primary)
