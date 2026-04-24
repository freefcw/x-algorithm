# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
规则兜底打分器单元测试

覆盖：
- 候选作者亲和度信号
- 场景匹配度信号
- 新鲜度信号
- 多样性惩罚
- 冷启动回落
- 综合分值范围 [0, 1]
"""

from __future__ import annotations

import time

import numpy as np
import pytest

from recsys_model import RecsysBatch
from services.rule_scorer import RuleScorer, RuleScorerConfig


# ------------------------------------------------------------------ fixtures

def _make_batch(
    user_hashes,
    history_post_hashes,
    history_author_hashes,
    history_product_surface,
    candidate_post_hashes,
    candidate_author_hashes,
    candidate_product_surface,
    history_len: int = 4,
    num_candidates: int = 3,
    num_actions: int = 19,
) -> RecsysBatch:
    """构造一个最小 RecsysBatch，用于规则分测试。"""
    history_actions = np.zeros((1, history_len, num_actions), dtype=np.float32)
    return RecsysBatch(
        user_hashes=np.asarray(user_hashes, dtype=np.int32),
        history_post_hashes=np.asarray(history_post_hashes, dtype=np.int32),
        history_author_hashes=np.asarray(history_author_hashes, dtype=np.int32),
        history_actions=history_actions,
        history_product_surface=np.asarray(history_product_surface, dtype=np.int32),
        candidate_post_hashes=np.asarray(candidate_post_hashes, dtype=np.int32),
        candidate_author_hashes=np.asarray(candidate_author_hashes, dtype=np.int32),
        candidate_product_surface=np.asarray(candidate_product_surface, dtype=np.int32),
    )


# ------------------------------------------------------------------ affinity

def test_affinity_hits_history_author():
    """候选作者哈希出现在历史里 → affinity=1，其他=0。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],        # 历史作者: {100, 101}
        history_product_surface=[[5, 5, 5, 5]],
        candidate_post_hashes=[[[20, 21], [22, 23], [24, 25]]],
        candidate_author_hashes=[[[100, 999],            # 命中历史 (100)
                                   [888, 888],            # 未命中
                                   [101, 555]]],          # 命中历史 (101)
        candidate_product_surface=[[5, 9, 9]],
        num_candidates=3,
    )
    scorer = RuleScorer()
    affinity = scorer._affinity_score(batch)
    assert affinity.shape == (1, 3)
    assert affinity[0, 0] == 1.0
    assert affinity[0, 1] == 0.0
    assert affinity[0, 2] == 1.0


def test_affinity_ignores_padding():
    """padding 位 (0) 不应被当作命中。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[0, 0]] * 4],              # 全 padding
        history_author_hashes=[[[0, 0]] * 4],
        history_product_surface=[[0, 0, 0, 0]],
        candidate_post_hashes=[[[20, 21]]],
        candidate_author_hashes=[[[0, 0]]],              # 0 不应命中
        candidate_product_surface=[[0]],
        num_candidates=1,
    )
    scorer = RuleScorer()
    affinity = scorer._affinity_score(batch)
    assert affinity[0, 0] == 0.0


# ------------------------------------------------------------------ surface

def test_surface_match_uses_top_frequencies():
    """候选 surface 命中历史 top-k 高频 → surface=1。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],
        history_product_surface=[[5, 5, 5, 9]],           # 5 出现 3 次, 9 出现 1 次
        candidate_post_hashes=[[[20, 21], [22, 23], [24, 25]]],
        candidate_author_hashes=[[[111, 222], [333, 444], [555, 666]]],
        candidate_product_surface=[[5, 9, 15]],           # 5, 9 都在 top-3；15 不在
        num_candidates=3,
    )
    scorer = RuleScorer(config=RuleScorerConfig(surface_topk=3))
    surface = scorer._surface_score(batch)
    assert surface[0, 0] == 1.0
    assert surface[0, 1] == 1.0
    assert surface[0, 2] == 0.0


# ------------------------------------------------------------------ freshness

def test_freshness_decays_with_age():
    """越近发布的候选新鲜度越高，未知发布时间的候选为中性 0.5。"""
    now = 1_700_000_000
    publish_time = {
        "p_fresh": now - 60,           # 1 分钟前
        "p_stale": now - 48 * 3600,    # 2 天前
        # p_unknown 故意不放入
    }
    scorer = RuleScorer(
        config=RuleScorerConfig(freshness_half_life_hours=12.0),
        post_publish_time=publish_time,
    )
    f = scorer._freshness_score(["p_fresh", "p_stale", "p_unknown"], now_ts=now)
    assert f.shape == (1, 3)
    assert f[0, 0] > 0.95                             # 刚发布 → 接近 1
    assert f[0, 1] < 0.1                              # 两天前 → 大幅衰减
    assert abs(f[0, 2] - 0.5) < 1e-6                  # 未知 → 0.5


# ------------------------------------------------------------------ diversity

def test_diversity_penalizes_repeated_author():
    """同一作者在候选中第二次起，惩罚递增。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],
        history_product_surface=[[5, 5, 5, 5]],
        candidate_post_hashes=[[[20, 21], [22, 23], [24, 25], [26, 27]]],
        candidate_author_hashes=[[[777, 0],               # A
                                   [777, 0],               # A (重复 1)
                                   [777, 0],               # A (重复 2)
                                   [888, 0]]],             # B (首次)
        candidate_product_surface=[[5, 5, 5, 5]],
        num_candidates=4,
    )
    scorer = RuleScorer()
    penalty = scorer._diversity_penalty(batch)
    assert penalty[0, 0] == 0.0       # 首次出现，无惩罚
    assert penalty[0, 1] == 0.5       # 第二次 → 0.5
    assert penalty[0, 2] == 1.0       # 第三次 → 1.0 (封顶)
    assert penalty[0, 3] == 0.0       # 不同作者


# ------------------------------------------------------------------ cold start

def test_cold_start_user_gets_neutral_score():
    """无历史的用户 (历史全 padding) 走 cold_start_score 分支。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[0, 0]] * 4],               # 全 padding
        history_author_hashes=[[[0, 0]] * 4],
        history_product_surface=[[0, 0, 0, 0]],
        candidate_post_hashes=[[[20, 21], [22, 23]]],
        candidate_author_hashes=[[[111, 222], [333, 444]]],
        candidate_product_surface=[[5, 9]],
        num_candidates=2,
    )
    cfg = RuleScorerConfig(cold_start_score=0.3, w_freshness=0.25)
    scorer = RuleScorer(config=cfg)
    scores = scorer.score(batch, ["c1", "c2"])
    # 无新鲜度信息 → 每个候选都应落在 cold_start + 0.25 * 0.5 附近
    expected = cfg.cold_start_score + cfg.w_freshness * 0.5
    np.testing.assert_allclose(scores[0], [expected, expected], atol=1e-6)


# ------------------------------------------------------------------ integration

def test_score_is_bounded_and_shape_correct():
    """综合分应在 [0, 1] 区间，形状为 [B, C]。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],
        history_product_surface=[[5, 5, 9, 9]],
        candidate_post_hashes=[[[20, 21], [22, 23], [24, 25]]],
        candidate_author_hashes=[[[100, 0], [888, 0], [100, 0]]],
        candidate_product_surface=[[5, 15, 9]],
        num_candidates=3,
    )
    scorer = RuleScorer()
    scores = scorer.score(batch, ["c1", "c2", "c3"])
    assert scores.shape == (1, 3)
    assert scores.min() >= 0.0
    assert scores.max() <= 1.0


def test_score_differentiates_candidates():
    """
    端到端：命中历史作者 + 命中 top surface 的候选应显著高于
    只命中多样性惩罚的候选。
    """
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],         # 历史: {100, 101}
        history_product_surface=[[5, 5, 5, 5]],           # 历史 top surface: {5}
        candidate_post_hashes=[[[20, 21], [22, 23], [24, 25]]],
        candidate_author_hashes=[[[100, 0],                # 命中 affinity
                                   [888, 0],                # 不命中
                                   [888, 0]]],              # 不命中 + 重复作者惩罚
        candidate_product_surface=[[5, 15, 15]],          # 只有第一个命中 surface
        num_candidates=3,
    )
    scorer = RuleScorer()
    scores = scorer.score(batch, ["good", "mid", "bad"])[0]
    assert scores[0] > scores[1], f"亲和+场景候选应更高: {scores}"
    assert scores[1] > scores[2], f"无惩罚应高于有惩罚: {scores}"


def test_score_breakdown_returns_all_signals():
    """score_breakdown 应返回所有分项信号，形状匹配。"""
    batch = _make_batch(
        user_hashes=[[1, 2]],
        history_post_hashes=[[[10, 11]] * 4],
        history_author_hashes=[[[100, 101]] * 4],
        history_product_surface=[[5, 5, 5, 5]],
        candidate_post_hashes=[[[20, 21], [22, 23]]],
        candidate_author_hashes=[[[100, 0], [888, 0]]],
        candidate_product_surface=[[5, 15]],
        num_candidates=2,
    )
    scorer = RuleScorer()
    bd = scorer.score_breakdown(batch, ["c1", "c2"])
    for key in ("affinity", "surface", "freshness", "diversity_penalty"):
        assert key in bd
        assert bd[key].shape == (1, 2)
