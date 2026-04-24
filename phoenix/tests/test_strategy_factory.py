# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
策略工厂 (`services.ranker_strategies.factory`) 与降级组合策略的单元测试。

测试覆盖：
- 别名解析 (`a/b/c/d` 与规范名互通)；
- 未知策略直接抛 StrategyError；
- 初始化期降级：主策略构造失败 → fallback 晋升为主（单策略返回）；
- 无 fallback 时主失败直接向上抛；
- fallback 构造失败：退化为只返回主；
- 主与 fallback 都成功：返回 FallbackStrategy 包装器；
- 运行时降级：主策略 score 抛 StrategyError → FallbackStrategy 调 fallback。
"""

from __future__ import annotations

import asyncio
from typing import List, Tuple

import numpy as np
import pytest

from recsys_model import RecsysBatch, RecsysEmbeddings
from runners import ACTIONS
from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
    create_strategy,
)
from services.ranker_strategies.factory import _ALIASES
from services.ranker_strategies.fallback_strategy import FallbackStrategy
from services.ranker_strategies.rule_strategy import RuleStrategy


# ---------------------------------------------------------------- fakes


class _FakeFeatureStore(FeatureStore):
    """最小可用的 FeatureStore，只实现 build_recsys_batch。"""

    async def get_user_features(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_candidate_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_user_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_item_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def get_author_embeddings(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    async def build_recsys_batch(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int,
        num_actions: int,
        num_user_hashes: int,
        num_item_hashes: int,
        num_author_hashes: int,
        product_surface_vocab_size: int,
    ) -> Tuple[RecsysBatch, RecsysEmbeddings]:
        num_cands = len(candidate_ids)
        batch = RecsysBatch(
            user_hashes=np.zeros((1, num_user_hashes), dtype=np.int32),
            history_post_hashes=np.zeros(
                (1, history_len, num_item_hashes), dtype=np.int32
            ),
            history_author_hashes=np.zeros(
                (1, history_len, num_author_hashes), dtype=np.int32
            ),
            history_actions=np.zeros((1, history_len, num_actions), dtype=np.float32),
            history_product_surface=np.zeros((1, history_len), dtype=np.int32),
            candidate_post_hashes=np.zeros(
                (1, num_cands, num_item_hashes), dtype=np.int32
            ),
            candidate_author_hashes=np.zeros(
                (1, num_cands, num_author_hashes), dtype=np.int32
            ),
            candidate_product_surface=np.zeros((1, num_cands), dtype=np.int32),
        )
        emb = RecsysEmbeddings(
            user_embeddings=np.zeros((1, num_user_hashes, 8), dtype=np.float32),
            history_post_embeddings=np.zeros(
                (1, history_len, num_item_hashes, 8), dtype=np.float32
            ),
            history_author_embeddings=np.zeros(
                (1, history_len, num_author_hashes, 8), dtype=np.float32
            ),
            candidate_post_embeddings=np.zeros(
                (1, num_cands, num_item_hashes, 8), dtype=np.float32
            ),
            candidate_author_embeddings=np.zeros(
                (1, num_cands, num_author_hashes, 8), dtype=np.float32
            ),
        )
        return batch, emb


class _AlwaysFailStrategy(RankingStrategy):
    """score 永远抛 StrategyError，用于模拟运行时失败。"""

    name = "always_fail"

    async def score(self, user_id, candidate_ids, history_len=32):
        raise StrategyError("forced failure for test")


class _ConstantStrategy(RankingStrategy):
    """返回固定分数的策略，用于断言降级是否真的触发到了它。"""

    name = "constant"

    async def score(self, user_id, candidate_ids, history_len=32):
        c = len(candidate_ids)
        scores = np.full((c, len(ACTIONS)), 0.42, dtype=np.float32)
        ranked = np.arange(c, dtype=np.int32)
        return StrategyResult(
            scores=scores,
            ranked_indices=ranked,
            strategy_name=self.name,
            model_version="constant-v0",
        )


@pytest.fixture
def cfg() -> RankerServiceConfig:
    return RankerServiceConfig()


@pytest.fixture
def store() -> FeatureStore:
    return _FakeFeatureStore()


# ================================================================ 别名解析


class TestAliasResolution:
    def test_all_aliases_point_to_known_canonical_names(self):
        valid_canonical = {"phoenix", "rule", "deepctr", "llm"}
        for alias, canonical in _ALIASES.items():
            assert canonical in valid_canonical, f"alias {alias} -> {canonical} invalid"

    def test_letter_shortcuts(self):
        # A/B/C/D 短别名应分别对应 rule/phoenix/deepctr/llm
        assert _ALIASES["a"] == "rule"
        assert _ALIASES["b"] == "phoenix"
        assert _ALIASES["c"] == "deepctr"
        assert _ALIASES["d"] == "llm"

    def test_unknown_strategy_raises(self, cfg, store):
        with pytest.raises(StrategyError) as exc_info:
            create_strategy("unknown_xyz", cfg, store)
        assert "unknown ranker strategy" in str(exc_info.value)


# ================================================================ 初始化期降级


class TestInitTimeFallback:
    def test_deepctr_fails_without_torch_and_falls_back_to_rule(self, cfg, store):
        """
        DeepCTR 策略 __init__ 阶段会因为缺 torch/deepctr_torch 抛 StrategyError。
        当配了 fallback='rule' 时，工厂应直接把 rule 晋升为主策略返回，
        而不是包一层 FallbackStrategy（因为运行时永远也走不到主策略了）。
        """
        strategy = create_strategy("deepctr", cfg, store, fallback="rule")
        assert isinstance(strategy, RuleStrategy)
        assert strategy.name == "rule"

    def test_deepctr_fails_without_fallback_raises(self, cfg, store):
        with pytest.raises(StrategyError):
            create_strategy("deepctr", cfg, store, fallback=None)

    def test_fallback_init_fail_returns_primary_only(self, cfg, store):
        """
        主策略 rule 构造成功；fallback 指向 deepctr 但会失败。
        期望：拿到裸 RuleStrategy（primary），无 FallbackStrategy 包装。
        """
        strategy = create_strategy("rule", cfg, store, fallback="deepctr")
        assert isinstance(strategy, RuleStrategy)


# ================================================================ 组合包装


class TestFallbackComposition:
    def test_both_init_ok_returns_fallback_wrapper(self, cfg, store):
        """主 rule + fallback rule 都能 init，应返回 FallbackStrategy 组合。"""
        strategy = create_strategy("rule", cfg, store, fallback="a")  # 'a' -> rule
        assert isinstance(strategy, FallbackStrategy)
        assert strategy.name == "rule+rule"


# ================================================================ 运行时降级


class TestRuntimeFallback:
    def test_score_failure_triggers_fallback(self):
        primary = _AlwaysFailStrategy()
        fallback = _ConstantStrategy()
        composed = FallbackStrategy(primary=primary, fallback=fallback)

        result = asyncio.run(composed.score("u", ["p1", "p2"], history_len=4))

        # 验证降级确实落到了 _ConstantStrategy
        assert np.allclose(result.scores, 0.42)
        # strategy_name 带箭头标注，便于监控识别
        assert result.strategy_name == "always_fail->constant"
        assert result.model_version == "constant-v0"

    def test_primary_success_no_fallback_trigger(self):
        primary = _ConstantStrategy()
        # 用 AlwaysFail 作 fallback，验证主成功时 fallback 不被调用
        fallback = _AlwaysFailStrategy()
        composed = FallbackStrategy(primary=primary, fallback=fallback)

        result = asyncio.run(composed.score("u", ["p1"], history_len=2))
        # 主策略名直接透传，没有箭头
        assert result.strategy_name == "constant"
