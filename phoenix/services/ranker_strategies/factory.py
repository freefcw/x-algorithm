# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
策略工厂

通过字符串名称构造策略实例；支持主策略 + 兜底策略的组合。
"""

from __future__ import annotations

import logging
from typing import Optional

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies.base import RankingStrategy, StrategyError
from services.ranker_strategies.fallback_strategy import FallbackStrategy

logger = logging.getLogger("ranker_strategies.factory")

_ALIASES = {
    "phoenix": "phoenix",
    "b": "phoenix",
    "rule": "rule",
    "a": "rule",
    "deepctr": "deepctr",
    "din": "deepctr",
    "c": "deepctr",
    "llm": "llm",
    "d": "llm",
}


def _create_single(
    name: str,
    config: RankerServiceConfig,
    feature_store: FeatureStore,
) -> RankingStrategy:
    """构造单一策略，不处理降级。"""
    canonical = _ALIASES.get(name.lower())
    if canonical is None:
        raise StrategyError(
            f"unknown ranker strategy '{name}'; "
            f"supported: {sorted(set(_ALIASES.values()))}"
        )

    # 惰性 import，避免 torch / deepctr 等可选依赖在启动时强加载
    if canonical == "phoenix":
        from services.ranker_strategies.phoenix_strategy import PhoenixStrategy

        return PhoenixStrategy(config=config, feature_store=feature_store)

    if canonical == "rule":
        from services.ranker_strategies.rule_strategy import RuleStrategy

        return RuleStrategy(
            config=config,
            feature_store=feature_store,
            post_metadata_path=getattr(config, "rule_post_metadata_path", None),
        )

    if canonical == "deepctr":
        from services.ranker_strategies.deepctr_strategy import DeepCTRStrategy

        return DeepCTRStrategy(config=config, feature_store=feature_store)

    if canonical == "llm":
        from services.ranker_strategies.llm_strategy import LLMStrategy

        return LLMStrategy(config=config, feature_store=feature_store)

    # 防御性分支（理论上不会到这里）
    raise StrategyError(f"strategy '{canonical}' has no constructor wired")


def create_strategy(
    name: str,
    config: RankerServiceConfig,
    feature_store: FeatureStore,
    fallback: Optional[str] = None,
) -> RankingStrategy:
    """
    根据名称创建策略；若传入 `fallback`，会自动包一层 `FallbackStrategy`。

    Args:
        name: 主策略名（见 `_ALIASES`）。
        config: 服务配置。
        feature_store: 特征存储实例。
        fallback: 可选的兜底策略名。主策略初始化失败时也会降级到 fallback。

    Returns:
        `RankingStrategy` 实例。
    """
    try:
        primary = _create_single(name, config, feature_store)
    except StrategyError as exc:
        if not fallback:
            raise
        logger.warning(
            "primary strategy '%s' init failed (%s); using fallback '%s' as primary",
            name,
            exc,
            fallback,
        )
        return _create_single(fallback, config, feature_store)

    if not fallback:
        return primary

    try:
        backup = _create_single(fallback, config, feature_store)
    except StrategyError as exc:
        logger.warning(
            "fallback strategy '%s' init failed (%s); running without fallback",
            fallback,
            exc,
        )
        return primary

    logger.info("ranker strategy=%s, fallback=%s", primary.name, backup.name)
    return FallbackStrategy(primary=primary, fallback=backup)
