# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
方案 A：规则兜底精排策略

零训练、零 GPU，仅依赖 `RecsysBatch` 里的历史/候选特征 + 可选的 post 元数据，
为候选集打出 [0, 1] 的综合分数。

设计用途：
    1. 模型权重不可用时的 MVP 链路（本仓库默认无预训练 checkpoint）；
    2. 主模型异常时的在线降级兜底；
    3. 作为 A/B 测试的对照组基线。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

from recsys_model import RecsysBatch
from runners import ACTIONS

from services.config import RankerServiceConfig
from services.feature_store import FeatureStore
from services.ranker_strategies.base import (
    RankingStrategy,
    StrategyError,
    StrategyResult,
)

logger = logging.getLogger("ranker_strategies.rule")


@dataclass
class RuleScorerConfig:
    """规则打分器权重，总和建议约等于 1（多样性惩罚为扣减项）。"""

    w_affinity: float = 0.35
    w_surface: float = 0.20
    w_freshness: float = 0.25
    w_diversity: float = 0.20

    freshness_half_life_hours: float = 12.0
    surface_topk: int = 3


# ---------------------------------------------------------------- 打分实现


class _RuleScorer:
    """内部规则打分逻辑，暴露给外部测试时可单独实例化。"""

    def __init__(
        self,
        config: Optional[RuleScorerConfig] = None,
        post_publish_time: Optional[Dict[str, int]] = None,
    ):
        self.config = config or RuleScorerConfig()
        self.post_publish_time = post_publish_time or {}

    # ---- 单一信号 ----

    def affinity_score(self, batch: RecsysBatch) -> np.ndarray:
        """候选作者是否出现在用户历史作者哈希中，返回 [B, C]。"""
        hist_authors = np.asarray(batch.history_author_hashes)   # [B, S, H]
        cand_authors = np.asarray(batch.candidate_author_hashes)  # [B, C, H]
        batch_size, num_cands, _ = cand_authors.shape
        out = np.zeros((batch_size, num_cands), dtype=np.float32)
        for b in range(batch_size):
            hist_set = {int(x) for x in hist_authors[b].reshape(-1) if x != 0}
            for c in range(num_cands):
                cand_set = {int(x) for x in cand_authors[b, c] if x != 0}
                if cand_set & hist_set:
                    out[b, c] = 1.0
        return out

    def surface_score(self, batch: RecsysBatch) -> np.ndarray:
        """候选 product_surface 是否命中用户历史高频 surface。"""
        hist_sur = np.asarray(batch.history_product_surface)     # [B, S]
        cand_sur = np.asarray(batch.candidate_product_surface)   # [B, C]
        batch_size, num_cands = cand_sur.shape
        out = np.zeros((batch_size, num_cands), dtype=np.float32)
        for b in range(batch_size):
            vals, counts = np.unique(hist_sur[b], return_counts=True)
            order = np.argsort(-counts)
            top = set(int(v) for v in vals[order[: self.config.surface_topk]])
            for c in range(num_cands):
                if int(cand_sur[b, c]) in top:
                    out[b, c] = 1.0
        return out

    def freshness_score(
        self, candidate_ids: List[str], now_ts: Optional[int] = None
    ) -> np.ndarray:
        """按半衰期计算新鲜度。缺失发布时间的候选取中性 0.5。"""
        if now_ts is None:
            now_ts = int(time.time())
        half_life_s = self.config.freshness_half_life_hours * 3600.0
        out = np.zeros((1, len(candidate_ids)), dtype=np.float32)
        for i, cid in enumerate(candidate_ids):
            pub_ts = self.post_publish_time.get(cid)
            if pub_ts is None:
                out[0, i] = 0.5
                continue
            age_s = max(now_ts - int(pub_ts), 0)
            out[0, i] = float(0.5 ** (age_s / half_life_s))
        return out

    def diversity_penalty(self, batch: RecsysBatch) -> np.ndarray:
        """同一作者在候选集里重复出现时，第 2 次起递增惩罚。"""
        cand_authors = np.asarray(batch.candidate_author_hashes)  # [B, C, H]
        batch_size, num_cands, _ = cand_authors.shape
        out = np.zeros((batch_size, num_cands), dtype=np.float32)
        for b in range(batch_size):
            seen: Dict[int, int] = {}
            primary = cand_authors[b, :, 0].tolist()  # 取第 0 个哈希作代表
            for c, author_hash in enumerate(primary):
                author_hash = int(author_hash)
                if author_hash == 0:
                    continue
                cnt = seen.get(author_hash, 0)
                out[b, c] = min(cnt * 0.5, 1.0)
                seen[author_hash] = cnt + 1
        return out

    # ---- 合成 ----

    def score(
        self,
        batch: RecsysBatch,
        candidate_ids: List[str],
        now_ts: Optional[int] = None,
    ) -> np.ndarray:
        cfg = self.config
        affinity = self.affinity_score(batch)
        surface = self.surface_score(batch)
        freshness = self.freshness_score(candidate_ids, now_ts=now_ts)
        penalty = self.diversity_penalty(batch)

        score = (
            cfg.w_affinity * affinity
            + cfg.w_surface * surface
            + cfg.w_freshness * freshness
            - cfg.w_diversity * penalty
        )
        return np.clip(score, 0.0, 1.0).astype(np.float32)


# ---------------------------------------------------------------- 策略包装


class RuleStrategy(RankingStrategy):
    """方案 A：规则兜底策略。"""

    name = "rule"

    def __init__(
        self,
        config: RankerServiceConfig,
        feature_store: FeatureStore,
        scorer_config: Optional[RuleScorerConfig] = None,
        post_metadata_path: Optional[str] = None,
    ):
        self._config = config
        self._feature_store = feature_store
        publish_time = self._load_publish_time(post_metadata_path)
        self._scorer = _RuleScorer(
            config=scorer_config,
            post_publish_time=publish_time,
        )
        logger.info(
            "RuleStrategy initialized (publish_time entries=%d)",
            len(publish_time),
        )

    @staticmethod
    def _load_publish_time(path: Optional[str]) -> Dict[str, int]:
        """可选：从 post 元数据 parquet 加载 `post_id -> publish_ts` 映射。"""
        if not path:
            return {}
        p = Path(path)
        if not p.exists():
            logger.warning("RuleStrategy: post metadata not found at %s", path)
            return {}
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(str(p))
            df = table.to_pandas()
            if "post_id" not in df or "publish_time" not in df:
                logger.warning(
                    "RuleStrategy: %s missing post_id/publish_time columns", path
                )
                return {}
            return {
                str(row["post_id"]): int(row["publish_time"])
                for _, row in df.iterrows()
                if row["publish_time"] is not None
            }
        except Exception as exc:
            logger.warning("RuleStrategy: failed to load %s: %s", path, exc)
            return {}

    async def score(
        self,
        user_id: str,
        candidate_ids: List[str],
        history_len: int = 32,
    ) -> StrategyResult:
        mc = self._config.model
        try:
            batch, _ = await self._feature_store.build_recsys_batch(
                user_id=user_id,
                candidate_ids=candidate_ids,
                history_len=history_len,
                num_actions=len(ACTIONS),
                num_user_hashes=mc.num_user_hashes,
                num_item_hashes=mc.num_item_hashes,
                num_author_hashes=mc.num_author_hashes,
                product_surface_vocab_size=mc.product_surface_vocab_size,
            )
            primary = self._scorer.score(batch, candidate_ids)[0]  # [C]
        except Exception as exc:
            raise StrategyError(f"rule scoring failed: {exc}") from exc

        # 把单一主分数在 num_actions 维上复制，保持与 Phoenix 一致的返回形状
        scores = np.tile(primary[:, None], (1, len(ACTIONS))).astype(np.float32)
        ranked_indices = np.argsort(-primary, kind="stable").astype(np.int32)

        return StrategyResult(
            scores=scores,
            ranked_indices=ranked_indices,
            strategy_name=self.name,
            model_version="rule-v1",
        )
