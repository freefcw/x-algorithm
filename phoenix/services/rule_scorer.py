# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
规则兜底打分器

在精排模型权重不可用（随机初始化）时，提供一个基于启发式信号的打分函数，
使 /v1/rank 的输出"看起来合理"而不是纯随机。

信号来源（仅依赖 RecsysBatch 本身 + 可选的 post 元数据）：
  1. 作者亲和度 (author_affinity)  : 候选作者哈希是否出现在历史作者哈希里
  2. 场景匹配度 (surface_match)    : 候选 product_surface 是否命中历史高频场景
  3. 新鲜度   (freshness)          : 候选发布时间越近分数越高 (需要 post_publish_time)
  4. 多样性惩罚 (diversity_penalty): 同一作者在候选里重复出现时递减
  5. 历史长度先验 (warmup_bonus)   : 有历史记录的用户，启用个性化信号；冷启动用户回落到均匀分布

最终分数 ∈ [0, 1]，可直接作为 ranker_service 响应里的 `overall_score` 和各 `p_*` 字段的值。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np

from recsys_model import RecsysBatch


@dataclass
class RuleScorerConfig:
    """规则打分权重，默认值基于经验调整。"""

    w_affinity: float = 0.35
    w_surface: float = 0.20
    w_freshness: float = 0.25
    w_diversity: float = 0.20  # 惩罚项权重（会被减掉）

    freshness_half_life_hours: float = 12.0
    surface_topk: int = 3

    # 无历史（冷启动）用户的 fallback 分数
    cold_start_score: float = 0.3


class RuleScorer:
    """
    仅依赖 RecsysBatch 本身即可给候选打分。

    用法：
        scorer = RuleScorer()
        scores = scorer.score(batch, candidate_ids)  # shape [B, C]
    """

    def __init__(
        self,
        config: Optional[RuleScorerConfig] = None,
        post_publish_time: Optional[Dict[str, int]] = None,
    ):
        self.config = config or RuleScorerConfig()
        self.post_publish_time: Dict[str, int] = post_publish_time or {}

    # ------------------------------------------------------------------ signals

    def _affinity_score(self, batch: RecsysBatch) -> np.ndarray:
        """历史作者与候选作者的哈希重叠信号，返回 [B, C]。"""
        hist_authors = np.asarray(batch.history_author_hashes)       # [B, S, H]
        cand_authors = np.asarray(batch.candidate_author_hashes)     # [B, C, H]
        B, C, _ = cand_authors.shape
        out = np.zeros((B, C), dtype=np.float32)
        for b in range(B):
            hist_set = set(int(x) for x in hist_authors[b].reshape(-1) if int(x) != 0)
            if not hist_set:
                continue
            for c in range(C):
                cand_set = set(int(x) for x in cand_authors[b, c] if int(x) != 0)
                if cand_set & hist_set:
                    out[b, c] = 1.0
        return out

    def _surface_score(self, batch: RecsysBatch) -> np.ndarray:
        """历史 top-k 高频 surface 与候选 surface 的命中信号，返回 [B, C]。"""
        hist_sur = np.asarray(batch.history_product_surface)         # [B, S]
        cand_sur = np.asarray(batch.candidate_product_surface)       # [B, C]
        B, C = cand_sur.shape
        out = np.zeros((B, C), dtype=np.float32)
        for b in range(B):
            vals, counts = np.unique(hist_sur[b], return_counts=True)
            if vals.size == 0:
                continue
            order = np.argsort(-counts)
            top = set(int(v) for v in vals[order[: self.config.surface_topk]].tolist())
            for c in range(C):
                if int(cand_sur[b, c]) in top:
                    out[b, c] = 1.0
        return out

    def _freshness_score(
        self,
        candidate_ids: List[str],
        now_ts: Optional[int] = None,
    ) -> np.ndarray:
        """发布时间越近分数越高。返回 [1, C]，由调用方按需广播到 [B, C]。"""
        if now_ts is None:
            now_ts = int(time.time())
        half_life = self.config.freshness_half_life_hours * 3600.0
        out = np.full((1, len(candidate_ids)), 0.5, dtype=np.float32)  # 未知发布时间：中性
        for i, cid in enumerate(candidate_ids):
            pub_ts = self.post_publish_time.get(cid)
            if pub_ts is None:
                continue
            age = max(now_ts - pub_ts, 0)
            out[0, i] = float(0.5 ** (age / half_life))
        return out

    def _diversity_penalty(self, batch: RecsysBatch) -> np.ndarray:
        """候选集中同作者重复出现的惩罚（第 k 次重复惩罚 0.5*(k-1)，封顶 1.0）。"""
        cand_authors = np.asarray(batch.candidate_author_hashes)     # [B, C, H]
        B, C, _ = cand_authors.shape
        out = np.zeros((B, C), dtype=np.float32)
        for b in range(B):
            seen: Dict[int, int] = {}
            # 用第 0 列哈希作代表
            primary = cand_authors[b, :, 0].tolist()
            for c, a in enumerate(primary):
                a_int = int(a)
                if a_int == 0:
                    continue
                cnt = seen.get(a_int, 0)
                out[b, c] = min(cnt * 0.5, 1.0)
                seen[a_int] = cnt + 1
        return out

    def _has_history_mask(self, batch: RecsysBatch) -> np.ndarray:
        """每个 batch 行是否有有效历史，返回 [B]。"""
        hist = np.asarray(batch.history_post_hashes)                 # [B, S, H]
        # 任意历史位的任一 hash 非 0 即视为有历史
        return (hist.reshape(hist.shape[0], -1) != 0).any(axis=1)

    # ------------------------------------------------------------------ score

    def score(
        self,
        batch: RecsysBatch,
        candidate_ids: List[str],
        now_ts: Optional[int] = None,
    ) -> np.ndarray:
        """
        返回 [B, C] 综合得分，落在 [0, 1] 近似区间。

        候选位 padding (candidate_post_hashes 全 0) 不单独剔除：
        padding 位的 affinity/surface 自然为 0，分数会很低，排序时自动沉底。
        """
        cfg = self.config
        affinity = self._affinity_score(batch)              # [B, C]
        surface = self._surface_score(batch)                # [B, C]
        freshness = self._freshness_score(candidate_ids, now_ts=now_ts)  # [1, C]
        penalty = self._diversity_penalty(batch)            # [B, C]
        has_history = self._has_history_mask(batch)         # [B]

        B, C = affinity.shape
        freshness_bcast = np.broadcast_to(freshness, (B, C))

        score = (
            cfg.w_affinity * affinity
            + cfg.w_surface * surface
            + cfg.w_freshness * freshness_bcast
            - cfg.w_diversity * penalty
        )

        # 冷启动用户：affinity/surface 全为 0，用 cold_start_score + 新鲜度兜底
        cold_mask = ~has_history
        if cold_mask.any():
            cold_score = cfg.cold_start_score + cfg.w_freshness * freshness_bcast
            score = np.where(cold_mask[:, None], cold_score, score)

        return np.clip(score, 0.0, 1.0).astype(np.float32)

    # ------------------------------------------------------------------ utils

    def score_breakdown(
        self,
        batch: RecsysBatch,
        candidate_ids: List[str],
        now_ts: Optional[int] = None,
    ) -> Dict[str, np.ndarray]:
        """
        返回各分项信号，便于日志和调试。
        分项都是 [B, C]，数值未加权。
        """
        affinity = self._affinity_score(batch)
        surface = self._surface_score(batch)
        freshness = np.broadcast_to(
            self._freshness_score(candidate_ids, now_ts=now_ts),
            affinity.shape,
        )
        penalty = self._diversity_penalty(batch)
        return {
            "affinity": affinity,
            "surface": surface,
            "freshness": np.asarray(freshness),
            "diversity_penalty": penalty,
        }


def load_post_publish_time(parquet_path: str) -> Dict[str, int]:
    """
    从 data/post_metadata.parquet 加载 post_id -> publish_unix_ts 映射。

    若 parquet 中不存在 `created_at` 或 `publish_time` 字段，返回空 dict，
    freshness 信号会退化为中性值 0.5。
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return {}
    try:
        table = pq.read_table(parquet_path)
    except Exception:
        return {}

    cols = set(table.schema.names)
    time_col = None
    for candidate in ("publish_time", "created_at", "post_time", "event_time"):
        if candidate in cols:
            time_col = candidate
            break
    if time_col is None or "post_id" not in cols:
        return {}

    df = table.select(["post_id", time_col]).to_pydict()
    out: Dict[str, int] = {}
    for pid, ts in zip(df["post_id"], df[time_col]):
        if pid is None or ts is None:
            continue
        try:
            out[str(pid)] = int(ts)
        except (TypeError, ValueError):
            continue
    return out
