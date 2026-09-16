# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
数据预处理脚本

将原始行为日志和帖子元数据转换为模型训练所需的格式。

输入：
    - data/behavior_logs/dt=YYYY-MM-DD/*.parquet  (行为事件表)
    - data/post_metadata.parquet                    (帖子元数据表)
    - data/impressions/dt=YYYY-MM-DD/*.parquet    (可选：曝光表，一行一条下发候选 + 归因标签)

输出：
    - data/training_samples/train_YYYYMMDD.parquet  (训练样本)

两种样本构造方式：
    - 只有行为日志：每条正向互动一条样本，负样本从帖子元数据池随机采（模型学的是"互动 vs 随机"）。
      行为日志若是事件表（带 action_type 列，一行一个行为），先按 (user, post) 合并成多热行。
    - 提供曝光表（--impressions-dir，由 scripts/build_training_inputs.py 产出）：每次下发请求一条样本，
      候选就是这次真正下发的帖子，标签是归因窗口内的行为，没有行为的下发候选就是负样本
      （模型学的是"看到并互动 vs 看到但没互动"）；行为日志只用来构造历史序列，并以请求时间为
      截止按帖子聚合（与线上 DefaultAggregator 同构），请求之后的行为不会进入历史。

使用示例：
    uv run data_preprocessor.py --behavior-dir data/behavior_logs \
        --post-meta data/post_metadata.parquet --output-dir data/training_samples
    uv run data_preprocessor.py --behavior-dir data/behavior_logs \
        --impressions-dir data/impressions --post-meta data/post_metadata.parquet \
        --output-dir data/training_samples
"""

import argparse
import bisect
import functools
import hashlib
import logging
import random
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from runners import ACTIONS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("preprocess")


# ── 配置常量 ──────────────────────────────────────────────────────────────────

HISTORY_SEQ_LEN = 32       # 历史序列长度
CANDIDATE_SEQ_LEN = 8      # 候选集长度（1正+7负）
NUM_ACTIONS = len(ACTIONS)  # 19种行为
NUM_HASHES = 2             # 每个ID的哈希数量
TABLE_SIZE = 100_000       # 哈希表大小（与训练脚本一致）
SURFACE_VOCAB = 16         # 场景词汇表大小
MAX_AGE_DAYS = 7           # 负样本候选的最大帖龄（生产取值需与 Home Mixer AgeFilter 对齐）
HISTORY_WINDOW_DAYS = 7    # 曝光模式下历史序列只看请求前 N 天（与线上 UAS 保留窗口一致）

# 行为字段名映射（输入日志字段 -> 内部使用）
BEHAVIOR_FIELDS = [
    "favorite",
    "reply",
    "repost",
    "photo_expand",
    "click",
    "profile_click",
    "vqv",
    "share",
    "share_via_dm",
    "share_via_copy_link",
    "dwell",
    "quote",
    "quoted_click",
    "follow_author",
    "not_interested",
    "block_author",
    "mute_author",
    "report",
    "dwell_time",
]


# ── 核心工具函数 ───────────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=2_000_000)
def _hash_id_cached(id_str: str, num_hashes: int, table_size: int) -> tuple[int, ...]:
    """lru_cache 友好的内部实现，返回 tuple 以便 hashable。"""
    id_bytes = id_str.encode("utf-8")
    hashes = []
    for i in range(num_hashes):
        seed_bytes = id_bytes + f"_hash{i}".encode()
        hash_val = int(hashlib.md5(seed_bytes).hexdigest(), 16)
        # 映射到 [1, table_size]，0 留给 padding
        hashes.append((hash_val % table_size) + 1)
    return tuple(hashes)


def hash_id_to_ints(id_str: str, num_hashes: int = NUM_HASHES, table_size: int = TABLE_SIZE) -> list[int]:
    """
    将字符串ID哈希成多个整数（用于嵌入表查询）。

    使用consistent hash确保同一ID总是映射到相同的哈希值。
    返回的哈希值范围: [1, table_size]，0保留给padding。

    内部用 lru_cache 缓存 md5 结果，热门 id 反复出现时大幅加速。
    """
    return list(_hash_id_cached(str(id_str), num_hashes, table_size))


def pad_sequence(seq: list, target_len: int, pad_value) -> list:
    """将序列padding或截断到目标长度。"""
    if len(seq) >= target_len:
        return seq[:target_len]
    return seq + [pad_value] * (target_len - len(seq))


def normalize_dwell(seconds: float, max_seconds: float = 300.0) -> float:
    """归一化停留时长到 [0, 1]。"""
    return min(seconds / max_seconds, 1.0)


def deterministic_user_seed(base_seed: int, user_id: str) -> int:
    """
    用 hashlib 派生每个用户的确定性 RNG 种子。

    Python 内建 hash(str) 受 PYTHONHASHSEED 影响，每个进程结果不同，
    会破坏预处理的可复现性；这里用 md5 替代。
    """
    h = int(hashlib.md5(user_id.encode("utf-8")).hexdigest()[:12], 16)
    return (base_seed + h) & 0xFFFFFFFF


def _to_float_or_zero(raw) -> float:
    """将任意输入转为 float；None/NaN/非数值全部视作 0.0。"""
    if raw is None:
        return 0.0
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if np.isnan(val):
        return 0.0
    return val


def encode_action_value(raw, field: str) -> float:
    """
    将原始行为字段编码为最终 float 值，严格区分字段类型；NaN 均视为 0。

    - dwell_time: 连续秒数 → 归一化到 [0, 1]
    - vqv: 连续 [0, 1]
    - 其他 17 个字段: 二值 0/1（> 0 即视为触发）
    """
    val = _to_float_or_zero(raw)
    if field == "dwell_time":
        return normalize_dwell(val) if val > 0 else 0.0
    if field == "vqv":
        return val if val > 0 else 0.0
    return 1.0 if val > 0 else 0.0


def encode_actions_vectorized(df: pd.DataFrame, dwell_max_seconds: float = 300.0) -> np.ndarray:
    """
    向量化编码 19 个行为字段，返回 shape=(N, 19) 的 float32 矩阵。

    逐列：
      - dwell_time: clip(x/300, 0, 1)，NaN/负值 → 0
      - vqv: clip(x, 0, 1)，NaN/负值 → 0
      - 其他 17 个二值字段: (x > 0) → 1.0 else 0.0

    避免循环内逐行 encode_action_value 的 55 亿次 Python 分支调用。
    """
    n = len(df)
    out = np.zeros((n, len(BEHAVIOR_FIELDS)), dtype=np.float32)
    for j, field in enumerate(BEHAVIOR_FIELDS):
        if field not in df.columns:
            continue
        col = pd.to_numeric(df[field], errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
        if field == "dwell_time":
            out[:, j] = np.clip(col / dwell_max_seconds, 0.0, 1.0)
        elif field == "vqv":
            out[:, j] = np.where(col > 0, col, 0.0)
        else:
            out[:, j] = (col > 0).astype(np.float32)
    return out


# ── 数据加载 ───────────────────────────────────────────────────────────────────

def load_behavior_logs(behavior_dir: str, date_str: str | None = None) -> pd.DataFrame:
    """
    加载行为日志。
    
    Args:
        behavior_dir: 行为日志根目录（包含 dt=YYYY-MM-DD 子目录）
        date_str: 指定日期（如 "2024-01-01"），为None则加载所有日期
    
    Returns:
        DataFrame，包含所有行为事件，按 user_id 和 event_time 排序
    """
    behavior_path = Path(behavior_dir)
    
    if date_str:
        pattern = f"dt={date_str}"
        parquet_files = list(behavior_path.rglob(f"{pattern}/**/*.parquet"))
    else:
        parquet_files = list(behavior_path.rglob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"在 {behavior_dir} 下未找到任何 Parquet 文件")
    
    logger.info(f"找到 {len(parquet_files)} 个行为日志文件")
    
    dfs = []
    for f in parquet_files:
        try:
            df = pq.read_table(str(f)).to_pandas()
            dfs.append(df)
            logger.debug(f"加载 {f}: {len(df)} 行")
        except (OSError, pa.ArrowException) as e:
            logger.warning(f"跳过 {f}: {e}")

    df = pd.concat(dfs, ignore_index=True)

    # event_time 清洗：非数值 → NaN → 丢弃（否则后续时间比较会静默跳过样本）
    total_before = len(df)
    df["event_time"] = pd.to_numeric(df["event_time"], errors="coerce")
    df = df.dropna(subset=["event_time"]).reset_index(drop=True)
    dropped = total_before - len(df)
    if dropped > 0:
        logger.warning(f"丢弃 {dropped} 行 event_time 非数值的脏数据")
    df["event_time"] = df["event_time"].astype(np.int64)

    df["user_id"] = df["user_id"].astype(str)
    df["post_id"] = df["post_id"].astype(str)
    df["author_id"] = df["author_id"].astype(str)

    # product_surface 缺失/NaN → 0，避免循环内逐行判 NaN
    if "product_surface" not in df.columns:
        df["product_surface"] = 0
    df["product_surface"] = pd.to_numeric(df["product_surface"], errors="coerce").fillna(0).astype(np.int32)

    # 按 user_id + event_time 排序（构造历史序列需要）；同秒事件靠原始行号稳定排序
    df = df.sort_values(["user_id", "event_time"], kind="stable").reset_index(drop=True)
    
    logger.info(f"行为日志加载完成: 共 {len(df)} 行，{df['user_id'].nunique()} 个用户")
    return df


def load_impressions(impressions_dir: str, date_str: str | None = None) -> pd.DataFrame:
    """
    加载曝光表（scripts/build_training_inputs.py 的 impressions/ 输出）。

    每行 = 一次请求里的一条下发候选，必需列：
        user_id, request_id, event_time(秒), position, post_id, author_id, product_surface,
        以及 BEHAVIOR_FIELDS 中的 19 个标签列（缺列按 0 补）。
    返回按 user_id, event_time, request_id, position 稳定排序的 DataFrame。
    """
    impressions_path = Path(impressions_dir)
    if date_str:
        parquet_files = list(impressions_path.rglob(f"dt={date_str}/**/*.parquet"))
    else:
        parquet_files = list(impressions_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"在 {impressions_dir} 下未找到任何 Parquet 文件")
    logger.info(f"找到 {len(parquet_files)} 个曝光文件")

    dfs = []
    for f in parquet_files:
        try:
            dfs.append(pq.read_table(str(f)).to_pandas())
        except (OSError, pa.ArrowException) as e:
            logger.warning(f"跳过 {f}: {e}")
    df = pd.concat(dfs, ignore_index=True)

    required = {"user_id", "request_id", "event_time", "position", "post_id", "author_id"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"曝光表缺少必需列：{missing}")

    df["event_time"] = pd.to_numeric(df["event_time"], errors="coerce")
    df = df.dropna(subset=["event_time"]).reset_index(drop=True)
    df["event_time"] = df["event_time"].astype(np.int64)
    for column in ("user_id", "request_id", "post_id", "author_id"):
        df[column] = df[column].astype(str)
    df["position"] = pd.to_numeric(df["position"], errors="coerce").fillna(0).astype(np.int32)
    if "product_surface" not in df.columns:
        df["product_surface"] = 0
    df["product_surface"] = (
        pd.to_numeric(df["product_surface"], errors="coerce").fillna(0).astype(np.int32)
    )
    for field in BEHAVIOR_FIELDS:
        if field not in df.columns:
            df[field] = 0.0

    df = df.sort_values(
        ["user_id", "event_time", "request_id", "position"], kind="stable"
    ).reset_index(drop=True)
    logger.info(
        f"曝光表加载完成: 共 {len(df)} 条下发候选，{df['request_id'].nunique()} 次请求，"
        f"{df['user_id'].nunique()} 个用户"
    )
    return df


def is_action_event_table(df: pd.DataFrame) -> bool:
    """行为表是否是"一行一个行为事件"的事件表（`scripts/build_training_inputs.py` 的产出）。

    事件表带 `action_type` 列（proto ActionName 枚举值）；`examples/generate_example_data.py`
    那种"一行一次互动、多热标签"的日志表没有这一列。
    """
    return "action_type" in df.columns


def aggregate_action_events(df: pd.DataFrame) -> pd.DataFrame:
    """把事件表按 (user_id, post_id) 全时段聚合成多热行（行为模式的正样本粒度）。

    行为模式把每一行当一条正样本、标签取这一行的行为向量。事件表一行只有一位，直接喂
    会把"点赞过也评论过"拆成两条互为负样本的行，所以先合并：标签取或（连续列取最大），
    `event_time` / `author_id` / `product_surface` 取最早一次行为。曝光模式不用这个函数——
    它在 `TrainingSampleBuilder.build_impression_samples` 里以请求时间为截止逐请求聚合。
    """
    if df.empty:
        return df.drop(columns=[c for c in ("action_type", "action_time_ms") if c in df.columns])
    ordered = df.sort_values(["user_id", "post_id", "event_time"], kind="stable")
    first = ordered.groupby(["user_id", "post_id"], sort=False).first()
    present = [field for field in BEHAVIOR_FIELDS if field in ordered.columns]
    maxed = ordered.groupby(["user_id", "post_id"], sort=False)[present].max()
    merged = first.drop(columns=present + ["action_type", "action_time_ms"], errors="ignore")
    merged = merged.join(maxed).reset_index()
    for field in BEHAVIOR_FIELDS:
        if field not in merged.columns:
            merged[field] = 0.0
    columns = ["user_id", "post_id", "author_id", "event_time", "product_surface"] + BEHAVIOR_FIELDS
    extra = [c for c in merged.columns if c not in columns]
    merged = merged[columns + extra]
    return merged.sort_values(["user_id", "event_time"], kind="stable").reset_index(drop=True)


def load_post_metadata(post_meta_path: str) -> pd.DataFrame:
    """加载帖子元数据。"""
    df = pq.read_table(post_meta_path).to_pandas()
    
    # 确保字段类型
    df["post_id"] = df["post_id"].astype(str)
    df["author_id"] = df["author_id"].astype(str)
    
    logger.info(f"帖子元数据加载完成: 共 {len(df)} 个帖子")
    return df


def build_post_to_author_map(post_meta_df: pd.DataFrame) -> dict[str, str]:
    """构建 post_id -> author_id 映射。"""
    return dict(zip(post_meta_df["post_id"], post_meta_df["author_id"]))


def build_active_post_set(post_meta_df: pd.DataFrame) -> set[str]:
    """获取在线帖子集合（is_active=1）。"""
    active_df = post_meta_df[post_meta_df.get("is_active", 1) == 1]
    return set(active_df["post_id"])


def build_negative_pool(post_meta_df: pd.DataFrame) -> tuple[list[int], list[str]]:
    """构建按 create_time 升序排列的负样本池 (create_times, post_ids)。

    负采样时按事件时刻用二分切出 [event_time - MAX_AGE, event_time] 窗口，保证负例都是
    “该时刻线上真的可能被推荐”的帖子：未来才发布的帖子在训练期没有任何互动，采进来会让
    模型和热度基线学到“没见过的 ID = 负例”这种线上不存在的规律。
    缺少 create_time 列时退化为全池（create_time 视为 0，且不做时间过滤）。
    """
    active_df = post_meta_df[post_meta_df.get("is_active", 1) == 1]
    if "create_time" in active_df.columns:
        times = pd.to_numeric(active_df["create_time"], errors="coerce").fillna(0).astype(np.int64)
    else:
        logger.warning("post_metadata 缺少 create_time，负样本池不做时间过滤")
        times = pd.Series(np.zeros(len(active_df), dtype=np.int64), index=active_df.index)
    order = np.argsort(times.to_numpy(), kind="stable")
    posts = active_df["post_id"].to_numpy()[order]
    return times.to_numpy()[order].tolist(), posts.tolist()


# ── 训练样本构造 ───────────────────────────────────────────────────────────────

class TrainingSampleBuilder:
    """
    从用户行为序列构造训练样本。
    
    每个训练样本包含：
    - 用户哈希
    - 历史序列（最近32条交互记录）
    - 候选集（1正样本 + 7负样本 = 8个候选）
    - 标签（候选集上每个行为的ground truth）
    """
    
    def __init__(
        self,
        post_to_author: dict[str, str],
        active_posts: set[str] | None,
        history_len: int = HISTORY_SEQ_LEN,
        candidate_len: int = CANDIDATE_SEQ_LEN,
        num_actions: int = NUM_ACTIONS,
        neg_sample_ratio: int = 7,  # 负样本数
        negative_pool: tuple[list[int], list[str]] | None = None,
        max_age_seconds: int = MAX_AGE_DAYS * 86400,
    ):
        self.post_to_author = post_to_author
        # active_posts 冻结为 frozenset 以支持高效成员判断；None 表示没有元数据、不过滤
        # （只在曝光模式允许：候选来自真实下发，不需要负样本池）。
        self.active_posts: frozenset[str] | None = (
            None if active_posts is None else frozenset(active_posts)
        )
        # 负样本池按 create_time 升序；未提供 create_time 时全池时间为 0（不做过滤）
        if negative_pool is None:
            posts = sorted(self.active_posts or ())
            negative_pool = ([0] * len(posts), posts)
        self._pool_times, self._pool_posts = negative_pool
        self._time_filter = any(t > 0 for t in self._pool_times)
        self.max_age_seconds = max_age_seconds
        self.history_len = history_len
        self.candidate_len = candidate_len
        self.num_actions = num_actions
        self.neg_sample_ratio = neg_sample_ratio

    def _get_author_id(self, post_id: str) -> str:
        """获取帖子的作者 ID。post_id 超出元数据时回退到占位符。"""
        return self.post_to_author.get(post_id, "unknown_author")

    def _row_author_id(self, row: pd.Series) -> str:
        """从行为日志行解析 author_id，容错缺失/NaN。"""
        val = row.get("author_id")
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return self._get_author_id(row["post_id"])
        return str(val)

    def _row_surface(self, row: pd.Series) -> int:
        """解析 product_surface，容错缺失/NaN。"""
        val = row.get("product_surface", 0)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return 0
        return int(val)

    def _pool_window(self, event_time: int) -> tuple[int, int]:
        """返回事件时刻可作为负例的池下标区间 [lo, hi)：create_time ∈ [t - max_age, t]。"""
        if not self._time_filter:
            return 0, len(self._pool_posts)
        lo = bisect.bisect_left(self._pool_times, event_time - self.max_age_seconds)
        hi = bisect.bisect_right(self._pool_times, event_time)
        return lo, hi

    def _sample_negative_posts(
        self,
        user_interacted_posts: frozenset,
        positive_post: str,
        n: int,
        rng: random.Random,
        event_time: int,
    ) -> list[str]:
        """
        从用户未交互过、且在事件时刻已发布且未过期的在线帖子中均匀采样负样本。

        实现：按时间窗口二分出池区间，直接采 n 个，冲突时补采。用户交互帖子远少于池，
        碰撞率极低，均匀分布与原实现一致。

        不足 n 个时（用户已交互帖子占据池近饱和）返回能采到的全部；
        空池返回空列表，调用方应据此决定是否放弃样本。
        """
        lo, hi = self._pool_window(event_time)
        pool = self._pool_posts
        pool_size = hi - lo
        if pool_size <= 0:
            return []

        exclude = user_interacted_posts | {positive_post}
        # 池几乎被 exclude 占光时撤退到全池差集（稀见路径）
        if len(exclude) >= pool_size - n:
            available = [p for p in pool[lo:hi] if p not in exclude]
            if not available:
                return []
            if len(available) <= n:
                return available
            return rng.sample(available, n)

        # 主路径：直接采 n 个 + 冲突补采，不受 exclude 规模影响
        picked: list[str] = []
        seen: set = set()
        # 最多尝试 5 轮，下限是 O(n)，上限极端不超过 5n
        for _ in range(5):
            need = n - len(picked)
            if need <= 0:
                break
            # 过采 1.5 倍减少循环轮数（碰撞率极低，一般 1~2 轮就够）
            batch_size = min(need + max(need // 2, 1), pool_size)
            for idx in rng.sample(range(lo, hi), batch_size):
                post = pool[idx]
                if post in exclude or post in seen:
                    continue
                seen.add(post)
                picked.append(post)
                if len(picked) >= n:
                    break

        return picked
    
    def build_sample(
        self,
        user_id: str,
        user_behavior_df: pd.DataFrame,
        rng: random.Random,
        user_actions: np.ndarray,
    ) -> list[dict]:
        """
        为单个用户构造训练样本。

        核心优化：per-record 特征（post_hash / author_hash / action / surface）在本函数开头
        只计算一次；每条样本的历史窗口通过列表切片复用，避免了同一事件被多达 history_len
        次重复 hash + encode 的冗余。

        Args:
            user_id: 用户 ID
            user_behavior_df: 该用户的行为日志（已按 event_time 稳定排序）
            rng: 该用户专属的随机源
            user_actions: shape=(len(user_behavior_df), num_actions) 的向量化 action 矩阵，
                          由 process_single_day 调用 encode_actions_vectorized 预计算

        Returns:
            样本列表，每个样本是一个字典
        """
        samples: list[dict] = []

        df = user_behavior_df.reset_index(drop=True)
        records: list[dict] = df.to_dict("records")
        N = len(records)
        if N == 0:
            return samples

        user_interacted_posts: frozenset = frozenset(r["post_id"] for r in records)
        user_hash = hash_id_to_ints(user_id)

        rec_post_hash, rec_author_hash, rec_surface, rec_actions = self._prepare_records(
            records, user_actions
        )

        history_len = self.history_len
        num_actions = self.num_actions
        pad_hash = [0] * NUM_HASHES
        pad_action = [0.0] * num_actions

        for pos in range(N):
            rec = records[pos]
            positive_post = rec["post_id"]

            if self.active_posts is not None and positive_post not in self.active_posts:
                continue
            if pos == 0:
                continue  # 冷启动：无历史

            negative_posts = self._sample_negative_posts(
                user_interacted_posts,
                positive_post,
                self.neg_sample_ratio,
                rng,
                int(rec["event_time"]),
            )
            if not negative_posts:
                continue

            # ── 历史窗口：直接切片 + 尾部 padding ──
            hist_start = pos - history_len if pos > history_len else 0
            actual_len = pos - hist_start
            pad_len = history_len - actual_len

            if pad_len > 0:
                # 注意：这里 [pad_hash] * pad_len 会复制引用，内容只读，pyarrow 写出独立序列化，安全
                hist_post_hashes = rec_post_hash[hist_start:pos] + [pad_hash] * pad_len
                hist_author_hashes = rec_author_hash[hist_start:pos] + [pad_hash] * pad_len
                hist_actions = rec_actions[hist_start:pos] + [pad_action] * pad_len
                hist_surface = rec_surface[hist_start:pos] + [0] * pad_len
            else:
                hist_post_hashes = rec_post_hash[hist_start:pos]
                hist_author_hashes = rec_author_hash[hist_start:pos]
                hist_actions = rec_actions[hist_start:pos]
                hist_surface = rec_surface[hist_start:pos]

            # ── 候选集：正样本查表 + 负样本哈希 + 尾部 padding ──
            surface_id = rec_surface[pos]
            cand_post_hashes: list[list[int]] = [rec_post_hash[pos]]
            cand_author_hashes: list[list[int]] = [rec_author_hash[pos]]
            cand_surfaces: list[int] = [surface_id]
            # 正样本 label = 当前事件的 ground-truth action（查表，不重新 encode）
            labels: list[list[float]] = [rec_actions[pos]]

            for neg_post in negative_posts:
                cand_post_hashes.append(hash_id_to_ints(neg_post))
                cand_author_hashes.append(hash_id_to_ints(self._get_author_id(neg_post)))
                cand_surfaces.append(surface_id)
                labels.append(pad_action)

            cand_pad = self.candidate_len - len(cand_post_hashes)
            if cand_pad > 0:
                cand_post_hashes.extend([pad_hash] * cand_pad)
                cand_author_hashes.extend([pad_hash] * cand_pad)
                cand_surfaces.extend([surface_id] * cand_pad)
                labels.extend([pad_action] * cand_pad)

            samples.append({
                "user_id": user_id,
                "event_time": int(rec["event_time"]),
                "user_hashes": user_hash,
                "history_post_hashes": hist_post_hashes,
                "history_author_hashes": hist_author_hashes,
                "history_actions": hist_actions,
                "history_product_surface": hist_surface,
                "candidate_post_hashes": cand_post_hashes,
                "candidate_author_hashes": cand_author_hashes,
                "candidate_product_surface": cand_surfaces,
                "labels": labels,
                "positive_post": positive_post,
                "negative_posts": negative_posts,
            })

        return samples

    def _aggregate_history(
        self,
        records: list[dict],
        record_times: list[int],
        rec_actions: list[list[float]],
        start: int,
        end: int,
    ) -> list[tuple[int, list[float]]]:
        """把 records[start:end]（已按时间升序）按帖子聚合，返回最近 history_len 条。

        与线上 `home-mixer/recsys_compat::DefaultAggregator` 同构：同一帖子的多次行为合并成
        一条，mask 取或（连续列取最大），时间 / 作者 / 场景取该帖在窗口内最早的一次行为，
        按 (最早时间, post_id) 升序，全零的记录丢弃（`DenseAggregatedActionFilter`），
        最后只保留最近 history_len 条。返回 [(最早行为的记录下标, 合并后的行为向量)]。
        """
        by_post: dict[str, tuple[int, list[float]]] = {}
        for idx in range(start, end):
            post_id = records[idx]["post_id"]
            entry = by_post.get(post_id)
            if entry is None:
                by_post[post_id] = (idx, list(rec_actions[idx]))
                continue
            merged = entry[1]
            row = rec_actions[idx]
            for j, value in enumerate(row):
                if value > merged[j]:
                    merged[j] = value
        entries = [
            (idx, mask)
            for idx, mask in by_post.values()
            if any(value > 0 for value in mask)
        ]
        entries.sort(key=lambda entry: (record_times[entry[0]], records[entry[0]]["post_id"]))
        return entries[-self.history_len :]

    @staticmethod
    def _prepare_records(
        records: list[dict], user_actions: np.ndarray
    ) -> tuple[list[list[int]], list[list[int]], list[int], list[list[float]]]:
        """per-record 预计算（同一事件作为历史最多出现 history_len 次，只算一次）。"""
        rec_post_hash = [hash_id_to_ints(r["post_id"]) for r in records]
        rec_author_hash = [hash_id_to_ints(r["author_id"]) for r in records]
        rec_surface = [int(r["product_surface"]) % SURFACE_VOCAB for r in records]
        # numpy → Python list-of-list（pyarrow 接受原生 Python list）
        rec_actions = user_actions.tolist()
        return rec_post_hash, rec_author_hash, rec_surface, rec_actions

    def build_impression_samples(
        self,
        user_id: str,
        user_behavior_df: pd.DataFrame,
        user_actions: np.ndarray,
        user_impressions_df: pd.DataFrame,
        impression_labels: np.ndarray,
        history_window_seconds: int = HISTORY_WINDOW_DAYS * 86400,
        include_negative_only: bool = False,
    ) -> list[dict]:
        """
        以"一次下发请求"为单位构造样本（曝光模式）。

        候选 = 这次请求真正下发的帖子（正样本排在前面、然后是按位置排列的负样本，截到
        candidate_len），标签 = 归因到该候选上的行为；历史 = 请求时刻之前、窗口内的行为记录，
        按帖子聚合后取最近 history_len 条，与线上 `DefaultAggregator` 一致（见
        `_aggregate_history`）。候选位 0 保证是正样本，以便召回训练复用同一份样本；没有任何
        正样本的请求默认丢弃，`include_negative_only=True` 时保留并把 `positive_post` 置为空串
        （召回训练据此跳过）。

        Args:
            user_behavior_df: 该用户的行为记录（已按 event_time 稳定排序），只用于历史。
                一行一个行为事件（`build_training_inputs.py` 的 behavior_logs）或一行一次多热
                互动都可以；聚合在这里按请求时间做，所以请求之后的行为不会进历史。
            user_actions: 与 user_behavior_df 行对齐的 (N, num_actions) 行为矩阵。
            user_impressions_df: 该用户的曝光行（已按 event_time, request_id, position 排序）。
            impression_labels: 与 user_impressions_df 行对齐的 (M, num_actions) 标签矩阵。
        """
        samples: list[dict] = []
        if len(user_impressions_df) == 0:
            return samples

        df = user_behavior_df.reset_index(drop=True)
        records: list[dict] = df.to_dict("records")
        if not records:
            return samples  # 冷启动：线上没有序列就不会调用模型，训练同样不构造样本
        rec_post_hash, rec_author_hash, rec_surface, rec_actions = self._prepare_records(
            records, user_actions
        )
        record_times = [int(r["event_time"]) for r in records]
        user_hash = hash_id_to_ints(user_id)

        history_len = self.history_len
        num_actions = self.num_actions
        pad_hash = [0] * NUM_HASHES
        pad_action = [0.0] * num_actions

        impressions = user_impressions_df.reset_index(drop=True)
        label_rows = impression_labels.tolist()
        for request_id, group in impressions.groupby("request_id", sort=False):
            group = group.sort_values("position", kind="stable")
            request_time = int(group["event_time"].iloc[0])

            # ── 历史：请求时刻之前、窗口内的行为，按帖子聚合后取最近 history_len 条 ──
            end = bisect.bisect_left(record_times, request_time)  # 严格早于请求
            start = bisect.bisect_left(record_times, request_time - history_window_seconds)
            if start >= end:
                continue  # 窗口内无历史，等价于线上无序列
            history_entries = self._aggregate_history(
                records, record_times, rec_actions, start, end
            )
            if not history_entries:
                continue
            pad_len = history_len - len(history_entries)
            hist_post_hashes = [rec_post_hash[i] for i, _ in history_entries] + [pad_hash] * pad_len
            hist_author_hashes = [rec_author_hash[i] for i, _ in history_entries] + [pad_hash] * pad_len
            hist_actions = [mask for _, mask in history_entries] + [pad_action] * pad_len
            hist_surface = [rec_surface[i] for i, _ in history_entries] + [0] * pad_len

            # ── 候选：正样本优先，其余按下发位置 ──
            positives: list[tuple[str, str, int, list[float]]] = []
            negatives: list[tuple[str, str, int, list[float]]] = []
            for row_index, row in zip(group.index, group.itertuples(index=False)):
                if self.active_posts is not None and row.post_id not in self.active_posts:
                    continue
                labels_row = label_rows[row_index]
                surface = int(row.product_surface) % SURFACE_VOCAB
                entry = (row.post_id, row.author_id, surface, labels_row)
                (positives if any(v > 0 for v in labels_row) else negatives).append(entry)
            if not positives and not include_negative_only:
                continue
            chosen = (positives + negatives)[: self.candidate_len]
            if not chosen:
                continue

            cand_post_hashes = [hash_id_to_ints(post) for post, _, _, _ in chosen]
            cand_author_hashes = [hash_id_to_ints(author) for _, author, _, _ in chosen]
            cand_surfaces = [surface for _, _, surface, _ in chosen]
            labels = [list(row_labels) for _, _, _, row_labels in chosen]
            cand_pad = self.candidate_len - len(chosen)
            if cand_pad > 0:
                pad_surface = cand_surfaces[0]
                cand_post_hashes.extend([pad_hash] * cand_pad)
                cand_author_hashes.extend([pad_hash] * cand_pad)
                cand_surfaces.extend([pad_surface] * cand_pad)
                labels.extend([pad_action] * cand_pad)

            positive_ids = {post for post, _, _, _ in positives}
            samples.append({
                "user_id": user_id,
                "event_time": request_time,
                "user_hashes": user_hash,
                "history_post_hashes": hist_post_hashes,
                "history_author_hashes": hist_author_hashes,
                "history_actions": hist_actions,
                "history_product_surface": hist_surface,
                "candidate_post_hashes": cand_post_hashes,
                "candidate_author_hashes": cand_author_hashes,
                "candidate_product_surface": cand_surfaces,
                "labels": labels,
                "positive_post": positives[0][0] if positives else "",
                "negative_posts": [post for post, _, _, _ in chosen if post not in positive_ids],
            })

        return samples


# ── 主处理流程 ──────────────────────────────────────────────────────────────────

# 输出 parquet 的显式 schema（与文档 / train_ranker.load_parquet_batch 的期望一致）
_TRAIN_SAMPLE_SCHEMA = pa.schema([
    ("user_id", pa.string()),
    ("event_time", pa.int64()),
    ("user_hashes", pa.list_(pa.int32())),
    ("history_post_hashes", pa.list_(pa.list_(pa.int32()))),
    ("history_author_hashes", pa.list_(pa.list_(pa.int32()))),
    ("history_actions", pa.list_(pa.list_(pa.float32()))),
    ("history_product_surface", pa.list_(pa.int32())),
    ("candidate_post_hashes", pa.list_(pa.list_(pa.int32()))),
    ("candidate_author_hashes", pa.list_(pa.list_(pa.int32()))),
    ("candidate_product_surface", pa.list_(pa.int32())),
    ("labels", pa.list_(pa.list_(pa.float32()))),
    ("positive_post", pa.string()),
    ("negative_posts", pa.list_(pa.string())),
])


def _samples_to_arrow_table(samples: list[dict]) -> pa.Table:
    """按 `_TRAIN_SAMPLE_SCHEMA` 显式构造 pyarrow Table，避免类型推断导致的 int64/float64。"""
    columns = {name: [s[name] for s in samples] for name in _TRAIN_SAMPLE_SCHEMA.names}
    arrays = [
        pa.array(columns[field.name], type=field.type) for field in _TRAIN_SAMPLE_SCHEMA
    ]
    return pa.Table.from_arrays(arrays, schema=_TRAIN_SAMPLE_SCHEMA)


def process_single_day(
    behavior_df: pd.DataFrame,
    post_meta_df: pd.DataFrame | None,
    output_path: str,
    neg_sample_ratio: int = 7,
    seed: int = 42,
    max_age_days: int = MAX_AGE_DAYS,
    impressions_df: pd.DataFrame | None = None,
    include_negative_only: bool = False,
    history_window_days: int = HISTORY_WINDOW_DAYS,
):
    """处理单日的行为日志（可选加曝光表），生成训练样本。"""

    if impressions_df is None and post_meta_df is None:
        raise ValueError("没有曝光表时必须提供帖子元数据（负样本池来自元数据）")

    # 构建辅助数据结构
    if post_meta_df is not None:
        post_to_author = build_post_to_author_map(post_meta_df)
        active_posts: set[str] | None = build_active_post_set(post_meta_df)
        negative_pool = build_negative_pool(post_meta_df)
        logger.info(f"在线帖子数: {len(active_posts)}，负样本窗口: {max_age_days} 天")
    else:
        post_to_author, active_posts, negative_pool = {}, None, ([], [])
        logger.info("未提供帖子元数据：曝光模式下不做在线帖子过滤")

    # 初始化样本构造器
    builder = TrainingSampleBuilder(
        post_to_author=post_to_author,
        active_posts=active_posts,
        neg_sample_ratio=neg_sample_ratio,
        negative_pool=negative_pool,
        max_age_seconds=max_age_days * 86400,
    )

    if impressions_df is None and is_action_event_table(behavior_df):
        # 行为模式以"一行一条正样本"构样本；事件表一行只有一位，先按 (user, post) 合并。
        before = len(behavior_df)
        behavior_df = aggregate_action_events(behavior_df)
        logger.info(f"事件表按 (user, post) 聚合：{before} 行 → {len(behavior_df)} 行")

    # 全局向量化预编码 19 个行为字段（替代循环内逐行 encode_action_value 的 55 亿次调用）
    logger.info("向量化编码 action 特征...")
    behavior_df = behavior_df.reset_index(drop=True)
    global_actions = encode_actions_vectorized(behavior_df)
    logger.info(f"action 矩阵形状: {global_actions.shape}")

    all_samples = []
    if impressions_df is None:
        # 行为模式：每条正向互动一条样本，负样本随机采
        user_groups = behavior_df.groupby("user_id", sort=False)
        logger.info(f"开始处理 {len(user_groups)} 个用户...")
        for user_idx, (user_key, user_df) in enumerate(user_groups):
            if user_idx % 1000 == 0:
                logger.info(f"已处理 {user_idx} 个用户...")

            user_id = str(user_key)
            # 确定性 per-user 种子（md5，不受 PYTHONHASHSEED 影响）
            rng = random.Random(deterministic_user_seed(seed, user_id))

            # 用原 index 从全局 actions 矩阵切出该用户的行（groupby 保留原 index）
            user_actions = global_actions[user_df.index.to_numpy()]
            samples = builder.build_sample(user_id, user_df, rng, user_actions)
            if samples:
                all_samples.extend(samples)
    else:
        # 曝光模式：每次下发请求一条样本，负样本是看到但没互动的候选
        impressions_df = impressions_df.reset_index(drop=True)
        global_labels = encode_actions_vectorized(impressions_df)
        behavior_groups = {
            str(user_key): user_df
            for user_key, user_df in behavior_df.groupby("user_id", sort=False)
        }
        impression_groups = impressions_df.groupby("user_id", sort=False)
        logger.info(
            f"开始处理 {len(impression_groups)} 个有曝光的用户"
            f"（历史窗口 {history_window_days} 天，"
            f"{'保留' if include_negative_only else '丢弃'}无正样本的请求）..."
        )
        for user_idx, (user_key, user_impressions) in enumerate(impression_groups):
            if user_idx % 1000 == 0:
                logger.info(f"已处理 {user_idx} 个用户...")
            user_id = str(user_key)
            user_df = behavior_groups.get(user_id)
            if user_df is None:
                continue  # 没有任何行为记录 = 没有历史，线上不会调用模型
            samples = builder.build_impression_samples(
                user_id,
                user_df,
                global_actions[user_df.index.to_numpy()],
                user_impressions,
                global_labels[user_impressions.index.to_numpy()],
                history_window_seconds=history_window_days * 86400,
                include_negative_only=include_negative_only,
            )
            if samples:
                all_samples.extend(samples)

    logger.info(f"总共生成 {len(all_samples)} 个训练样本")

    if not all_samples:
        logger.warning("没有生成任何样本，请检查输入数据")
        return

    # 显式 pyarrow schema 写出，保证 dtype 与文档一致
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)
    table = _samples_to_arrow_table(all_samples)
    pq.write_table(table, str(output_path_obj))

    logger.info(f"训练样本已保存: {output_path}")
    logger.info(f"样本示例: user_hashes={all_samples[0]['user_hashes']}")
    logger.info(f"历史序列长度: {len(all_samples[0]['history_post_hashes'])}")
    logger.info(f"候选集大小: {len(all_samples[0]['candidate_post_hashes'])}")
    logger.info(f"标签维度: {len(all_samples[0]['labels'][0])}")
    logger.info(f"Parquet schema:\n{table.schema}")


def main():
    parser = argparse.ArgumentParser(description="从原始日志生成训练样本")
    parser.add_argument(
        "--behavior-dir",
        type=str,
        required=True,
        help="行为日志目录（包含 dt=YYYY-MM-DD 子目录）",
    )
    parser.add_argument(
        "--post-meta",
        type=str,
        default=None,
        help="帖子元数据 Parquet 文件路径（无曝光表时必填；曝光模式下可选，用于过滤已删除帖子）",
    )
    parser.add_argument(
        "--impressions-dir",
        type=str,
        default=None,
        help=(
            "曝光表目录（scripts/build_training_inputs.py 的 impressions/ 输出，含 dt= 子目录）。"
            "提供时按下发请求构造样本，负样本来自同一请求里没有互动的候选"
        ),
    )
    parser.add_argument(
        "--include-negative-only-requests",
        action="store_true",
        help="曝光模式下保留没有任何正样本的请求（positive_post 为空，召回训练会跳过）",
    )
    parser.add_argument(
        "--history-window-days",
        type=int,
        default=HISTORY_WINDOW_DAYS,
        help="曝光模式下历史序列只取请求前 N 天的行为（默认 7，与线上 UAS 窗口一致）",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/training_samples",
        help="输出目录",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定处理某天数据（如 2024-01-01），不指定则处理全部",
    )
    parser.add_argument(
        "--neg-ratio",
        type=int,
        default=7,
        help="负样本比例（默认 7，即 1:7）",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="随机种子",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_AGE_DAYS,
        help="负样本只从事件时刻前 N 天内发布的帖子中采样（默认 7，与线上过滤一致）",
    )
    
    args = parser.parse_args()
    if args.impressions_dir is None and args.post_meta is None:
        parser.error("--post-meta 在没有 --impressions-dir 时必填（负样本池来自帖子元数据）")
    if args.history_window_days <= 0:
        parser.error("--history-window-days 必须为正整数")

    logger.info("=== 数据预处理开始 ===")

    # 加载数据
    post_meta_df = None
    if args.post_meta is not None:
        logger.info("加载帖子元数据...")
        post_meta_df = load_post_metadata(args.post_meta)

    logger.info("加载行为日志...")
    # 曝光模式下历史需要请求日之前的行为，所以不按 --date 裁剪行为日志
    behavior_df = load_behavior_logs(
        args.behavior_dir, None if args.impressions_dir else args.date
    )

    impressions_df = None
    if args.impressions_dir is not None:
        logger.info("加载曝光表...")
        impressions_df = load_impressions(args.impressions_dir, args.date)

    # 确定输出文件名
    if args.date:
        output_file = f"train_{args.date.replace('-', '')}.parquet"
    else:
        output_file = "train_all.parquet"
    output_path = Path(args.output_dir) / output_file

    # 处理
    process_single_day(
        behavior_df=behavior_df,
        post_meta_df=post_meta_df,
        output_path=str(output_path),
        neg_sample_ratio=args.neg_ratio,
        seed=args.seed,
        max_age_days=args.max_age_days,
        impressions_df=impressions_df,
        include_negative_only=args.include_negative_only_requests,
        history_window_days=args.history_window_days,
    )
    
    logger.info("=== 数据预处理完成 ===")


if __name__ == "__main__":
    main()
