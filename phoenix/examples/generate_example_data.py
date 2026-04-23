# 版权所有 2026 X.AI Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
生成示例数据用于测试预处理流程

输出的字段与类型严格按照 `docs/真实数据接入指引.md` 与 `data_preprocessor.py` 的约定：

behavior_logs/dt=YYYY-MM-DD/*.parquet 字段：
    - event_time        int64   事件时间戳（秒）
    - user_id           string
    - post_id           string
    - author_id         string
    - product_surface   int8    场景 ID，值域 [0, 16)
    - 17 个二值行为     int8    0/1（见 BINARY_BEHAVIORS）
    - vqv               float32 视频播放质量分，值域 [0, 1]
    - dwell_time        int32   停留秒数（归一化前）

post_metadata.parquet 字段：
    - post_id           string
    - author_id         string
    - create_time       int64
    - is_active         int8    0/1

user_metadata.parquet 字段：
    - user_id           string
    - register_time     int64
    - is_active         int8

使用示例：
    uv run generate_example_data.py
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ── 行为字段定义（与 data_preprocessor.BEHAVIOR_FIELDS 一致）──────────────────

# 二值行为（int 0/1），每个字段对应一次触发概率
BINARY_BEHAVIORS: dict[str, float] = {
    "favorite": 0.15,
    "reply": 0.05,
    "repost": 0.08,
    "photo_expand": 0.10,
    "click": 0.30,
    "profile_click": 0.08,
    "share": 0.05,
    "share_via_dm": 0.03,
    "share_via_copy_link": 0.02,
    "dwell": 0.40,
    "quote": 0.03,
    "quoted_click": 0.02,
    "follow_author": 0.05,
    "not_interested": 0.02,
    "block_author": 0.01,
    "mute_author": 0.01,
    "report": 0.005,
}

# 连续行为触发概率：触发时使用特定分布，否则为 0
VQV_TRIGGER_PROB = 0.20       # vqv 有值的概率
DWELL_TIME_TRIGGER_PROB = 0.50  # dwell_time > 0 的概率

SURFACE_VOCAB_SIZE = 16  # 与 data_preprocessor.SURFACE_VOCAB 一致


# ── 帖子/用户元数据 ──────────────────────────────────────────────────────────

def generate_post_metadata(num_posts: int = 1000, seed: int = 42) -> pd.DataFrame:
    """生成帖子元数据。"""
    rng = np.random.default_rng(seed)

    base_ts = int(datetime(2024, 1, 1).timestamp())
    post_ids = [f"p_20240101_{i:06d}" for i in range(num_posts)]
    author_ids = [f"u_author_{int(x)}" for x in rng.integers(1, 100, size=num_posts)]
    create_times = base_ts + rng.integers(0, 86400, size=num_posts)
    is_active = (rng.random(size=num_posts) > 0.05).astype(np.int8)  # 95% 在线

    return pd.DataFrame({
        "post_id": pd.array(post_ids, dtype="string"),
        "author_id": pd.array(author_ids, dtype="string"),
        "create_time": create_times.astype(np.int64),
        "is_active": is_active,
    })


def generate_user_metadata(num_users: int = 100, seed: int = 42) -> pd.DataFrame:
    """生成用户元数据（可选）。"""
    rng = np.random.default_rng(seed)

    base_ts = int(datetime(2023, 6, 1).timestamp())
    user_ids = [f"u_{100000 + i}" for i in range(num_users)]
    register_times = base_ts + rng.integers(0, 86400 * 180, size=num_users)
    is_active = np.ones(num_users, dtype=np.int8)

    return pd.DataFrame({
        "user_id": pd.array(user_ids, dtype="string"),
        "register_time": register_times.astype(np.int64),
        "is_active": is_active,
    })


# ── 行为日志 ─────────────────────────────────────────────────────────────────

def generate_behavior_logs(
    post_df: pd.DataFrame,
    num_users: int = 100,
    events_per_user: int = 50,
    date_str: str = "2024-01-01",
    seed: int = 42,
) -> pd.DataFrame:
    """
    生成行为日志。每个用户在一天内按时间递增产生 `events_per_user` 条交互。

    返回的 DataFrame 严格按照文档中的 dtype：二值行为为 int8，vqv 为 float32，
    dwell_time 为 int32，event_time 为 int64，product_surface 为 int8。
    """
    rng = np.random.default_rng(seed)

    active_posts = post_df.loc[post_df["is_active"] == 1, "post_id"].to_numpy()
    post_to_author = dict(zip(post_df["post_id"].tolist(), post_df["author_id"].tolist()))

    base_ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
    total = num_users * events_per_user

    # ── 基础列（向量化生成）─────────────────────────────────────────────
    user_idx = np.repeat(np.arange(num_users), events_per_user)
    user_ids = np.array([f"u_{100000 + u}" for u in user_idx])

    # 每个用户内部按序累计 60~600 秒的间隔
    step_seconds = rng.integers(60, 600, size=total, dtype=np.int64)
    # 用户内部事件序号 0..events_per_user-1
    event_idx_in_user = np.tile(np.arange(events_per_user, dtype=np.int64), num_users)
    event_time = (base_ts + event_idx_in_user * step_seconds).astype(np.int64)

    chosen_post_idx = rng.integers(0, len(active_posts), size=total)
    post_ids = active_posts[chosen_post_idx]
    author_ids = np.array([post_to_author[pid] for pid in post_ids])

    product_surface = rng.integers(0, 4, size=total).astype(np.int8)

    data: dict[str, np.ndarray] = {
        "event_time": event_time,
        "user_id": user_ids,
        "post_id": post_ids,
        "author_id": author_ids,
        "product_surface": product_surface,
    }

    # ── 二值行为：按触发概率采样 0/1（int8）──────────────────────────────
    for field, prob in BINARY_BEHAVIORS.items():
        data[field] = (rng.random(size=total) < prob).astype(np.int8)

    # ── vqv：触发时取 [0,1) 均匀分布，否则 0（float32）───────────────────
    vqv_trigger = rng.random(size=total) < VQV_TRIGGER_PROB
    vqv_values = rng.random(size=total).astype(np.float32)
    data["vqv"] = np.where(vqv_trigger, vqv_values, np.float32(0.0)).astype(np.float32)

    # ── dwell_time：触发时 5~300 秒（int32），否则 0 ──────────────────────
    dwell_trigger = rng.random(size=total) < DWELL_TIME_TRIGGER_PROB
    dwell_values = rng.integers(5, 300, size=total, dtype=np.int32)
    data["dwell_time"] = np.where(dwell_trigger, dwell_values, np.int32(0)).astype(np.int32)

    df = pd.DataFrame(data)
    # 字符串列用 pandas StringDtype，落盘为 pyarrow string
    for col in ("user_id", "post_id", "author_id"):
        df[col] = df[col].astype("string")

    return df


# ── 主流程 ───────────────────────────────────────────────────────────────────

def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # preserve_index=False 确保不把 pandas 的 RangeIndex 写进 parquet
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path))


def main():
    parser = argparse.ArgumentParser(description="生成示例数据用于测试")
    parser.add_argument("--output-dir", type=str, default="data", help="输出目录")
    parser.add_argument("--num-posts", type=int, default=1000, help="帖子数量")
    parser.add_argument("--num-users", type=int, default=100, help="用户数量")
    parser.add_argument("--events-per-user", type=int, default=50, help="每个用户的行为记录数")
    parser.add_argument("--date", type=str, default="2024-01-01", help="日期")
    parser.add_argument("--seed", type=int, default=42, help="随机种子")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    print("=== 生成示例数据 ===")

    # 1. 帖子元数据
    print(f"生成 {args.num_posts} 个帖子元数据...")
    post_df = generate_post_metadata(args.num_posts, args.seed)
    post_meta_path = output_dir / "post_metadata.parquet"
    _write_parquet(post_df, post_meta_path)
    print(f"  保存到: {post_meta_path}")

    # 2. 用户元数据
    print(f"生成 {args.num_users} 个用户元数据...")
    user_df = generate_user_metadata(args.num_users, args.seed)
    user_meta_path = output_dir / "user_metadata.parquet"
    _write_parquet(user_df, user_meta_path)
    print(f"  保存到: {user_meta_path}")

    # 3. 行为日志
    print(f"生成行为日志（{args.num_users} 用户 × {args.events_per_user} 事件）...")
    behavior_df = generate_behavior_logs(
        post_df, args.num_users, args.events_per_user, args.date, args.seed
    )
    behavior_path = output_dir / "behavior_logs" / f"dt={args.date}" / "part-00000.parquet"
    _write_parquet(behavior_df, behavior_path)
    print(f"  保存到: {behavior_path}")

    print("\n=== 数据生成完成 ===")
    print(f"总事件数: {len(behavior_df)}")
    print(f"用户数量: {behavior_df['user_id'].nunique()}")
    print(f"帖子数量: {post_df['post_id'].nunique()}")
    print(f"在线帖子: {int(post_df['is_active'].sum())}")
    print("\nbehavior_logs dtypes:")
    print(behavior_df.dtypes.to_string())
    print("\n接下来可以运行预处理:")
    print(
        f"  uv run data_preprocessor.py --behavior-dir {output_dir}/behavior_logs "
        f"--post-meta {post_meta_path} --date {args.date}"
    )


if __name__ == "__main__":
    main()
