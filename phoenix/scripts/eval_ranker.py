# 离线基线对照：在同一组候选上比较 热度 / 规则近似 / 作者亲和 / Phoenix
#
# 用法：
#   uv run scripts/eval_ranker.py \
#       --eval-dir data/training_samples_eval \
#       --behavior-dir data/real_data/behavior_logs \
#       --post-meta data/real_data/post_metadata.parquet \
#       --ckpt-dir checkpoints/recommendation-v1
#
# 评估集是 data_preprocessor 产出的 Parquet（含 positive_post / negative_posts 原始 ID），
# 基线统计只使用严格早于评估集最早事件的行为日志，避免用未来信息打分。

import argparse
import json
import logging
import math
import os
from collections import defaultdict
from pathlib import Path

import _setup_path  # noqa: F401
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import train_ranker as tr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("eval_ranker")

DAY_SECONDS = 86400.0
# 与 recommendation-service/src/ranking.rs::fallback 一致
RULE_FRESHNESS_WEIGHT = 1.5
RULE_FRESHNESS_DAYS = 7.0
RULE_NETWORK_BONUS = 2.0
RULE_ENGAGEMENT_WEIGHT = 0.2


def load_eval_rows(eval_dir: str, max_samples: int) -> dict:
    """读取评估集的原始 ID 列，拼成 [N, C] 的候选 ID 矩阵（None = padding）。"""
    files = sorted(Path(eval_dir).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"{eval_dir} 下没有 Parquet")
    columns = ["user_id", "event_time", "positive_post", "negative_posts", "candidate_post_hashes"]
    tables = []
    remaining = max_samples
    for f in files:
        t = pq.read_table(str(f), columns=columns)
        if remaining is not None:
            t = t.slice(0, remaining)
            remaining -= t.num_rows
        tables.append(t)
        if remaining is not None and remaining <= 0:
            break
    table = pa.concat_tables(tables)
    users = table["user_id"].to_pylist()
    times = np.asarray(table["event_time"].to_pylist(), dtype=np.int64)
    positives = table["positive_post"].to_pylist()
    negatives = table["negative_posts"].to_pylist()
    num_candidates = len(table["candidate_post_hashes"][0].as_py())

    candidates = []
    for pos, negs in zip(positives, negatives):
        row = [pos] + list(negs)
        row = row[:num_candidates] + [None] * (num_candidates - len(row))
        candidates.append(row)
    return {
        "user_id": users,
        "event_time": times,
        "candidates": candidates,
        "num_candidates": num_candidates,
    }


def load_training_stats(behavior_dir: str, train_end: int, users: set[str]) -> dict:
    """只用 event_time < train_end 的日志统计：帖子热度/点赞/回复、用户-作者互动次数。"""
    post_count: dict[str, int] = defaultdict(int)
    post_fav: dict[str, int] = defaultdict(int)
    post_reply: dict[str, int] = defaultdict(int)
    user_author: dict[tuple[str, str], int] = defaultdict(int)
    user_array = pa.array(sorted(users))

    files = sorted(Path(behavior_dir).rglob("*.parquet"))
    for f in files:
        t = pq.read_table(
            str(f), columns=["event_time", "user_id", "post_id", "author_id", "favorite", "reply"]
        )
        t = t.filter(
            pc.less(  # ty: ignore[unresolved-attribute]  # pyarrow.compute 动态注册，stub 未导出
                pc.cast(t["event_time"], pa.int64()), train_end
            )
        )
        if t.num_rows == 0:
            continue
        t = t.set_column(
            t.schema.get_field_index("favorite"), "favorite", pc.cast(t["favorite"], pa.int64())
        )
        t = t.set_column(
            t.schema.get_field_index("reply"), "reply", pc.cast(t["reply"], pa.int64())
        )
        per_post = t.group_by("post_id").aggregate(
            [("favorite", "sum"), ("reply", "sum"), ("post_id", "count")]
        )
        for pid, fav, rep, cnt in zip(
            per_post["post_id"].to_pylist(),
            per_post["favorite_sum"].to_pylist(),
            per_post["reply_sum"].to_pylist(),
            per_post["post_id_count"].to_pylist(),
        ):
            post_fav[pid] += fav
            post_reply[pid] += rep
            post_count[pid] += cnt

        sub = t.filter(
            pc.is_in(  # ty: ignore[unresolved-attribute]
                t["user_id"], value_set=user_array
            )
        )
        if sub.num_rows:
            pairs = sub.group_by(["user_id", "author_id"]).aggregate([("post_id", "count")])
            for uid, aid, cnt in zip(
                pairs["user_id"].to_pylist(),
                pairs["author_id"].to_pylist(),
                pairs["post_id_count"].to_pylist(),
            ):
                user_author[(uid, aid)] += cnt
        logger.info(f"  统计 {f.name}: 训练期行数 {t.num_rows}")

    return {
        "post_count": post_count,
        "post_fav": post_fav,
        "post_reply": post_reply,
        "user_author": user_author,
    }


def load_post_meta(post_meta_path: str) -> tuple[dict[str, str], dict[str, int]]:
    t = pq.read_table(post_meta_path, columns=["post_id", "author_id", "create_time"])
    posts = t["post_id"].to_pylist()
    authors = t["author_id"].to_pylist()
    times = t["create_time"].to_pylist()
    return dict(zip(posts, authors)), dict(zip(posts, times))


def rule_score(age_days: float, network: bool, likes: int, replies: int) -> float:
    freshness = RULE_FRESHNESS_WEIGHT * max(0.0, 1.0 - age_days / RULE_FRESHNESS_DAYS)
    engagement = (
        RULE_ENGAGEMENT_WEIGHT * math.log1p(likes + 2.0 * replies) * math.pow(0.5, age_days)
    )
    return freshness + (RULE_NETWORK_BONUS if network else 0.0) + engagement


def baseline_scores(rows: dict, stats: dict, post_author: dict, post_time: dict) -> dict:
    n, c = len(rows["candidates"]), rows["num_candidates"]
    popularity = np.zeros((n, c))
    affinity = np.zeros((n, c))
    rule = np.zeros((n, c))
    valid = np.zeros((n, c), dtype=bool)

    for i, (uid, t, cands) in enumerate(
        zip(rows["user_id"], rows["event_time"], rows["candidates"])
    ):
        for j, pid in enumerate(cands):
            if pid is None:
                continue
            valid[i, j] = True
            author = post_author.get(pid, "")
            interactions = stats["user_author"].get((uid, author), 0)
            popularity[i, j] = stats["post_count"].get(pid, 0)
            affinity[i, j] = interactions
            created = post_time.get(pid)
            age_days = max(0.0, (t - created) / DAY_SECONDS) if created else RULE_FRESHNESS_DAYS
            # 评估集没有 Candidate.source，只能用用户-作者历史互动近似线上 Network source。
            # 因此该分数不是 recommendation-service fallback 的逐字节复刻。
            rule[i, j] = rule_score(
                age_days,
                interactions > 0,
                stats["post_fav"].get(pid, 0),
                stats["post_reply"].get(pid, 0),
            )
    positive = np.zeros((n, c), dtype=bool)
    positive[:, 0] = True
    return {
        "valid": valid,
        "positive": positive,
        "scores": {
            "popularity": popularity,
            "rule_approximation": rule,
            "author_affinity": affinity,
        },
    }


def phoenix_scores(ckpt_dir: str, eval_dir: str, max_samples: int, batch_size: int) -> np.ndarray:
    with open(os.path.join(ckpt_dir, "metadata.json"), encoding="utf-8") as f:
        metadata = json.load(f)
    params = tr.load_checkpoint(os.path.join(ckpt_dir, metadata["model_params"]))
    tables = tr.load_embedding_tables(os.path.join(ckpt_dir, metadata["embedding_tables"]))
    emb_state = tr.init_embedding_state(*tables)
    head_mask = tr.resolve_head_mask(",".join(metadata["observed_actions"]))
    data = tr.load_parquet_dataset(eval_dir, max_samples=max_samples)
    probs = tr.predict_probs(params, emb_state, data, batch_size)
    binary_heads = [i for i in range(18) if head_mask[i] > 0]
    return probs[:, :, binary_heads].sum(axis=-1)


def main():
    parser = argparse.ArgumentParser(description="Phoenix 精排离线基线对照")
    parser.add_argument("--eval-dir", required=True, help="评估集 Parquet 目录")
    parser.add_argument("--behavior-dir", required=True, help="原始行为日志目录（用于基线统计）")
    parser.add_argument("--post-meta", required=True, help="帖子元数据 Parquet")
    parser.add_argument("--ckpt-dir", default=None, help="Phoenix 产物目录（含 metadata.json）；不传则只跑基线")
    parser.add_argument("--max-samples", type=int, default=20000)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--output", default=None, help="把结果写成 JSON")
    args = parser.parse_args()

    rows = load_eval_rows(args.eval_dir, args.max_samples)
    train_end = int(rows["event_time"].min())
    logger.info(f"评估样本 {len(rows['candidates'])} 条，训练期截止 event_time < {train_end}")

    post_author, post_time = load_post_meta(args.post_meta)
    stats = load_training_stats(args.behavior_dir, train_end, set(rows["user_id"]))
    base = baseline_scores(rows, stats, post_author, post_time)
    scores = dict(base["scores"])

    if args.ckpt_dir:
        phoenix = phoenix_scores(args.ckpt_dir, args.eval_dir, args.max_samples, args.batch_size)
        assert phoenix.shape == base["valid"].shape, (phoenix.shape, base["valid"].shape)
        scores["phoenix"] = phoenix

    results = {}
    for name, score in scores.items():
        results[name] = tr.ranking_metrics(score, base["valid"], base["positive"])
    random_hr1 = next(iter(results.values()))["random_hr@1"]

    logger.info(f"{'scorer':18s} {'HR@1':>8s} {'MRR':>8s}")
    logger.info(f"{'random':18s} {random_hr1:8.4f} {'-':>8s}")
    for name, m in results.items():
        logger.info(f"{name:18s} {m['hr@1']:8.4f} {m['mrr']:8.4f}")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump({"train_end": train_end, "results": results}, f, ensure_ascii=False, indent=2)
        logger.info(f"结果已写入 {args.output}")


if __name__ == "__main__":
    main()
