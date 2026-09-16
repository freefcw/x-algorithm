#!/usr/bin/env python3
"""
离线构建召回向量索引：用候选塔把真实帖子编码成向量，写出网关可加载的 ``.npz``。

这是"召回候选池变真"的离线半边。在线半边是 `scripts/run_grpc_gateway.py --corpus-path`，
它在启动时加载本脚本的产物，并按 ``--corpus-refresh-seconds`` 周期检查文件是否被重建。
两边必须使用同一个 retrieval checkpoint + 嵌入表，否则网关会拒绝加载（模型版本不一致）。

输入（Parquet / CSV / JSON Lines，按扩展名识别）至少两列：
    post_id         帖子 ID，24 位小写 hex ObjectId（mrpyq feed_id）
    author_id       作者皮 ID，24 位小写 hex ObjectId（mrpyq creator_member_id）
可选列：
    created_at_ms   发布时间（毫秒）。提供时按 --max-age-hours 过滤，默认 48 小时，
                    与 home-mixer AgeFilter（params::MAX_POST_AGE）一致——更老的帖子即使
                    被召回也会在 home-mixer 被丢弃，索引里不必保留。

输入应只包含"当前可推荐"的帖子（mrpyq 一级 recommendation_eligible、未删除、作者皮 ID
非空）；本脚本不做业务资格判断，只做 ID 形状校验和去重。

用法:
    uv run scripts/build_retrieval_index.py \
        --posts data/recommendable_posts.parquet \
        --retrieval-checkpoint checkpoints_retrieval/retrieval_params_step200.npz \
        --emb-tables checkpoints_retrieval/embedding_tables.npz \
        --output indexes/retrieval_index.npz

    # 本地冒烟：随机权重也能建索引（网关非 demo 模式会拒绝随机权重，只用于跑通链路）
    uv run scripts/build_retrieval_index.py --posts posts.csv --output /tmp/idx.npz --allow-random

写出是原子的（临时文件 + rename），正在运行的网关不会读到半个文件；定时任务直接覆盖
同一路径即可完成滚动刷新。
"""

import _setup_path  # noqa: F401

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

from services.retrieval_index import RetrievalIndex, is_object_id, now_ms

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_retrieval_index")

DEFAULT_MAX_AGE_HOURS = 48.0  # 与 home-mixer params::MAX_POST_AGE 对齐


def read_posts(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True, dtype=False)
    raise SystemExit(f"不支持的输入格式 {suffix!r}（支持 .parquet / .csv / .jsonl）")


def select_posts(
    df: pd.DataFrame, max_age_hours: float, reference_ms: int
) -> tuple[list[str], list[str], dict[str, int]]:
    """校验 ID 形状、去重、按帖龄过滤；返回 (post_ids, author_ids, 丢弃计数)。"""
    for column in ("post_id", "author_id"):
        if column not in df.columns:
            raise SystemExit(f"输入缺少必需列 {column!r}，现有列：{list(df.columns)}")

    dropped = {"invalid_post_id": 0, "invalid_author_id": 0, "duplicate": 0, "too_old": 0}
    cutoff_ms = None
    if max_age_hours > 0 and "created_at_ms" in df.columns:
        cutoff_ms = reference_ms - int(max_age_hours * 3600 * 1000)
        created = pd.to_numeric(df["created_at_ms"], errors="coerce")
    else:
        created = None

    post_ids: list[str] = []
    author_ids: list[str] = []
    seen: set[str] = set()
    for row_index, (post_id, author_id) in enumerate(
        zip(df["post_id"].astype(str), df["author_id"].astype(str))
    ):
        post_id = post_id.strip()
        author_id = author_id.strip()
        if not is_object_id(post_id):
            dropped["invalid_post_id"] += 1
            continue
        if not is_object_id(author_id):
            dropped["invalid_author_id"] += 1
            continue
        if post_id in seen:
            dropped["duplicate"] += 1
            continue
        if created is not None:
            created_ms = created.iloc[row_index]
            # 缺失发布时间的帖子保留：过滤只针对确定过期的帖子。
            if pd.notna(created_ms) and cutoff_ms is not None and created_ms < cutoff_ms:
                dropped["too_old"] += 1
                continue
        seen.add(post_id)
        post_ids.append(post_id)
        author_ids.append(author_id)
    return post_ids, author_ids, dropped


def main() -> None:
    parser = argparse.ArgumentParser(description="用候选塔离线编码真实帖子，构建召回索引")
    parser.add_argument("--posts", required=True, help="帖子清单（parquet / csv / jsonl）")
    parser.add_argument(
        "--retrieval-checkpoint",
        default=None,
        help="召回模型参数（train_retrieval.py 产出的 retrieval_params_step*.npz）",
    )
    parser.add_argument(
        "--emb-tables",
        default=None,
        help="嵌入表 embedding_tables.npz；必须与 checkpoint 来自同一次训练",
    )
    parser.add_argument("--output", required=True, help="输出索引路径（.npz）")
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help="只保留 created_at_ms 在最近 N 小时内的帖子（默认 48，0 表示不过滤）",
    )
    parser.add_argument(
        "--allow-random",
        action="store_true",
        help="允许在没有 checkpoint 时用随机权重建索引（仅本地冒烟；线上网关会拒绝随机权重）",
    )
    args = parser.parse_args()

    if args.retrieval_checkpoint is None and not args.allow_random:
        parser.error(
            "未提供 --retrieval-checkpoint；随机权重的索引没有意义，冒烟请加 --allow-random"
        )
    if (args.retrieval_checkpoint is None) != (args.emb_tables is None):
        parser.error("--retrieval-checkpoint 与 --emb-tables 必须同时提供或同时省略")

    posts_path = Path(args.posts)
    if not posts_path.exists():
        parser.error(f"帖子清单不存在：{posts_path}")

    logger.info("读取帖子清单 %s", posts_path)
    df = read_posts(posts_path)
    post_ids, author_ids, dropped = select_posts(df, args.max_age_hours, now_ms())
    logger.info(
        "输入 %d 行 → 保留 %d 条；丢弃 invalid_post_id=%d invalid_author_id=%d "
        "duplicate=%d too_old=%d",
        len(df),
        len(post_ids),
        dropped["invalid_post_id"],
        dropped["invalid_author_id"],
        dropped["duplicate"],
        dropped["too_old"],
    )
    if not post_ids:
        logger.error("没有可编码的帖子，不写出索引")
        sys.exit(2)

    # 延迟导入：JAX 初始化较慢，先把输入问题报出来。
    from services.grpc_gateway import EmbeddingTables, RetrievalEngine

    tables = EmbeddingTables(args.emb_tables)
    engine = RetrievalEngine(tables, args.retrieval_checkpoint, corpus_size=0)

    started = time.time()
    embeddings = engine.encode_posts(post_ids, author_ids)
    logger.info("编码完成：%d 条，维度 %d，耗时 %.1f s", *embeddings.shape, time.time() - started)

    index = RetrievalIndex(
        post_ids=tuple(post_ids),
        author_ids=tuple(author_ids),
        embeddings=embeddings,
        model_version=engine.model_version,
        built_at_ms=now_ms(),
    )
    output = index.save(args.output)
    logger.info("索引已写出 %s（%s）", output, index.describe())
    logger.info(
        "网关加载：uv run scripts/run_grpc_gateway.py --retrieval-checkpoint %s%s --corpus-path %s",
        args.retrieval_checkpoint or "<none>",
        f" --emb-tables {args.emb_tables}" if args.emb_tables else "",
        output,
    )


if __name__ == "__main__":
    main()
