# Copyright 2026 X.AI Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Canonical offline entry for the published retrieval + ranking pipeline.

The upstream arguments remain stable. ``--impression_timestamp`` is the only
local additive argument and makes post-age features reproducible. Model loading,
hashing, preprocessing, forward execution, and output mapping live in the same
published core used by the gRPC gateway.
"""

import argparse
import json
import logging
import os

from services.published_artifacts import (
    TWITTER_EPOCH_MS,
    build_candidate_post_age_timestamps,
    build_hash_functions,
    build_model_config,
    build_unified_emb_table,
    infer_corpus_impression_timestamp,
)
from services.published_pipeline import (
    IDX_DWELL,
    IDX_FAV,
    IDX_REPLY,
    IDX_RT,
    IDX_VQV,
    PublishedPipeline,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger("jax").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# Re-exported helpers above preserve the upstream module contract for callers and tests.
__all__ = [
    "TWITTER_EPOCH_MS",
    "build_candidate_post_age_timestamps",
    "build_hash_functions",
    "build_model_config",
    "build_unified_emb_table",
    "infer_corpus_impression_timestamp",
    "IDX_FAV",
    "IDX_REPLY",
    "IDX_RT",
    "IDX_DWELL",
    "IDX_VQV",
    "main",
]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run retrieval + ranking pipeline")
    parser.add_argument("--artifacts_dir", default="./artifacts")
    parser.add_argument(
        "--sequence_file",
        default=None,
        help="Path to user action sequence JSON. Default: artifacts_dir/example_sequence.json",
    )
    parser.add_argument(
        "--corpus_file",
        default=None,
        help="Path to corpus NPZ. Default: artifacts_dir/sports_corpus.npz",
    )
    parser.add_argument("--top_k_retrieval", type=int, default=200)
    parser.add_argument("--top_k_display", type=int, default=30)
    parser.add_argument(
        "--impression_timestamp",
        type=int,
        default=None,
        help="Offline impression timestamp in Unix seconds; defaults to the corpus end time.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    artifacts = args.artifacts_dir
    sequence_file = args.sequence_file or os.path.join(
        artifacts, "example_sequence.json"
    )
    corpus_file = args.corpus_file or os.path.join(artifacts, "sports_corpus.npz")

    log.info("Loading user sequence from %s", sequence_file)
    with open(sequence_file, encoding="utf-8") as sequence_handle:
        sequence = json.load(sequence_handle)

    log.info("Loading published retrieval and ranking core from %s", artifacts)
    pipeline = PublishedPipeline(artifacts, corpus_file=corpus_file)
    result = pipeline.run(
        sequence,
        top_k_retrieval=args.top_k_retrieval,
        impression_timestamp=args.impression_timestamp,
    )
    display_results(result, args.top_k_display)
    return result


def display_results(result, top_k_display):
    display_count = min(top_k_display, len(result.candidates))
    print("\n" + "=" * 120)
    print(f"PIPELINE RESULTS - User {result.user_id}")
    print(f"History: {result.history_count} items | Corpus: {result.corpus_count} posts")
    print(f"Retrieved top {len(result.candidates)} -> Ranked by engagement model")
    print(f"Impression timestamp: {result.impression_timestamp}")
    print("=" * 120)
    print(
        f"{'Rank':<5} {'Score':<8} {'Ret':<7} {'Fav':<7} {'Reply':<7} "
        f"{'RT':<7} {'Dwell':<7} {'VQV':<7} {'Topics':<30} Post URL"
    )
    print("-" * 120)

    for rank, candidate in enumerate(result.candidates[:display_count], start=1):
        probabilities = candidate.prediction.action_probs
        print(
            f"{rank:<5} {candidate.weighted_score:<8.4f} "
            f"{candidate.retrieval_score:<7.4f} "
            f"{float(probabilities[IDX_FAV]):<7.4f} "
            f"{float(probabilities[IDX_REPLY]):<7.4f} "
            f"{float(probabilities[IDX_RT]):<7.4f} "
            f"{float(probabilities[IDX_DWELL]):<7.4f} "
            f"{float(probabilities[IDX_VQV]):<7.4f} "
            f"{candidate.topic[:28]:<30} "
            f"https://x.com/a/status/{candidate.post_id}"
        )

    if result.candidates:
        print(
            "\nWeighted score range: "
            f"[{result.candidates[-1].weighted_score:.4f}, "
            f"{result.candidates[0].weighted_score:.4f}]"
        )
    print("=" * 120)


if __name__ == "__main__":
    main()
