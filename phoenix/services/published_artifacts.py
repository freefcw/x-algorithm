"""Shared published-artifact loading, hashing, and model configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Type

import numpy as np

from grok import TransformerConfig
from recsys_model import HashConfig, PhoenixModelConfig
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from runners import load_embedding_table, load_model_params

TWITTER_EPOCH_MS = 1288834974657


def _snowflake_creation_timestamp_seconds(post_ids):
    post_ids = np.asarray(post_ids, dtype=np.uint64)
    creation_seconds = ((post_ids >> 22) + TWITTER_EPOCH_MS) // 1000
    return np.where(creation_seconds > TWITTER_EPOCH_MS // 1000, creation_seconds, 0)


def infer_corpus_impression_timestamp(corpus_post_ids):
    creation_seconds = _snowflake_creation_timestamp_seconds(corpus_post_ids)
    latest_creation_seconds = int(np.max(creation_seconds, initial=0))
    if latest_creation_seconds == 0:
        raise ValueError("Corpus contains no valid Snowflake post IDs; pass --impression_timestamp")
    return latest_creation_seconds


def build_candidate_post_age_timestamps(candidate_post_ids, candidate_len, impression_seconds):
    post_ids = np.zeros(candidate_len, dtype=np.uint64)
    candidate_count = min(len(candidate_post_ids), candidate_len)
    post_ids[:candidate_count] = candidate_post_ids[:candidate_count]
    creation_seconds = _snowflake_creation_timestamp_seconds(post_ids)
    impression_seconds = np.full((1, candidate_len), impression_seconds, dtype=np.int64)
    return impression_seconds, creation_seconds.reshape(1, candidate_len).astype(np.int64)


def _hash_ids(ids, scales, biases, modulus, num_buckets):
    """Use the published linear congruential hash with wrapping int64 arithmetic."""
    ids = np.asarray(ids, dtype=np.int64).ravel()
    scales = np.asarray(scales, dtype=np.int64)
    biases = np.asarray(biases, dtype=np.int64)
    out = np.empty((len(ids), len(scales)), dtype=np.int32)
    with np.errstate(over="ignore"):
        for row, identifier in enumerate(ids):
            for column, (scale, bias) in enumerate(zip(scales, biases)):
                raw = (identifier * scale + bias) % np.int64(modulus)
                out[row, column] = (
                    0
                    if identifier == 0
                    else int((int(raw) % (num_buckets - 1)) + 1)
                )
    return out


def build_hash_functions(config):
    hp = config["hash_params"]
    pad = 65
    user_vocab = config["user_vocab_size"]
    item_vocab = config["item_vocab_size"]
    author_vocab = config["author_vocab_size"]

    def hash_user(user_ids):
        hashed = _hash_ids(
            user_ids,
            hp["user_hash_scales"],
            hp["user_biases"],
            hp["user_modulus"],
            user_vocab,
        )
        return np.where(hashed == 0, 0, hashed + pad)

    def hash_item(item_ids):
        hashed = _hash_ids(
            item_ids,
            hp["item_hash_scales"],
            hp["item_biases"],
            hp["item_modulus"],
            item_vocab,
        )
        return np.where(hashed == 0, 0, hashed + pad + user_vocab)

    def hash_author(author_ids):
        hashed = _hash_ids(
            author_ids,
            hp["author_hash_scales"],
            hp["author_biases"],
            hp["author_modulus"],
            author_vocab,
        )
        return np.where(
            hashed == 0,
            0,
            hashed + pad + user_vocab + item_vocab,
        )

    return hash_user, hash_item, hash_author


def build_unified_emb_table(embeddings, config):
    emb_size = config["emb_size"]
    user_vocab = config["user_vocab_size"]
    item_vocab = config["item_vocab_size"]
    author_vocab = config["author_vocab_size"]
    pad = 65
    table = np.zeros(
        (pad + user_vocab + item_vocab + author_vocab, emb_size),
        dtype=np.float32,
    )
    table[pad : pad + user_vocab] = embeddings["user_embeddings"]
    table[pad + user_vocab : pad + user_vocab + item_vocab] = embeddings[
        "item_embeddings"
    ]
    table[pad + user_vocab + item_vocab :] = embeddings["author_embeddings"]
    return table


def build_model_config(
    config,
    config_class: Type[PhoenixModelConfig] | Type[PhoenixRetrievalModelConfig],
):
    kwargs = dict(
        emb_size=config["emb_size"],
        history_seq_len=config["history_seq_len"],
        candidate_seq_len=config["candidate_seq_len"],
        hash_config=HashConfig(
            num_user_hashes=config["num_user_hashes"],
            num_item_hashes=config["num_item_hashes"],
            num_author_hashes=config["num_author_hashes"],
        ),
        product_surface_vocab_size=config.get("product_surface_vocab_size", 16),
        model=TransformerConfig(
            emb_size=config["emb_size"],
            key_size=config["key_size"],
            num_q_heads=config["num_heads"],
            num_kv_heads=config["num_heads"],
            num_layers=config["num_layers"],
            widening_factor=2.0,
            attn_output_multiplier=0.125,
        ),
    )
    if config_class is PhoenixModelConfig:
        kwargs["num_actions"] = config["num_actions"]
        kwargs["post_age_granularity_mins"] = config.get(
            "post_age_granularity_mins", 60
        )
        kwargs["enable_post_age"] = True
        kwargs["enable_continuous_actions"] = True
        kwargs["enable_continuous_predictions"] = True
        kwargs["right_anchored_rope"] = config.get("right_anchored_rope", False)
    elif config_class is PhoenixRetrievalModelConfig:
        kwargs["enable_linear_proj"] = True

    model_config = config_class(**kwargs)
    model_config.initialize()
    return model_config


class PublishedArtifact:
    """One ranker or retrieval artifact directory with validated shared assets."""

    def __init__(self, artifact_dir: str | Path):
        self.path = Path(artifact_dir)
        with (self.path / "config.json").open(encoding="utf-8") as config_file:
            self.config = json.load(config_file)
        self.params = load_model_params(self.path / "model_params.npz")
        self.embedding_table = build_unified_emb_table(
            load_embedding_table(self.path / "embedding_tables.npz"),
            self.config,
        )
