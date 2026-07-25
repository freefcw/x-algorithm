"""gRPC engines backed by the published Phoenix artifact bundle."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import List, Sequence, Tuple

import numpy as np

from recsys_model import PhoenixModelConfig, RecsysBatch, RecsysEmbeddings
from recsys_retrieval_model import PhoenixRetrievalModelConfig
from run_pipeline import build_hash_functions, build_model_config, build_unified_emb_table
from runners import (
    ModelRunner,
    RecsysInferenceRunner,
    RecsysRetrievalInferenceRunner,
    RetrievalModelRunner,
    load_embedding_table,
)
from services.grpc_gateway import (
    ACTION_IDX_TO_ENUM,
    CandidatePrediction,
    HistoryFeatures,
    TWITTER_EPOCH_MS,
)


class PublishedFeatureAdapter:
    """Converts public proto inputs into the exact published checkpoint tensors."""

    def __init__(self, artifact_dir: Path):
        with (artifact_dir / "config.json").open(encoding="utf-8") as config_file:
            self.config = json.load(config_file)

        self.history_len = int(self.config["history_seq_len"])
        self.candidate_len = int(self.config["candidate_seq_len"])
        self.num_actions = int(self.config["num_actions"])
        self.num_continuous_actions = 8
        self.surface_vocab = int(self.config.get("product_surface_vocab_size", 16))
        self.hash_user, self.hash_item, self.hash_author = build_hash_functions(
            self.config
        )
        self.embedding_table = build_unified_emb_table(
            load_embedding_table(artifact_dir / "embedding_tables.npz"),
            self.config,
        )

    def history_from_uas(self, uas) -> HistoryFeatures:
        post_ids = np.zeros(self.history_len, dtype=np.uint64)
        author_ids = np.zeros(self.history_len, dtype=np.uint64)
        actions = np.zeros(
            (1, self.history_len, self.num_actions), dtype=np.float32
        )
        surface = np.zeros((1, self.history_len), dtype=np.int32)

        records = []
        if uas is not None and uas.HasField("user_actions_data"):
            container = uas.user_actions_data
            if container.HasField("ordered_aggregated_user_actions_list"):
                records = list(
                    container.ordered_aggregated_user_actions_list.aggregated_user_actions
                )
        records = records[-self.history_len :]

        for index, record in enumerate(records):
            post_ids[index] = record.tweet_id
            author_ids[index] = record.author_id
            surface[0, index] = record.product_surface % self.surface_vocab
            mask = list(record.action_mask)
            for model_index, enum_value in enumerate(ACTION_IDX_TO_ENUM):
                if model_index >= self.num_actions:
                    break
                if enum_value < len(mask) and mask[enum_value]:
                    actions[0, index, model_index] = 1.0

        return HistoryFeatures(
            post_hashes=self.hash_item(post_ids).reshape(
                1, self.history_len, -1
            ),
            author_hashes=self.hash_author(author_ids).reshape(
                1, self.history_len, -1
            ),
            actions=actions,
            product_surface=surface,
        )

    def build_batch(
        self,
        user_id: int,
        history: HistoryFeatures,
        candidate_post_ids: Sequence[int],
        candidate_author_ids: Sequence[int],
        *,
        ranker_features: bool,
    ) -> RecsysBatch:
        post_ids = np.zeros(self.candidate_len, dtype=np.uint64)
        author_ids = np.zeros(self.candidate_len, dtype=np.uint64)
        candidate_count = min(len(candidate_post_ids), self.candidate_len)
        post_ids[:candidate_count] = candidate_post_ids[:candidate_count]
        author_ids[:candidate_count] = candidate_author_ids[:candidate_count]

        batch = RecsysBatch(
            user_hashes=self.hash_user(
                np.asarray([user_id], dtype=np.uint64)
            ),
            history_post_hashes=history.post_hashes,
            history_author_hashes=history.author_hashes,
            history_actions=history.actions,
            history_product_surface=history.product_surface,
            candidate_post_hashes=self.hash_item(post_ids).reshape(
                1, self.candidate_len, -1
            ),
            candidate_author_hashes=self.hash_author(author_ids).reshape(
                1, self.candidate_len, -1
            ),
            candidate_product_surface=np.zeros(
                (1, self.candidate_len), dtype=np.int32
            ),
        )
        if not ranker_features:
            return batch

        now_seconds = int(time.time())
        creation_seconds = np.zeros(self.candidate_len, dtype=np.int64)
        for index, post_id in enumerate(post_ids[:candidate_count]):
            timestamp_ms = (int(post_id) >> 22) + TWITTER_EPOCH_MS
            if timestamp_ms > TWITTER_EPOCH_MS:
                creation_seconds[index] = timestamp_ms // 1000

        return batch._replace(
            history_continuous_actions=np.zeros(
                (1, self.history_len, self.num_continuous_actions),
                dtype=np.float32,
            ),
            candidate_impr_ts=np.full(
                (1, self.candidate_len), now_seconds, dtype=np.int64
            ),
            candidate_post_creation_ts=creation_seconds.reshape(
                1, self.candidate_len
            ),
        )

    def lookup(self, batch: RecsysBatch) -> RecsysEmbeddings:
        table = self.embedding_table
        return RecsysEmbeddings(
            user_embeddings=table[np.asarray(batch.user_hashes, dtype=np.intp)],
            history_post_embeddings=table[
                np.asarray(batch.history_post_hashes, dtype=np.intp)
            ],
            candidate_post_embeddings=table[
                np.asarray(batch.candidate_post_hashes, dtype=np.intp)
            ],
            history_author_embeddings=table[
                np.asarray(batch.history_author_hashes, dtype=np.intp)
            ],
            candidate_author_embeddings=table[
                np.asarray(batch.candidate_author_hashes, dtype=np.intp)
            ],
        )


class PublishedRankerEngine:
    def __init__(self, artifacts_dir: str):
        root = Path(artifacts_dir)
        artifact_dir = root / "ranker"
        self._features = PublishedFeatureAdapter(artifact_dir)
        model_config = build_model_config(
            self._features.config, PhoenixModelConfig
        )
        runner = RecsysInferenceRunner(
            runner=ModelRunner(model=model_config, bs_per_device=0.125),
            name="grpc_published_ranker",
        )
        checkpoint = artifact_dir / "model_params.npz"
        runner.initialize(checkpoint_path=checkpoint)
        self._runner = runner
        self._lock = threading.Lock()
        self.model_version = root.name

    def predict(
        self, user_id: int, uas, candidates: Sequence[Tuple[int, int]]
    ) -> List[CandidatePrediction]:
        history = self._features.history_from_uas(uas)
        predictions: List[CandidatePrediction] = []
        chunk_size = self._features.candidate_len

        with self._lock:
            for start in range(0, len(candidates), chunk_size):
                chunk = candidates[start : start + chunk_size]
                batch = self._features.build_batch(
                    user_id,
                    history,
                    [candidate[0] for candidate in chunk],
                    [candidate[1] for candidate in chunk],
                    ranker_features=True,
                )
                output = self._runner.rank(batch, self._features.lookup(batch))
                action_probs = np.asarray(output.scores[0], dtype=np.float64)
                continuous = (
                    None
                    if output.continuous_preds is None
                    else np.asarray(output.continuous_preds[0], dtype=np.float64)
                )
                for index in range(len(chunk)):
                    predictions.append(
                        CandidatePrediction(
                            action_probs=action_probs[index],
                            continuous_values=(
                                None if continuous is None else continuous[index]
                            ),
                        )
                    )

        return predictions


class PublishedRetrievalEngine:
    def __init__(self, artifacts_dir: str):
        root = Path(artifacts_dir)
        artifact_dir = root / "retrieval"
        self._features = PublishedFeatureAdapter(artifact_dir)
        model_config = build_model_config(
            self._features.config, PhoenixRetrievalModelConfig
        )
        runner = RecsysRetrievalInferenceRunner(
            runner=RetrievalModelRunner(model=model_config, bs_per_device=0.125),
            name="grpc_published_retrieval",
        )
        runner.initialize(checkpoint_path=artifact_dir / "model_params.npz")

        with np.load(root / "sports_corpus.npz", allow_pickle=False) as corpus:
            self._post_ids = np.asarray(corpus["post_ids"], dtype=np.int64)
            self._author_ids = np.asarray(corpus["author_ids"], dtype=np.int64)
            corpus_representations = np.asarray(
                corpus["candidate_representations"], dtype=np.float32
            )
        runner.set_corpus(corpus_representations, self._post_ids)

        self._runner = runner
        self._lock = threading.Lock()
        self.model_version = root.name

    def retrieve(
        self, user_id: int, uas, max_results: int
    ) -> List[Tuple[int, int, float]]:
        history = self._features.history_from_uas(uas)
        batch = self._features.build_batch(
            user_id, history, [], [], ranker_features=False
        )
        top_k = min(max(int(max_results), 1), len(self._post_ids))

        with self._lock:
            output = self._runner.retrieve(
                batch,
                self._features.lookup(batch),
                top_k=top_k,
            )

        indices = np.asarray(output.top_k_indices[0], dtype=np.intp)
        scores = np.asarray(output.top_k_scores[0], dtype=np.float64)
        return [
            (
                int(self._post_ids[index]),
                int(self._author_ids[index]),
                float(score),
            )
            for index, score in zip(indices, scores)
        ]
