"""Shared published retrieval-to-ranking inference core."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from runners import ACTIONS
from services.inference_types import CandidatePrediction
from services.published_artifacts import infer_corpus_impression_timestamp
from services.published_gateway import PublishedRankerEngine, PublishedRetrievalEngine

ACTION_TO_INDEX = {name: index for index, name in enumerate(ACTIONS)}
IDX_FAV = ACTION_TO_INDEX["favorite_score"]
IDX_REPLY = ACTION_TO_INDEX["reply_score"]
IDX_RT = ACTION_TO_INDEX["repost_score"]
IDX_DWELL = ACTION_TO_INDEX["dwell_score"]
IDX_VQV = ACTION_TO_INDEX["vqv_score"]


@dataclass(frozen=True)
class PublishedPipelineCandidate:
    post_id: int
    author_id: int
    retrieval_score: float
    topic: str
    weighted_score: float
    prediction: CandidatePrediction


@dataclass(frozen=True)
class PublishedPipelineResult:
    user_id: int
    history_count: int
    corpus_count: int
    impression_timestamp: int
    candidates: list[PublishedPipelineCandidate]


class PublishedPipeline:
    """One model core used by the offline CLI and published gRPC adapters."""

    def __init__(self, artifacts_dir: str, corpus_file: str | None = None):
        self.ranker = PublishedRankerEngine(artifacts_dir)
        self.retrieval = PublishedRetrievalEngine(artifacts_dir, corpus_file=corpus_file)

    @classmethod
    def from_engines(cls, ranker, retrieval) -> "PublishedPipeline":
        pipeline = cls.__new__(cls)
        pipeline.ranker = ranker
        pipeline.retrieval = retrieval
        return pipeline

    def run(
        self,
        sequence,
        *,
        top_k_retrieval: int,
        impression_timestamp: int | None = None,
    ) -> PublishedPipelineResult:
        user_id = int(sequence["user_id"])
        snapshot = (
            infer_corpus_impression_timestamp(self.retrieval.post_ids)
            if impression_timestamp is None
            else int(impression_timestamp)
        )
        retrieved = self.retrieval.retrieve_from_json(
            user_id,
            sequence,
            top_k_retrieval,
        )
        candidate_ids = [(post_id, author_id) for post_id, author_id, _ in retrieved]
        predictions = self.ranker.predict_from_json(
            user_id,
            sequence,
            candidate_ids,
            impression_timestamp=snapshot,
        )
        candidates = self._rank_candidates(retrieved, predictions)
        return PublishedPipelineResult(
            user_id=user_id,
            history_count=len(sequence.get("history", [])),
            corpus_count=self.retrieval.corpus_size,
            impression_timestamp=snapshot,
            candidates=candidates,
        )

    def _rank_candidates(
        self,
        retrieved: Sequence[tuple[int, int, float]],
        predictions: Sequence[CandidatePrediction],
    ) -> list[PublishedPipelineCandidate]:
        if len(retrieved) != len(predictions):
            raise ValueError(
                "published pipeline length mismatch: "
                f"retrieved={len(retrieved)} predicted={len(predictions)}"
            )

        candidates = []
        for (post_id, author_id, retrieval_score), prediction in zip(
            retrieved, predictions
        ):
            probabilities = np.asarray(prediction.action_probs)
            weighted_score = float(
                probabilities[IDX_FAV]
                + probabilities[IDX_REPLY] * 0.5
                + probabilities[IDX_RT] * 0.3
                + probabilities[IDX_DWELL] * 0.2
            )
            candidates.append(
                PublishedPipelineCandidate(
                    post_id=post_id,
                    author_id=author_id,
                    retrieval_score=retrieval_score,
                    topic=self.retrieval.topic_for_post(post_id),
                    weighted_score=weighted_score,
                    prediction=prediction,
                )
            )

        return sorted(candidates, key=lambda candidate: candidate.weighted_score, reverse=True)
