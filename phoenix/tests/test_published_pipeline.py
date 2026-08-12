import numpy as np
import pytest

from services.inference_types import CandidatePrediction
from services.published_gateway import PublishedRetrievalEngine
from services.published_pipeline import IDX_FAV, IDX_REPLY, PublishedPipeline


class FakeRetrievalEngine:
    post_ids = np.array([11, 22], dtype=np.int64)
    corpus_size = 2

    def retrieve_from_json(self, user_id, sequence, top_k):
        assert user_id == sequence["user_id"]
        assert top_k == 2
        return [(11, 101, 0.9), (22, 202, 0.8)]

    def topic_for_post(self, post_id):
        return {11: "one", 22: "two"}[post_id]


class FakeRankerEngine:
    def predict_from_json(
        self, user_id, sequence, candidate_ids, *, impression_timestamp
    ):
        assert user_id == sequence["user_id"]
        assert candidate_ids == [(11, 101), (22, 202)]
        assert impression_timestamp == 1_800_000_000
        first = np.zeros(19, dtype=np.float32)
        first[IDX_FAV] = 0.1
        second = np.zeros(19, dtype=np.float32)
        second[IDX_REPLY] = 1.0
        return [CandidatePrediction(first), CandidatePrediction(second)]


def test_published_pipeline_runs_shared_retrieval_ranking_and_mapping():
    pipeline = PublishedPipeline.from_engines(FakeRankerEngine(), FakeRetrievalEngine())

    result = pipeline.run(
        {"user_id": 42, "history": []},
        top_k_retrieval=2,
        impression_timestamp=1_800_000_000,
    )

    assert result.user_id == 42
    assert result.corpus_count == 2
    assert [candidate.post_id for candidate in result.candidates] == [22, 11]
    assert [candidate.topic for candidate in result.candidates] == ["two", "one"]


def test_published_pipeline_rejects_ranker_length_mismatch():
    class ShortRanker(FakeRankerEngine):
        def predict_from_json(self, *args, **kwargs):
            return []

    pipeline = PublishedPipeline.from_engines(ShortRanker(), FakeRetrievalEngine())

    with pytest.raises(ValueError, match="length mismatch"):
        pipeline.run(
            {"user_id": 42, "history": []},
            top_k_retrieval=2,
            impression_timestamp=1_800_000_000,
        )


def test_topic_lookup_uses_prebuilt_index():
    engine = PublishedRetrievalEngine.__new__(PublishedRetrievalEngine)
    engine._topic_by_post = {11: "one"}

    assert engine.topic_for_post(11) == "one"
    assert engine.topic_for_post(99) == ""
