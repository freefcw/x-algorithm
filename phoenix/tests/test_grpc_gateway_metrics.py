"""gRPC 网关的 Prometheus 指标：两个 RPC 的计数 / 耗时 / 在途、引擎耗时、候选量与候选池大小。"""

import numpy as np
import pytest

from runners import ACTIONS
from services.gateway_metrics import GatewayMetrics
from services.grpc_gateway import create_servicers
from services.inference_types import CandidatePrediction
from services.recsys_proto import load_proto_modules

recsys_pb2, recsys_pb2_grpc = load_proto_modules()


class _Context:
    def set_trailing_metadata(self, metadata):
        self.metadata = dict(metadata)


class _Ranker:
    model_version = "unit-ranker"

    def __init__(self, fail_with: BaseException | None = None):
        self.fail_with = fail_with

    def predict(self, user_id, uas, candidates):
        if self.fail_with is not None:
            raise self.fail_with
        return [CandidatePrediction(action_probs=np.full(len(ACTIONS), 0.5)) for _ in candidates]


class _Retrieval:
    model_version = "unit-retrieval"
    corpus_version = "unit-retrieval@1:3"
    corpus_size = 3

    def retrieve(self, user_id, uas, max_results):
        return [
            ("69def6d4f0c8754f5c2fc994", "602e867f0de2d061ee418407", 0.9),
            ("69def6d4f0c8754f5c2fc995", "602e867f0de2d061ee418408", 0.8),
        ][:max_results]


def _predict_request(num_candidates: int):
    return recsys_pb2.PredictNextActionsRequest(
        user_id="5506dd82fbe78e7de77976ca",
        candidates=[
            recsys_pb2.TweetInfo(
                tweet_id=f"69def6d4f0c8754f5c2fc9{i:02x}", author_id="602e867f0de2d061ee418407"
            )
            for i in range(num_candidates)
        ],
    )


def _series(metrics: GatewayMetrics) -> str:
    return metrics.exposition().decode("utf-8")


def test_rpcs_are_counted_timed_and_release_in_flight():
    metrics = GatewayMetrics()
    prediction, retrieval = create_servicers(
        recsys_pb2, recsys_pb2_grpc, _Ranker(), _Retrieval(), metrics=metrics
    )

    prediction.PredictNextActions(_predict_request(3), _Context())
    prediction.PredictNextActions(_predict_request(1), _Context())
    retrieval.Retrieve(
        recsys_pb2.RetrieveRequest(user_id="5506dd82fbe78e7de77976ca", max_results=2),
        _Context(),
    )

    text = _series(metrics)
    for expected in [
        'phoenix_gateway_rpc_requests_total{code="OK",rpc="PredictNextActions"} 2.0',
        'phoenix_gateway_rpc_requests_total{code="OK",rpc="Retrieve"} 1.0',
        'phoenix_gateway_rpc_duration_seconds_count{rpc="PredictNextActions"} 2.0',
        'phoenix_gateway_rpc_in_flight{rpc="PredictNextActions"} 0.0',
        'phoenix_gateway_rpc_in_flight{rpc="Retrieve"} 0.0',
        'phoenix_gateway_engine_duration_seconds_count{engine="ranker"} 2.0',
        'phoenix_gateway_engine_duration_seconds_count{engine="retrieval"} 1.0',
        "phoenix_gateway_rank_candidates_count 2.0",
        "phoenix_gateway_rank_candidates_sum 4.0",
        "phoenix_gateway_retrieval_returned_sum 2.0",
        "phoenix_gateway_corpus_size 3.0",
    ]:
        assert expected in text, f"missing {expected}\n{text}"


def test_engine_failures_are_counted_by_status_and_re_raised():
    class _Aborted(Exception):
        def code(self):
            class _Code:
                name = "INVALID_ARGUMENT"

            return _Code()

    metrics = GatewayMetrics()
    for error, expected_code in [(RuntimeError("boom"), "UNKNOWN"), (_Aborted(), "INVALID_ARGUMENT")]:
        prediction, _ = create_servicers(
            recsys_pb2, recsys_pb2_grpc, _Ranker(fail_with=error), _Retrieval(), metrics=metrics
        )
        with pytest.raises(type(error)):
            prediction.PredictNextActions(_predict_request(1), _Context())
        assert (
            f'phoenix_gateway_rpc_requests_total{{code="{expected_code}",rpc="PredictNextActions"}} 1.0'
            in _series(metrics)
        )

    text = _series(metrics)
    assert 'phoenix_gateway_rpc_in_flight{rpc="PredictNextActions"} 0.0' in text
    assert 'code="OK",rpc="PredictNextActions"' not in text


def test_model_versions_are_exposed_as_info():
    metrics = GatewayMetrics()
    metrics.set_model_versions("step-000200@abc", "retrieval_params_step200@def")
    text = _series(metrics)
    assert (
        'phoenix_gateway_model_info{ranker_version="step-000200@abc",'
        'retrieval_version="retrieval_params_step200@def"} 1.0'
    ) in text


def test_servicers_without_metrics_still_work():
    prediction, _ = create_servicers(recsys_pb2, recsys_pb2_grpc, _Ranker(), _Retrieval())
    response = prediction.PredictNextActions(_predict_request(1), _Context())
    assert len(response.distribution_sets[0].candidate_distributions) == 1
