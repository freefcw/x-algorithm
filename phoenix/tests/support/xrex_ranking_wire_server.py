"""Fake xrex ranking server plus the real adapter for the Rust wire contract test."""

from __future__ import annotations

import sys
from concurrent import futures
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.recsys_proto import load_proto_modules  # noqa: E402
from services.xrex_adapter import PhoenixAdapter  # noqa: E402
from xai_proto import recsys_pb2 as xrex_pb2  # noqa: E402
from xai_proto import recsys_pb2_grpc as xrex_grpc  # noqa: E402


class FakeRankingServer(xrex_grpc.RecsysPredictorServicer):
    def PredictNextActions(self, request, context):
        if len(request.candidateSets) != 1:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "expected one candidate set")

        distributions = []
        for candidate in request.candidateSets[0].candidates:
            log_probs = [-10.0] * 49
            log_probs[1], log_probs[4], log_probs[16] = -0.1, -0.2, -0.3
            if candidate.tweetId == 23:
                log_probs = log_probs[:16]  # Missing the required report head.
            distribution = xrex_pb2.NextActionDistribution(candidate=candidate)
            # Match the real engine's mutually exclusive map/array branches.
            if request.returnLogMap:
                distribution.indexToLogits.update({1: -0.1, 4: -0.2, 16: -0.3})
                distribution.indexToContinuousValues[1] = 2.5
            elif request.returnLogprob:
                distribution.topLogProbs.extend(log_probs)
                distribution.continuousActionsValues.extend([0.0, 2.5])
            else:
                distribution.logits.extend(log_probs)
                distribution.continuousActionsValues.extend([0.0, 2.5])
            distributions.append(distribution)

        return xrex_pb2.PredictNextActionsResponse(
            distributionSets=[
                xrex_pb2.CandidateDistributionSet(
                    userId=request.candidateSets[0].userId,
                    candidateDistributions=distributions,
                )
            ]
        )


def main() -> None:
    backend = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    xrex_grpc.add_RecsysPredictorServicer_to_server(FakeRankingServer(), backend)
    backend_port = backend.add_insecure_port("127.0.0.1:0")
    if not backend_port:
        raise RuntimeError("could not bind fake xrex ranking server")
    backend.start()

    _, legacy_grpc = load_proto_modules()
    adapter_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    legacy_grpc.add_PhoenixPredictionServiceServicer_to_server(
        PhoenixAdapter(f"127.0.0.1:{backend_port}", "test-checkpoint@0123456789ab", "a" * 64),
        adapter_server,
    )
    adapter_port = adapter_server.add_insecure_port("127.0.0.1:0")
    if not adapter_port:
        raise RuntimeError("could not bind Phoenix adapter")
    adapter_server.start()
    print(f"ADAPTER_PORT={adapter_port}", flush=True)
    adapter_server.wait_for_termination()


if __name__ == "__main__":
    main()
