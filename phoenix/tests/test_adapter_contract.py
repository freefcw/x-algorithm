from __future__ import annotations

import json
import urllib.request

import grpc
import pytest

from services.id_registry_client import IdentityRegistryClient
from services.recsys_proto import load_proto_modules
from services.xrex_adapter import (
    IDENTITY_MAPPING_VERSION,
    IdentityRegistryClient as LegacyIdentityRegistryClient,
    PhoenixAdapter,
    PhoenixXrexTranslator,
    XREX_TO_PHOENIX_ACTION,
)
from xai_proto import id_registry_pb2, id_registry_pb2_grpc
from xai_proto import recsys_pb2 as xrex_pb2


recsys_pb2, _ = load_proto_modules()


def oid(value: int) -> str:
    return f"{value:024x}"


def translator() -> PhoenixXrexTranslator:
    return PhoenixXrexTranslator()


def test_adapter_metadata_binds_model_and_identity_artifacts() -> None:
    class Context:
        metadata = ()

        def set_trailing_metadata(self, metadata) -> None:
            self.metadata = metadata

    context = Context()
    adapter = PhoenixAdapter("localhost:50054", "step-42@0123456789ab", "a" * 64)
    adapter._metadata(context)

    metadata = dict(context.metadata)
    assert metadata["model-version"] == "step-42@0123456789ab"
    assert metadata["identity-map-version"] == str(IDENTITY_MAPPING_VERSION)
    assert metadata["identity-map-sha256"] == "a" * 64


@pytest.mark.parametrize("model_version", ["", " ", "random"])
def test_adapter_rejects_unversioned_models(model_version: str) -> None:
    with pytest.raises(ValueError, match="model version"):
        PhoenixAdapter("localhost:50054", model_version, "a" * 64)


def test_prediction_request_passes_numeric_ids_without_a_registry() -> None:
    # The translator holds no registry: a batch of candidates must not trigger
    # any identity lookup. Identity is already numeric end to end.
    assert not hasattr(translator(), "_registry")
    request = recsys_pb2.PredictNextActionsRequest(
        user_id=11,
        candidates=[recsys_pb2.TweetInfo(tweet_id=22, author_id=33)],
    )

    translated = translator().prediction_request(request)

    assert translated.candidateSets[0].userId == 11
    assert translated.candidateSets[0].candidates[0].tweetId == 22
    assert translated.candidateSets[0].candidates[0].authorId == 33


def test_prediction_request_rejects_non_snowflake_ids() -> None:
    for bad in (0, 0x8000_0000_0000_0000, 0xFFFF_FFFF_FFFF_FFFF):
        request = recsys_pb2.PredictNextActionsRequest(
            user_id=bad,
            candidates=[recsys_pb2.TweetInfo(tweet_id=22, author_id=33)],
        )
        with pytest.raises(ValueError, match="Snowflake"):
            translator().prediction_request(request)


def test_prediction_request_converts_aggregated_uas() -> None:
    request = recsys_pb2.PredictNextActionsRequest(
        user_id=11,
        user_action_sequence=recsys_pb2.UserActionSequence(
            user_id=11,
            metadata=recsys_pb2.UserActionSequenceMeta(length=1),
            user_actions_data=recsys_pb2.UserActionSequenceDataContainer(
                ordered_aggregated_user_actions_list=recsys_pb2.AggregatedUserActionList(
                    aggregated_user_actions=[
                        recsys_pb2.AggregatedUserAction(
                            tweet_id=22,
                            author_id=33,
                            impressed_time_ms=1_700_000_000_000,
                            action_mask=[False, True, False],
                            product_surface=1,
                        )
                    ]
                )
            ),
        ),
        candidates=[recsys_pb2.TweetInfo(tweet_id=22, author_id=33)],
    )

    translated = translator().prediction_request(request)

    assert translated.sequences[0].userId == 11
    aggregated = translated.sequences[0].userActionsData.orderedAggregatedUserActionsList
    action = aggregated.aggregatedUserActions[0]
    assert action.tweetInfo.tweetId == 22
    assert action.tweetInfo.authorId == 33
    assert [item.actionName for item in action.actions] == [1]


def test_prediction_response_maps_xrex_actions_and_restores_requested_ids() -> None:
    request = recsys_pb2.PredictNextActionsRequest(
        user_id=11,
        candidates=[recsys_pb2.TweetInfo(tweet_id=22, author_id=33)],
    )
    response = xrex_pb2.PredictNextActionsResponse(
        distributionSets=[
            xrex_pb2.CandidateDistributionSet(
                candidateDistributions=[
                    xrex_pb2.NextActionDistribution(
                        candidate=xrex_pb2.TweetInfo(tweetId=22, authorId=33),
                        topLogProbs=[float(index) for index in range(200)],
                        continuousActionsValues=[0.0, 2.5],
                    )
                ]
            )
        ]
    )

    translated = translator().prediction_response(request, response)
    distribution = translated.distribution_sets[0].candidate_distributions[0]

    assert distribution.candidate.tweet_id == 22
    assert distribution.candidate.author_id == 33
    assert distribution.top_log_probs[1] == 1.0
    assert distribution.top_log_probs[2] == 4.0
    assert distribution.top_log_probs[3] == 6.0
    assert distribution.continuous_actions_values == [0.0, 2.5]
    assert XREX_TO_PHOENIX_ACTION[21] == 14


def test_prediction_response_rejects_unknown_candidates() -> None:
    request = recsys_pb2.PredictNextActionsRequest(
        user_id=11,
        candidates=[recsys_pb2.TweetInfo(tweet_id=22, author_id=33)],
    )
    response = xrex_pb2.PredictNextActionsResponse(
        distributionSets=[
            xrex_pb2.CandidateDistributionSet(
                candidateDistributions=[
                    xrex_pb2.NextActionDistribution(
                        candidate=xrex_pb2.TweetInfo(tweetId=99, authorId=33),
                        topLogProbs=[0.0] * 200,
                    )
                ]
            )
        ]
    )

    with pytest.raises(ValueError, match="unknown candidate"):
        translator().prediction_response(request, response)


def test_retrieval_response_passes_numeric_ids_without_a_registry() -> None:
    response = xrex_pb2.RetrieveTopKCandidatesResponse(
        topKCandidates=[
            xrex_pb2.ScoredCandidates(
                candidates=[
                    xrex_pb2.ScoredCandidate(
                        candidate=xrex_pb2.TweetInfo(tweetId=44, authorId=55),
                        score=0.25,
                        sourceIdx=0,
                        datasetType=1,
                    ),
                    xrex_pb2.ScoredCandidate(
                        candidate=xrex_pb2.TweetInfo(tweetId=66, authorId=55),
                        score=0.5,
                        sourceIdx=7,
                        datasetType=8,
                    ),
                ]
            )
        ]
    )

    translated = translator().retrieval_response(response)

    candidates = translated.top_k_candidates[0].candidates
    assert candidates[0].candidate.tweet_id == 44
    assert candidates[0].candidate.author_id == 55
    assert candidates[0].score == 0.25
    assert candidates[0].HasField("source_idx")
    assert candidates[0].source_idx == 0
    assert candidates[0].dataset_type == 1
    assert candidates[1].candidate.tweet_id == 66
    assert candidates[1].source_idx == 7
    assert candidates[1].dataset_type == 8


def test_retrieval_response_rejects_zero_ids_and_non_finite_scores() -> None:
    bad_ids = xrex_pb2.RetrieveTopKCandidatesResponse(
        topKCandidates=[
            xrex_pb2.ScoredCandidates(
                candidates=[
                    xrex_pb2.ScoredCandidate(
                        candidate=xrex_pb2.TweetInfo(tweetId=0, authorId=55),
                        score=0.25,
                    )
                ]
            )
        ]
    )
    with pytest.raises(ValueError, match="Snowflake"):
        translator().retrieval_response(bad_ids)

    bad_score = xrex_pb2.RetrieveTopKCandidatesResponse(
        topKCandidates=[
            xrex_pb2.ScoredCandidates(
                candidates=[
                    xrex_pb2.ScoredCandidate(
                        candidate=xrex_pb2.TweetInfo(tweetId=44, authorId=55),
                        score=float("nan"),
                    )
                ]
            )
        ]
    )
    with pytest.raises(ValueError, match="non-finite"):
        translator().retrieval_response(bad_score)


class _FakeResponse:
    def __init__(self, payload) -> None:
        self._payload = json.dumps(payload).encode()

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _patch_urlopen(monkeypatch: pytest.MonkeyPatch, payload, captured: list) -> None:
    def fake_urlopen(request, timeout=None):
        captured.append(request)
        return _FakeResponse(payload)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)


def test_registry_client_prefers_grpc_for_production_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = []

    class FakeStub:
        def __init__(self, channel) -> None:
            captured.append(("channel", channel))

        def ResolveBatch(self, request, timeout=None):
            captured.append((request, timeout))
            return id_registry_pb2.ResolveBatchResponse(
                rows=[
                    id_registry_pb2.ResolveResponse(
                        object_id=oid(1),
                        entity_kind=id_registry_pb2.POST,
                        snowflake_id=42,
                        mapping_version=IDENTITY_MAPPING_VERSION,
                    )
                ]
            )

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", FakeStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    monkeypatch.setenv("ID_REGISTRY_GRPC_ADDR", "registry.test:50072")
    client = IdentityRegistryClient()

    client.resolve_batch([(oid(1), "Post")])

    request, timeout = captured[1]
    assert request.ids[0].object_id == oid(1)
    assert request.ids[0].entity_kind == id_registry_pb2.POST
    assert timeout is not None and 0 < timeout <= 0.5


def test_registry_client_allocate_batch_uses_grpc_allocate_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    class FakeStub:
        def __init__(self, channel) -> None:
            pass

        def ResolveBatch(self, request, timeout=None):
            calls.append("ResolveBatch")
            raise AssertionError("provided ids must not use ResolveBatch")

        def AllocateBatch(self, request, timeout=None):
            calls.append("AllocateBatch")
            return id_registry_pb2.ResolveBatchResponse(
                rows=[
                    id_registry_pb2.ResolveResponse(
                        object_id=request.ids[0].object_id,
                        entity_kind=id_registry_pb2.POST,
                        snowflake_id=request.ids[0].snowflake_id,
                        mapping_version=IDENTITY_MAPPING_VERSION,
                    )
                ]
            )

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", FakeStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    client = IdentityRegistryClient(grpc_endpoint="registry.test:50072")

    client.allocate_batch([(oid(1), "Post", 42)])

    assert calls == ["AllocateBatch"]


def test_legacy_registry_client_import_uses_allocate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert LegacyIdentityRegistryClient is IdentityRegistryClient
    captured = []
    rows = [
        {
            "object_id": oid(1),
            "entity_kind": "Post",
            "snowflake_id": 42,
            "mapping_version": IDENTITY_MAPPING_VERSION,
        }
    ]
    _patch_urlopen(monkeypatch, rows, captured)
    client = LegacyIdentityRegistryClient("http://registry.test")

    client.allocate_batch([(oid(1), "Post", 42)])

    assert captured[0].full_url == "http://registry.test/v1/allocate:batch"


def test_registry_client_does_not_fallback_to_http_when_grpc_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def details(self):
            return "unavailable"

    class FailingStub:
        def __init__(self, channel) -> None:
            pass

        def ResolveBatch(self, request, timeout=None):
            raise Unavailable()

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", FailingStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *args, **kwargs: pytest.fail("gRPC failures must not retry over HTTP"),
    )
    client = IdentityRegistryClient(grpc_endpoint="registry.test:50070")

    with pytest.raises(RuntimeError, match="gRPC request failed"):
        client.resolve_batch([(oid(1), "Post")])


def test_registry_client_does_not_fallback_for_internal_grpc_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Internal(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.INTERNAL

    class FailingStub:
        def __init__(self, channel) -> None:
            pass

        def ResolveBatch(self, request, timeout=None):
            raise Internal()

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", FailingStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *args, **kwargs: pytest.fail("deterministic gRPC errors must not retry over HTTP"),
    )
    client = IdentityRegistryClient(grpc_endpoint="registry.test:50072")

    with pytest.raises(RuntimeError, match="gRPC request failed"):
        client.resolve_batch([(oid(1), "Post")])


def test_registry_client_gives_grpc_the_remaining_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    captured_timeouts: list[float] = []

    class CapturingStub:
        def __init__(self, channel) -> None:
            pass

        def ResolveBatch(self, request, timeout=None):
            assert timeout is not None
            captured_timeouts.append(timeout)
            raise Unavailable()

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", CapturingStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    client = IdentityRegistryClient(grpc_endpoint="registry.test:50072", timeout_seconds=0.5)

    with pytest.raises(RuntimeError, match="gRPC request failed"):
        client.resolve_batch([(oid(1), "Post")])

    assert captured_timeouts and 0 < captured_timeouts[0] <= 0.5


def test_registry_client_reverse_uses_grpc_and_validates_object_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeStub:
        def __init__(self, channel) -> None:
            pass

        def ReverseBatch(self, request, timeout=None):
            assert request.ids[0].snowflake_id == 42
            assert request.ids[0].entity_kind == id_registry_pb2.POST
            return id_registry_pb2.ReverseBatchResponse(
                rows=[
                    id_registry_pb2.ReverseResponse(
                        snowflake_id=42,
                        object_id=oid(7),
                        entity_kind=id_registry_pb2.POST,
                        mapping_version=IDENTITY_MAPPING_VERSION,
                    )
                ]
            )

    monkeypatch.setattr(id_registry_pb2_grpc, "IdentityRegistryServiceStub", FakeStub)
    monkeypatch.setattr("services.id_registry_client.grpc.insecure_channel", lambda endpoint: endpoint)
    client = IdentityRegistryClient(grpc_endpoint="registry.test:50072")

    assert client.reverse(42, "Post") == oid(7)


def test_registry_client_rejects_invalid_registry_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    client = IdentityRegistryClient("http://registry.test")

    with pytest.raises(ValueError, match="ObjectId"):
        client.resolve_batch([("not-an-object-id", "Post")])
    with pytest.raises(ValueError, match="entity_kind"):
        client.resolve_batch([(oid(1), "Comment")])
    with pytest.raises(ValueError, match="SnowflakeId"):
        client.reverse_batch([(0, "Post")])


def test_registry_client_allocate_batch_preserves_provided_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = []
    rows = [
        {
            "object_id": oid(1),
            "entity_kind": "Post",
            "snowflake_id": 42,
            "mapping_version": IDENTITY_MAPPING_VERSION,
        }
    ]
    _patch_urlopen(monkeypatch, rows, captured)
    client = IdentityRegistryClient("http://registry.test")

    client.allocate_batch([(oid(1), "Post", 42)])

    assert captured[0].full_url == "http://registry.test/v1/allocate:batch"
    request_body = json.loads(captured[0].data)
    assert request_body == {
        "ids": [
            {
                "object_id": oid(1),
                "entity_kind": "Post",
                "snowflake_id": 42,
            }
        ]
    }


def test_registry_client_rechecks_cached_value_for_provided_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = iter(
        [
            [{
                "object_id": oid(1),
                "entity_kind": "Post",
                "snowflake_id": 41,
                "mapping_version": IDENTITY_MAPPING_VERSION,
            }],
            [{
                "object_id": oid(1),
                "entity_kind": "Post",
                "snowflake_id": 42,
                "mapping_version": IDENTITY_MAPPING_VERSION,
            }],
        ]
    )
    calls = []

    def fake_urlopen(request, timeout=None):
        calls.append(json.loads(request.data))
        return _FakeResponse(next(payloads))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = IdentityRegistryClient("http://registry.test")

    client.resolve_batch([(oid(1), "Post")])
    client.allocate_batch([(oid(1), "Post", 42)])

    assert len(calls) == 2
    assert calls[1]["ids"][0]["snowflake_id"] == 42


def test_registry_client_resolve_batch_checks_mapping_version(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = []
    rows = [
        {
            "object_id": oid(1),
            "entity_kind": "Post",
            "snowflake_id": 42,
            "mapping_version": IDENTITY_MAPPING_VERSION,
        }
    ]
    _patch_urlopen(monkeypatch, rows, captured)
    client = IdentityRegistryClient("http://registry.test")

    client.resolve_batch([(oid(1), "Post")])

    request_body = json.loads(captured[0].data)
    assert request_body == {"ids": [{"object_id": oid(1), "entity_kind": "Post"}]}
    assert client.resolve(oid(1), "Post") == 42


def test_registry_client_resolve_batch_rejects_unknown_version(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_urlopen(
        monkeypatch,
        [{"object_id": oid(1), "entity_kind": "Post", "snowflake_id": 42, "mapping_version": 99}],
        [],
    )
    client = IdentityRegistryClient("http://registry.test")

    with pytest.raises(RuntimeError, match="mapping_version"):
        client.resolve_batch([(oid(1), "Post")])


def test_registry_client_reverse_batch_validates_order_kind_and_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = []
    rows = [
        {
            "snowflake_id": 42,
            "object_id": oid(7),
            "entity_kind": "Post",
            "mapping_version": IDENTITY_MAPPING_VERSION,
        }
    ]
    _patch_urlopen(monkeypatch, rows, captured)
    client = IdentityRegistryClient("http://registry.test")

    assert client.reverse(42, "Post") == oid(7)
    request_body = json.loads(captured[0].data)
    assert request_body == {"ids": [{"snowflake_id": 42, "entity_kind": "Post"}]}

    # Mismatched kind is rejected.
    _patch_urlopen(
        monkeypatch,
        [{**rows[0], "entity_kind": "User"}],
        [],
    )
    with pytest.raises(RuntimeError, match="entity kind"):
        IdentityRegistryClient("http://registry.test").reverse_batch([(42, "Post")])

    # Out-of-order snowflake ids are rejected.
    _patch_urlopen(
        monkeypatch,
        [{**rows[0], "snowflake_id": 43}],
        [],
    )
    with pytest.raises(RuntimeError, match="SnowflakeId"):
        IdentityRegistryClient("http://registry.test").reverse_batch([(42, "Post")])

    # Unknown mapping versions are rejected.
    _patch_urlopen(
        monkeypatch,
        [{**rows[0], "mapping_version": 99}],
        [],
    )
    with pytest.raises(RuntimeError, match="mapping_version"):
        IdentityRegistryClient("http://registry.test").reverse_batch([(42, "Post")])
