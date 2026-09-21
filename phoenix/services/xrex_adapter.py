"""Home Mixer Phoenix-contract adapter for the xrex gRPC services.

The adapter deliberately owns all action-taxonomy and retrieval-index
translation. Identity is already numeric: Home Mixer and xrex share the
Registry-allocated SnowflakeId space, so IDs pass through with range checks.
"""

from __future__ import annotations

import math
from typing import Any

import grpc

from services.id_registry_client import IdentityRegistryClient  # noqa: F401
from services.model_contract import (
    FEATURE_SCHEMA,
    IDENTITY_MAPPING_VERSION,
    RANDOM_MODEL_VERSION,
)
from services.recsys_proto import load_proto_modules
from xai_proto import recsys_pb2 as xrex_pb2
from xai_proto import recsys_pb2_grpc as xrex_grpc


recsys_pb2, recsys_grpc = load_proto_modules()

# xrex ActionName -> Home Mixer ActionName. Values are proto enum numbers.
XREX_TO_PHOENIX_ACTION = {
    1: 1,   # fav
    4: 2,   # reply
    5: 4,   # quote
    6: 3,   # retweet
    11: 12,  # dwell
    12: 20,  # not dwell
    13: 8,   # video quality view
    14: 5,   # photo expand
    16: 18,  # report
    17: 15,  # not interested
    21: 14,  # follow author
    23: 16,  # block author
    25: 17,  # mute author
    29: 6,   # click
    30: 7,   # click profile
    32: 11,  # copy link
    33: 10,  # direct message
    37: 9,   # share
    38: 13,  # quoted tweet click
    48: 19,  # quoted tweet video quality view
}
PHOENIX_TO_XREX_ACTION = {value: key for key, value in XREX_TO_PHOENIX_ACTION.items()}


class PhoenixXrexTranslator:
    """Pure protobuf conversion and fail-closed response validation.

    The Home-facing Phoenix contract is numeric: SnowflakeIds allocated by the
    shared ID Registry. The translator validates range and passes them through;
    it never calls the Registry.
    """

    @staticmethod
    def _numeric_id(value: int, field: str) -> int:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
            or value > 0x7FFF_FFFF_FFFF_FFFF
        ):
            raise ValueError(f"invalid Snowflake {field}: {value!r}")
        return value

    def _xrex_tweet(self, tweet: Any) -> Any:
        result = xrex_pb2.TweetInfo(
            tweetId=self._numeric_id(tweet.tweet_id, "tweet_id"),
            authorId=self._numeric_id(tweet.author_id, "author_id"),
            safetyLabelMask=tweet.safety_label_mask,
        )
        if tweet.in_reply_to_tweet_id:
            result.inReplyToTweetId = self._numeric_id(
                tweet.in_reply_to_tweet_id, "in_reply_to_tweet_id"
            )
        return result

    def _xrex_sequence(self, sequence: Any, user_id: int) -> Any:
        source = sequence or recsys_pb2.UserActionSequence(user_id=user_id)
        numeric_user = self._numeric_id(user_id, "user_id")
        xrex_sequence = xrex_pb2.UserActionSequence(userId=numeric_user)
        if source.metadata:
            xrex_sequence.metadata.CopyFrom(
                xrex_pb2.UserActionSequenceMeta(
                    length=source.metadata.length,
                    firstSequenceTime=source.metadata.first_sequence_time,
                    lastSequenceTime=source.metadata.last_sequence_time,
                    lastModifiedEpochMs=source.metadata.last_modified_epoch_ms,
                    previousKafkaPublishEpochMs=source.metadata.previous_kafka_publish_epoch_ms,
                )
            )

        actions = []
        data = source.user_actions_data
        if data and data.HasField("ordered_aggregated_user_actions_list"):
            for item in data.ordered_aggregated_user_actions_list.aggregated_user_actions:
                xrex_item = xrex_pb2.AggregatedUserAction(
                    userId=numeric_user,
                    tweetInfo=self._xrex_tweet(
                        recsys_pb2.TweetInfo(
                            tweet_id=item.tweet_id,
                            author_id=item.author_id,
                        )
                    ),
                    impressedTimeMs=item.impressed_time_ms,
                )
                for phoenix_action, enabled in enumerate(item.action_mask):
                    if enabled and phoenix_action in PHOENIX_TO_XREX_ACTION:
                        xrex_item.actions.add(
                            actionName=PHOENIX_TO_XREX_ACTION[phoenix_action],
                            actionCategory=1,
                            actionGroup=0,
                        )
                if xrex_item.actions:
                    xrex_item.actions[0].userActionMeta.productSurface = item.product_surface
                actions.append(xrex_item)

        if actions:
            aggregated = xrex_sequence.userActionsData.orderedAggregatedUserActionsList
            aggregated.aggregatedUserActions.extend(actions)
        return xrex_sequence

    def prediction_request(self, request: Any) -> Any:
        user_id = self._numeric_id(request.user_id, "user_id")
        candidates = [self._xrex_tweet(candidate) for candidate in request.candidates]
        candidate_set = xrex_pb2.CandidateSet(
            userId=user_id,
            candidates=candidates,
            productSurface="PRODUCT_SURFACE_HOME_TIMELINE_RANKING",
        )
        return xrex_pb2.PredictNextActionsRequest(
            sequences=[self._xrex_sequence(request.user_action_sequence, user_id)],
            candidateSets=[candidate_set],
            returnLogprob=True,
            returnLogMap=True,
            requestedActionIndices=sorted(PHOENIX_TO_XREX_ACTION.values()),
            requestedContinuousActionIndices=[1],
        )

    def prediction_response(self, request: Any, response: Any) -> Any:
        if len(response.distributionSets) != 1:
            raise ValueError("xrex prediction response must contain one distribution set")
        requested = {
            self._numeric_id(item.tweet_id, "tweet_id"): item for item in request.candidates
        }
        distributions = []
        returned_ids = set()
        for item in response.distributionSets[0].candidateDistributions:
            candidate_id = item.candidate.tweetId
            original = requested.get(candidate_id)
            if original is None:
                raise ValueError(f"xrex returned unknown candidate {candidate_id}")
            if candidate_id in returned_ids:
                raise ValueError(f"xrex returned duplicate candidate {candidate_id}")
            returned_ids.add(candidate_id)
            if item.candidate.authorId != self._numeric_id(original.author_id, "author_id"):
                raise ValueError(f"xrex author mismatch for candidate {original.tweet_id}")
            values = [0.0] * 19
            for xrex_action, phoenix_action in XREX_TO_PHOENIX_ACTION.items():
                # Home Mixer v2 has a 19-slot response contract. The two
                # reserved enum values (19/20) are not part of that array.
                if phoenix_action < len(values) and xrex_action < len(item.topLogProbs):
                    values[phoenix_action] = item.topLogProbs[xrex_action]
            continuous = list(item.continuousActionsValues[:2])
            continuous.extend([0.0] * (2 - len(continuous)))
            distributions.append(
                recsys_pb2.CandidateDistribution(
                    candidate=original,
                    top_log_probs=values,
                    continuous_actions_values=continuous,
                )
            )
        if returned_ids != set(requested):
            raise ValueError("xrex prediction response is missing candidates")
        return recsys_pb2.PredictNextActionsResponse(
            distribution_sets=[recsys_pb2.DistributionSet(candidate_distributions=distributions)]
        )

    def retrieval_request(self, request: Any) -> Any:
        user_id = self._numeric_id(request.user_id, "user_id")
        return xrex_pb2.RetrieveTopKCandidatesRequest(
            user_ids=[user_id],
            sequences=[self._xrex_sequence(request.user_action_sequence, user_id)],
            topK=request.max_results,
        )

    def retrieval_response(self, response: Any) -> Any:
        groups = []
        for group in response.topKCandidates:
            candidates = []
            for item in group.candidates:
                tweet_id = self._numeric_id(item.candidate.tweetId, "tweet_id")
                author_id = self._numeric_id(item.candidate.authorId, "author_id")
                if not math.isfinite(item.score):
                    raise ValueError(f"xrex returned non-finite score for {tweet_id}")
                candidate = recsys_pb2.TweetInfo(
                    tweet_id=tweet_id,
                    author_id=author_id,
                    safety_label_mask=item.candidate.safetyLabelMask,
                )
                candidates.append(recsys_pb2.ScoredCandidate(candidate=candidate, score=item.score))
            groups.append(recsys_pb2.ScoredCandidates(candidates=candidates))
        return recsys_pb2.RetrieveResponse(top_k_candidates=groups)


class PhoenixAdapter(
    recsys_grpc.PhoenixPredictionServiceServicer,
    recsys_grpc.PhoenixRetrievalServiceServicer,
):
    """Home Mixer-facing gRPC server backed by xrex stubs."""

    def __init__(
        self,
        xrex_address: str,
        model_version: str,
        identity_mapping_sha256: str,
    ) -> None:
        model_version = model_version.strip()
        if not model_version or model_version == RANDOM_MODEL_VERSION:
            raise ValueError("a non-random model version is required")
        if len(identity_mapping_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in identity_mapping_sha256
        ):
            raise ValueError("identity mapping sha256 must be 64 lowercase hex characters")
        self._channel = grpc.insecure_channel(xrex_address)
        self._prediction = xrex_grpc.RecsysPredictorStub(self._channel)
        self._retrieval = xrex_grpc.RecsysRetrievalPredictorStub(self._channel)
        self._translator = PhoenixXrexTranslator()
        self._model_version = model_version
        self._identity_mapping_sha256 = identity_mapping_sha256

    def _metadata(self, context: Any) -> None:
        context.set_trailing_metadata(
            (
                ("feature-schema", FEATURE_SCHEMA),
                ("identity-map-version", str(IDENTITY_MAPPING_VERSION)),
                ("identity-map-sha256", self._identity_mapping_sha256),
                ("model-version", self._model_version),
                ("random-weights", "false"),
                ("supported-actions", "1,2,18"),
            )
        )

    def PredictNextActions(self, request: Any, context: Any) -> Any:
        try:
            response = self._prediction.PredictNextActions(
                self._translator.prediction_request(request),
                timeout=context.time_remaining(),
            )
            result = self._translator.prediction_response(request, response)
            self._metadata(context)
            return result
        except (ValueError, KeyError) as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except (RuntimeError, grpc.RpcError) as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))

    def Retrieve(self, request: Any, context: Any) -> Any:
        try:
            response = self._retrieval.RetrieveTopKCandidates(
                self._translator.retrieval_request(request),
                timeout=context.time_remaining(),
            )
            result = self._translator.retrieval_response(response)
            self._metadata(context)
            return result
        except (ValueError, KeyError) as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except (RuntimeError, grpc.RpcError) as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
