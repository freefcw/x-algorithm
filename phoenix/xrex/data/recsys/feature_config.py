# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from __future__ import annotations

import enum


class CategoricalFeature(enum.IntEnum):
    productSurfaceSeq = 0
    timezoneSeq = 5
    localHourOfDaySeq = 6
    localDayOfWeekSeq = 7
    postAgeBucketSeq = 8
    favCountBucketSeq = 9
    replyCountBucketSeq = 10
    repostCountBucketSeq = 11
    quoteCountBucketSeq = 12
    viewCountBucketSeq = 13
    authorIsNsfwSeq = 14


COMPUTED_CATEGORICAL_FEATURE_NAMES: frozenset[str] = frozenset(
    {
        "localHourOfDaySeq",
        "localDayOfWeekSeq",
        "postAgeBucketSeq",
        "favCountBucketSeq",
        "replyCountBucketSeq",
        "repostCountBucketSeq",
        "quoteCountBucketSeq",
        "viewCountBucketSeq",
        "authorIsNsfwSeq",
    }
)

AUTHOR_NSFW_BIT = 2


class BoolFeature(enum.IntEnum):
    pass


class FloatFeature(enum.IntEnum):
    pass


class Int64Feature(enum.IntEnum):
    clientAppIdSeq = 0
    promotedIdSeq = 1
    favCountSeq = 5
    replyCountSeq = 6
    repostCountSeq = 7
    quoteCountSeq = 8
    viewCountSeq = 9
    ipAddressSeq = 10


CATEGORICAL_FEATURES: list[str] = [f.name for f in CategoricalFeature]
BOOL_FEATURES: list[str] = [f.name for f in BoolFeature]
FLOAT_FEATURES: list[str] = [f.name for f in FloatFeature]
INT64_FEATURES: list[str] = [f.name for f in Int64Feature]

ENGAGEMENT_COUNT_BUCKET_MAP: tuple[tuple[CategoricalFeature, Int64Feature], ...] = (
    (CategoricalFeature.favCountBucketSeq, Int64Feature.favCountSeq),
    (CategoricalFeature.replyCountBucketSeq, Int64Feature.replyCountSeq),
    (CategoricalFeature.repostCountBucketSeq, Int64Feature.repostCountSeq),
    (CategoricalFeature.quoteCountBucketSeq, Int64Feature.quoteCountSeq),
    (CategoricalFeature.viewCountBucketSeq, Int64Feature.viewCountSeq),
)


class UserCategoricalFeature(enum.IntEnum):
    userLanguageCode = 0
    userCountryCode = 1
    userState = 2
    userGender = 3
    userAgeBracket = 4
    userDmaCode = 5
    userInferredGender = 6


class UserBoolFeature(enum.IntEnum):
    pass


class UserFloatFeature(enum.IntEnum):
    userLongitude = 0
    userLatitude = 1
    userInferredGenderScore = 2
    userAgeInYears = 3


class UserInt64Feature(enum.IntEnum):
    pass


USER_CATEGORICAL_FEATURES: list[str] = [f.name for f in UserCategoricalFeature]
USER_BOOL_FEATURES: list[str] = [f.name for f in UserBoolFeature]
USER_FLOAT_FEATURES: list[str] = [f.name for f in UserFloatFeature]
USER_INT64_FEATURES: list[str] = [f.name for f in UserInt64Feature]

BASE_REQUIRED_COLUMNS: list[str] = [
    "length",
    "userId",
    "tweetIdSeq",
    "authorIdSeq",
    "promotedIdSeq",
    "impressedTimeMsSeq",
    "clientAppIdSeq",
    "actionNameMultiHotSeqSeq",
    "continuousActionValuesSeqSeq",
    "paddingMask",
    "newEventMask",
    "installedAppsMultiHot",
]


def _build_required_columns() -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for col in BASE_REQUIRED_COLUMNS:
        if col not in seen:
            result.append(col)
            seen.add(col)
    for feature_list in (
        CATEGORICAL_FEATURES,
        BOOL_FEATURES,
        FLOAT_FEATURES,
        INT64_FEATURES,
        USER_CATEGORICAL_FEATURES,
        USER_BOOL_FEATURES,
        USER_FLOAT_FEATURES,
        USER_INT64_FEATURES,
    ):
        for col in feature_list:
            if col not in seen and col not in COMPUTED_CATEGORICAL_FEATURE_NAMES:
                result.append(col)
                seen.add(col)
    return result


REQUIRED_COLUMNS: list[str] = _build_required_columns()

OPTIONAL_COLUMNS: list[str] = [
    "searchQueryEmbeddingSeq",
    "lineItemObjectiveSeq",
    "safetyLabelMaskSeq",
    "semanticIdSeq",
    "sampleWeight",
    "authorFollowerCountSeq",
    "inReplyToPostIdSeq",
]
