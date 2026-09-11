"""引擎侧（xrex）双塔用户塔 segment id 测试。

放在 `tests/engine/` 而不是 `tests/` 根目录：`xrex.models.recsys_two_tower_model` 顶层
`from xai_proto import recsys_pb2`，会把旧 Phoenix 引擎的 `recsys.proto` 注册进进程级
protobuf descriptor pool。演示链路（根目录模型 / scripts / services）的测试进程不应加载
xrex 与 xai_proto，所以默认 `uv run pytest` 不收集本目录，需显式运行：

    uv run pytest tests/engine
"""

import jax
import jax.numpy as jnp
import numpy as np

from xrex.models.recsys_two_tower_model import user_tower_segment_ids
from xrex.pallas.ranker_attention_utils import HISTORY_SEGMENT_ID, PADDING_SEGMENT_ID


class TestUserTowerSegmentIds:
    """Tests for distinguishing valid history tokens from padding tokens."""

    def test_padding_mask_marks_only_valid_tokens_as_history(self):
        padding_mask = jnp.array(
            [[True, True, False, False], [True, False, True, False]], dtype=jnp.bool_
        )

        segment_ids = user_tower_segment_ids(
            2,
            4,
            use_history_segment_ids=False,
            padding_mask=padding_mask,
        )

        expected = jnp.array(
            [
                [
                    HISTORY_SEGMENT_ID,
                    HISTORY_SEGMENT_ID,
                    PADDING_SEGMENT_ID,
                    PADDING_SEGMENT_ID,
                ],
                [
                    HISTORY_SEGMENT_ID,
                    PADDING_SEGMENT_ID,
                    HISTORY_SEGMENT_ID,
                    PADDING_SEGMENT_ID,
                ],
            ],
            dtype=jnp.int32,
        )
        np.testing.assert_array_equal(np.array(segment_ids), np.array(expected))

    def test_explicit_history_mode_ignores_padding_mask(self):
        padding_mask = jnp.array([[True, False, False]], dtype=jnp.bool_)

        segment_ids = user_tower_segment_ids(
            1,
            3,
            use_history_segment_ids=True,
            padding_mask=padding_mask,
        )

        np.testing.assert_array_equal(
            np.array(segment_ids), [[HISTORY_SEGMENT_ID] * 3]
        )

    def test_missing_padding_mask_preserves_zero_segments(self):
        segment_ids = user_tower_segment_ids(
            2,
            3,
            use_history_segment_ids=False,
        )

        np.testing.assert_array_equal(
            np.array(segment_ids), np.zeros((2, 3), dtype=np.int32)
        )

    def test_compiled_user_input_path_preserves_padding_segments(self):
        padding_mask = jnp.array(
            [[True, True, True, False, False]], dtype=jnp.bool_
        )
        build_segment_ids = jax.jit(
            lambda mask: user_tower_segment_ids(
                mask.shape[0],
                mask.shape[1],
                use_history_segment_ids=False,
                padding_mask=mask,
            )
        )

        segment_ids = build_segment_ids(padding_mask)

        np.testing.assert_array_equal(
            np.array(segment_ids),
            [[HISTORY_SEGMENT_ID] * 3 + [PADDING_SEGMENT_ID] * 2],
        )
