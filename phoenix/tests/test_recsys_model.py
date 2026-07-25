# Copyright 2026 X.AI Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import jax.numpy as jnp
import numpy as np
import pytest

from grok import make_recsys_attn_mask, right_anchored_rope_positions
from recsys_model import (
    ContinuousActionConfig,
    NormConfig,
    compute_post_age_bucket,
    normalize_continuous_value,
)


class TestMakeRecsysAttnMask:
    """Tests for the make_recsys_attn_mask function."""

    def test_output_shape(self):
        """Test that the output has the correct shape [1, 1, seq_len, seq_len]."""
        seq_len = 10
        candidate_start_offset = 5

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)

        assert mask.shape == (1, 1, seq_len, seq_len)

    def test_user_history_has_causal_attention(self):
        """Test that user+history positions (before candidate_start_offset) have causal attention."""
        seq_len = 8
        candidate_start_offset = 5

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        for i in range(candidate_start_offset):
            for j in range(candidate_start_offset):
                if j <= i:
                    assert mask_2d[i, j] == 1, f"Position {i} should attend to position {j}"
                else:
                    assert mask_2d[i, j] == 0, (
                        f"Position {i} should NOT attend to future position {j}"
                    )

    def test_candidates_attend_to_user_history(self):
        """Test that candidates can attend to all user+history positions."""
        seq_len = 8
        candidate_start_offset = 5

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        for candidate_pos in range(candidate_start_offset, seq_len):
            for history_pos in range(candidate_start_offset):
                assert mask_2d[candidate_pos, history_pos] == 1, (
                    f"Candidate at {candidate_pos} should attend to user+history at {history_pos}"
                )

    def test_candidates_attend_to_themselves(self):
        """Test that candidates can attend to themselves (self-attention)."""
        seq_len = 8
        candidate_start_offset = 5

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        for candidate_pos in range(candidate_start_offset, seq_len):
            assert mask_2d[candidate_pos, candidate_pos] == 1, (
                f"Candidate at {candidate_pos} should attend to itself"
            )

    def test_candidates_do_not_attend_to_other_candidates(self):
        """Test that candidates cannot attend to other candidates."""
        seq_len = 8
        candidate_start_offset = 5

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        for query_pos in range(candidate_start_offset, seq_len):
            for key_pos in range(candidate_start_offset, seq_len):
                if query_pos != key_pos:
                    assert mask_2d[query_pos, key_pos] == 0, (
                        f"Candidate at {query_pos} should NOT attend to candidate at {key_pos}"
                    )

    def test_full_mask_structure(self):
        """Test the complete mask structure with a small example."""
        # Sequence: [user, h1, h2, c1, c2, c3]
        # Positions:  0     1   2   3   4   5

        seq_len = 6
        candidate_start_offset = 3

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        # Expected mask structure:
        # Query positions are rows, key positions are columns
        # 1 = can attend, 0 = cannot attend
        #
        #        Keys:  u   h1  h2  c1  c2  c3
        # Query u   :   1   0   0   0   0   0
        # Query h1  :   1   1   0   0   0   0
        # Query h2  :   1   1   1   0   0   0
        # Query c1  :   1   1   1   1   0   0   <- c1 attends to user+history + self
        # Query c2  :   1   1   1   0   1   0   <- c2 attends to user+history + self
        # Query c3  :   1   1   1   0   0   1   <- c3 attends to user+history + self

        expected = np.array(
            [
                [1, 0, 0, 0, 0, 0],  # user
                [1, 1, 0, 0, 0, 0],  # h1
                [1, 1, 1, 0, 0, 0],  # h2
                [1, 1, 1, 1, 0, 0],  # c1: user+history + self
                [1, 1, 1, 0, 1, 0],  # c2: user+history + self
                [1, 1, 1, 0, 0, 1],  # c3: user+history + self
            ],
            dtype=np.float32,
        )

        np.testing.assert_array_equal(
            np.array(mask_2d),
            expected,
            err_msg="Full mask structure does not match expected pattern",
        )

    def test_dtype_preserved(self):
        """Test that the specified dtype is used."""
        seq_len = 5
        candidate_start_offset = 3

        mask_f32 = make_recsys_attn_mask(seq_len, candidate_start_offset, dtype=jnp.float32)
        mask_f16 = make_recsys_attn_mask(seq_len, candidate_start_offset, dtype=jnp.float16)

        assert mask_f32.dtype == jnp.float32
        assert mask_f16.dtype == jnp.float16

    def test_single_candidate(self):
        """Test edge case with a single candidate."""
        seq_len = 4
        candidate_start_offset = 3

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        expected = np.array(
            [
                [1, 0, 0, 0],
                [1, 1, 0, 0],
                [1, 1, 1, 0],
                [1, 1, 1, 1],
            ],
            dtype=np.float32,
        )

        np.testing.assert_array_equal(np.array(mask_2d), expected)

    def test_all_candidates(self):
        """Test edge case where all positions except first are candidates."""
        seq_len = 4
        candidate_start_offset = 1

        mask = make_recsys_attn_mask(seq_len, candidate_start_offset)
        mask_2d = mask[0, 0]

        expected = np.array(
            [
                [1, 0, 0, 0],  # user
                [1, 1, 0, 0],  # c1: user + self
                [1, 0, 1, 0],  # c2: user + self
                [1, 0, 0, 1],  # c3: user + self
            ],
            dtype=np.float32,
        )

        np.testing.assert_array_equal(np.array(mask_2d), expected)


class TestRightAnchoredRopePositions:
    """Tests for stable positions with right-padded user history."""

    def test_output_shape(self):
        padding_mask = jnp.ones((2, 10), dtype=jnp.bool_)

        positions = right_anchored_rope_positions(
            padding_mask, history_seq_len=6, num_user_prefix_tokens=1
        )

        assert positions.shape == (2, 10)

    def test_prefix_positions_are_preserved(self):
        padding_mask = jnp.ones((1, 10), dtype=jnp.bool_)

        positions = right_anchored_rope_positions(
            padding_mask, history_seq_len=6, num_user_prefix_tokens=2
        )

        np.testing.assert_array_equal(np.array(positions[0, :2]), [0.0, 1.0])

    def test_short_history_is_anchored_to_the_right(self):
        # Layout: [user, history, history, pad, pad, candidate, candidate]
        padding_mask = jnp.array(
            [[True, True, True, False, False, True, True]], dtype=jnp.bool_
        )

        positions = right_anchored_rope_positions(
            padding_mask, history_seq_len=4, num_user_prefix_tokens=1
        )

        np.testing.assert_array_equal(
            np.array(positions[0]), [0.0, 3.0, 4.0, 0.0, 0.0, 5.0, 5.0]
        )

    def test_candidates_share_history_end_position(self):
        padding_mask = jnp.ones((1, 8), dtype=jnp.bool_)

        positions = right_anchored_rope_positions(
            padding_mask, history_seq_len=4, num_user_prefix_tokens=1
        )

        np.testing.assert_array_equal(np.array(positions[0, 5:]), [5.0, 5.0, 5.0])

    def test_padding_positions_are_zero(self):
        padding_mask = jnp.array(
            [[True, True, True, True, False, False, False, False]], dtype=jnp.bool_
        )

        positions = right_anchored_rope_positions(
            padding_mask, history_seq_len=4, num_user_prefix_tokens=1
        )

        np.testing.assert_array_equal(np.array(positions[0, 4:]), np.zeros(4))


class TestComputePostAgeBucket:
    """Tests for converting post age into bounded model buckets."""

    def test_hour_boundaries_advance_to_the_next_bucket(self):
        impression = jnp.array([[1_000_000, 1_000_000, 1_000_000]])
        creation = jnp.array(
            [[1_000_000, 1_000_000 - 59 * 60, 1_000_000 - 60 * 60]]
        )

        buckets = compute_post_age_bucket(impression, creation, granularity_mins=60)

        np.testing.assert_array_equal(np.array(buckets), [[1, 1, 2]])

    def test_two_hour_old_post_uses_third_bucket(self):
        impression = jnp.array([[1_000_000]])
        creation = jnp.array([[1_000_000 - 120 * 60]])

        bucket = compute_post_age_bucket(impression, creation, granularity_mins=60)

        assert int(bucket[0, 0]) == 3

    @pytest.mark.parametrize(
        ("impression", "creation"),
        [(0, 1_000_000), (1_000_000, 0)],
    )
    def test_missing_timestamp_uses_unknown_bucket(self, impression, creation):
        bucket = compute_post_age_bucket(
            jnp.array([[impression]]),
            jnp.array([[creation]]),
            granularity_mins=60,
        )

        assert int(bucket[0, 0]) == 0

    def test_future_creation_timestamp_uses_unknown_bucket(self):
        impression = jnp.array([[1_000_000]])
        creation = jnp.array([[1_000_000 + 60 * 60]])

        bucket = compute_post_age_bucket(impression, creation, granularity_mins=60)

        assert int(bucket[0, 0]) == 0

    def test_very_old_post_uses_overflow_bucket(self):
        impression = jnp.array([[1_000_000]])
        creation = jnp.array([[1_000_000 - 5_000 * 60]])

        bucket = compute_post_age_bucket(impression, creation, granularity_mins=60)

        assert int(bucket[0, 0]) == 81

    def test_batch_shape_and_integer_dtype_are_preserved(self):
        impression = jnp.full((2, 3), 1_000_000)
        creation = jnp.array(
            [
                [1_000_000 - 30 * 60, 1_000_000 - 120 * 60, 0],
                [1_000_000, 1_000_000 - 60 * 60, 1_000_000 - 5_000 * 60],
            ]
        )

        buckets = compute_post_age_bucket(impression, creation, granularity_mins=60)

        assert buckets.shape == (2, 3)
        assert buckets.dtype == jnp.int32
        np.testing.assert_array_equal(np.array(buckets), [[1, 3, 0], [1, 2, 81]])


class TestNormalizeContinuousValue:
    """Tests for bounded continuous feature normalization."""

    def test_linear_normalization(self):
        config = NormConfig(norm_scale=30.0, use_log=False)
        values = jnp.array([0.0, 15.0, 30.0, 60.0])

        result = normalize_continuous_value(values, config)

        np.testing.assert_allclose(np.array(result), [0.0, 0.5, 1.0, 1.0])

    def test_log_normalization(self):
        config = NormConfig(norm_scale=30.0, use_log=True)
        values = jnp.array([0.0, 30.0])

        result = normalize_continuous_value(values, config)

        np.testing.assert_allclose(np.array(result), [0.0, 1.0], atol=1e-6)

    def test_values_are_clamped_to_configured_range(self):
        config = NormConfig(norm_scale=10.0, use_log=False)
        values = jnp.array([-5.0, 0.0, 5.0, 15.0])

        result = normalize_continuous_value(values, config)

        np.testing.assert_allclose(np.array(result), [0.0, 0.0, 0.5, 1.0])


class TestContinuousActionConfig:
    """Tests for continuous action configuration defaults."""

    def test_defaults_match_published_model_contract(self):
        config = ContinuousActionConfig()

        assert config.loss_weight == 0.0
        assert config.loss_type == "mae"
        assert config.tweedie_power == 1.5
        assert config.norm_config == NormConfig(norm_scale=30.0, use_log=False)

    def test_default_normalization_config_is_not_shared(self):
        first = ContinuousActionConfig()
        second = ContinuousActionConfig()

        assert first.norm_config is not second.norm_config

    def test_custom_normalization_config_is_preserved(self):
        norm_config = NormConfig(norm_scale=120.0, use_log=True)

        config = ContinuousActionConfig(norm_config=norm_config)

        assert config.norm_config is norm_config


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
