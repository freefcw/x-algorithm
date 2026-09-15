"""Regression tests for combined two-tower attention presets."""

from xrex.configs.xrecsys_two_tower import CONFIGS
from xrex.models.transformer import RematType


def _combined_config(name: str):
    return CONFIGS[f"{name}_aggregated_kafka"]


def test_combined_presets_satisfy_cutedsl_varlen_attention_prerequisites():
    """Both hardware presets must be valid for the CuTeDSL varlen kernel."""
    for name in ("xrecsys_two_tower_combined", "xrecsys_two_tower_combined_gb300"):
        user_tower = _combined_config(name).model_config.user_tower_config
        attention = user_tower.model_config.attn_config
        assert attention.qk_norm is True
        assert attention.attn_logit_cap <= 0

    attention = _combined_config("xrecsys_two_tower_combined").model_config.user_tower_config
    assert attention.model_config.attn_config.attn_impl == "cutedsl_ranker_varlen_attn"


def test_gb300_combined_preset_keeps_local_scaling_overrides():
    config = _combined_config("xrecsys_two_tower_combined_gb300")
    transformer = config.model_config.user_tower_config.model_config

    assert config.bs_per_device == 768
    assert config.parallel_config.ep == 32
    assert transformer.remat_policy is RematType.SAVE_GB300_RECSYS
    assert transformer.unroll_layer_stack is True
