import ast
import subprocess
import sys
from pathlib import Path

from xrex.data.recsys import feature_config


def test_public_post_bool_schema_is_stable_and_optional():
    assert feature_config.STALE_POST_14D_TTL_SEC == 1_209_600
    assert 1 << feature_config.AUTHOR_NSFW_BIT == 4
    assert list(feature_config.BoolFeature) == [
        feature_config.BoolFeature.isAuthorFollowedByViewerSeq,
        feature_config.BoolFeature.isAuthorFollowingViewerSeq,
        feature_config.BoolFeature.isStalePost14d,
    ]
    assert [feature.value for feature in feature_config.BoolFeature] == [0, 1, 2]

    bool_names = {feature.name for feature in feature_config.BoolFeature}
    assert bool_names == feature_config.OPTIONAL_BOOL_FEATURE_NAMES
    assert bool_names.isdisjoint(feature_config.REQUIRED_COLUMNS)


def test_all_python_server_entrypoints_forward_stale_post_config():
    inference_dir = Path(__file__).parents[1] / "xrex" / "inference"
    expected_calls = {
        "model_runner.py": 2,
        "sid_retrieval_runner.py": 1,
        "gen_recs_runner.py": 1,
    }

    for filename, expected_count in expected_calls.items():
        tree = ast.parse((inference_dir / filename).read_text())
        forwarded = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg == "enable_stale_post":
                    forwarded.append(ast.unparse(keyword.value))
        assert forwarded == ["self.enable_stale_post"] * expected_count


def test_legacy_batches_default_missing_bool_features_to_false():
    code = """
from xrex.data.recsys import recsys_batch
arrays = recsys_batch.empty_feature_arrays(batch_size=2, seq_len=4)
assert recsys_batch.POST_BOOL_FEATURE_SIZE == 3
assert arrays[\"bool_features\"].shape == (2, 4, 3)
assert str(arrays[\"bool_features\"].dtype) == \"bool\"
assert not arrays[\"bool_features\"].any()
"""
    subprocess.run([sys.executable, "-c", code], check=True)
