import ast
import subprocess
import sys
from pathlib import Path

from xrex.data.recsys import feature_config


def test_public_post_bool_schema_is_stable_and_optional():
    # aad7179 起跟随上游公开排序（stale 前移为 0）与 TTL 终值；
    # 此前 c65aa17 的本地钉住顺序（followed/following/stale = 0/1/2）已废弃。
    assert feature_config.STALE_POST_14D_TTL_SEC == 1_213_200
    assert 1 << feature_config.AUTHOR_NSFW_BIT == 4
    assert list(feature_config.BoolFeature) == [
        feature_config.BoolFeature.isStalePost14d,
        feature_config.BoolFeature.isAuthorFollowedByViewerSeq,
        feature_config.BoolFeature.isAuthorFollowingViewerSeq,
    ]
    assert [feature.value for feature in feature_config.BoolFeature] == [0, 1, 2]

    bool_names = {feature.name for feature in feature_config.BoolFeature}
    assert bool_names == feature_config.OPTIONAL_BOOL_FEATURE_NAMES
    assert bool_names.isdisjoint(feature_config.REQUIRED_COLUMNS)


def test_predictor_servers_can_only_be_constructed_through_shared_factory():
    inference_dir = Path(__file__).parents[1] / "xrex" / "inference"
    server_names = {"RecsysPredictorServer", "RecsysRetrievalPredictorServer"}
    direct_constructions = []
    factory_calls = []

    for path in inference_dir.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id == "create_recsys_server":
                factory_calls.append((path.name, node.lineno))
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in server_names
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "xai_recsys_engine"
            ):
                direct_constructions.append((path.name, node.lineno))

    assert not direct_constructions
    assert len(factory_calls) == 4


def test_shared_server_factory_owns_the_stale_post_contract():
    code = """
from xrex.inference.server_factory import create_recsys_server

class Runner:
    enable_stale_post = True

captured = {}
def fake_server(*args, **kwargs):
    captured.update(kwargs)
    return object()

create_recsys_server(fake_server, Runner(), 1, custom="value")
assert captured["num_post_bool_features"] == 3
assert captured["enable_stale_post"] is True
assert captured["custom"] == "value"

try:
    create_recsys_server(fake_server, Runner(), num_post_bool_features=0)
except TypeError as error:
    assert "cannot be overridden" in str(error)
else:
    raise AssertionError("reserved feature arguments must be factory-owned")
"""
    subprocess.run([sys.executable, "-c", code], check=True)


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
