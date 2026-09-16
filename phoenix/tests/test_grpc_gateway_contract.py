# gRPC 网关的跨语言契约测试
#
# 网关是 home-mixer（Rust / proto 契约）与 Phoenix 模型（Python ACTIONS 顺序）
# 之间的翻译层。这里的映射错一位不会报任何错——请求照常返回、分数照常输出，
# 只有排序悄悄张冠李戴。因此每个转换点都需要用独立表达方式对拍：
#
#   1. ACTION_IDX_TO_ENUM：Python 行为下标 → proto ActionName 枚举值
#   2. uas_to_history：proto 行为序列 → 模型历史特征
#   3. checkpoint 加载：训练脚本的 flatten npz → Haiku 嵌套参数
#   4. demo_object_id：演示 retrieval corpus 使用业务字符串 ID

import math

import numpy as np
import pytest

from data_preprocessor import hash_id_to_ints
from runners import ACTIONS
from services.grpc_gateway import (
    ACTION_IDX_TO_ENUM,
    HISTORY_LEN,
    NUM_HASHES,
    TABLE_SIZE,
    demo_object_id,
    uas_to_history,
)

# proto 桩代码需要 grpcio-tools 自动生成（uv sync --group service）
pytest.importorskip("grpc_tools", reason="需要 grpcio-tools 生成 proto 桩代码")

from services.recsys_proto import load_proto_modules

recsys_pb2, _ = load_proto_modules()


# ==================== 1. 行为映射表 ====================

# 与实现中按下标排列的 ACTION_IDX_TO_ENUM 相互独立的第二份表达：
# 按"行为名字"对应 proto 枚举名。两份表达对拍，任何一边错位都会被抓住。
ACTION_NAME_TO_PROTO_NAME = {
    "favorite_score": "SERVER_TWEET_FAV",
    "reply_score": "SERVER_TWEET_REPLY",
    "repost_score": "SERVER_TWEET_RETWEET",
    "photo_expand_score": "CLIENT_TWEET_PHOTO_EXPAND",
    "click_score": "CLIENT_TWEET_CLICK",
    "profile_click_score": "CLIENT_TWEET_CLICK_PROFILE",
    "vqv_score": "CLIENT_TWEET_VIDEO_QUALITY_VIEW",
    "share_score": "CLIENT_TWEET_SHARE",
    "share_via_dm_score": "CLIENT_TWEET_CLICK_SEND_VIA_DIRECT_MESSAGE",
    "share_via_copy_link_score": "CLIENT_TWEET_SHARE_VIA_COPY_LINK",
    "dwell_score": "CLIENT_TWEET_RECAP_DWELLED",
    "quote_score": "SERVER_TWEET_QUOTE",
    "quoted_click_score": "CLIENT_QUOTED_TWEET_CLICK",
    "follow_author_score": "CLIENT_TWEET_FOLLOW_AUTHOR",
    "not_interested_score": "CLIENT_TWEET_NOT_INTERESTED_IN",
    "block_author_score": "CLIENT_TWEET_BLOCK_AUTHOR",
    "mute_author_score": "CLIENT_TWEET_MUTE_AUTHOR",
    "report_score": "CLIENT_TWEET_REPORT",
}


def test_action_mapping_covers_all_discrete_actions():
    """映射表长度 = 离散行为数（ACTIONS 去掉连续值 dwell_time），且枚举值 1..18 恰好各出现一次。"""
    assert len(ACTION_IDX_TO_ENUM) == len(ACTIONS) - 1
    assert ACTIONS[-1] == "dwell_time", "连续值必须是 ACTIONS 最后一项，否则映射表下标假设失效"
    assert sorted(ACTION_IDX_TO_ENUM) == list(range(1, 19))


def test_action_mapping_semantic_alignment():
    """逐行为对拍：按下标的映射结果必须与按名字的独立映射一致。

    典型的会被抓住的错误：quote 在 Python 排第 11、在 proto 里是枚举 4，
    如果有人按"顺序一致"的直觉改表，这里立即失败。
    """
    for py_idx, enum_val in enumerate(ACTION_IDX_TO_ENUM):
        action_name = ACTIONS[py_idx]
        expected_proto_name = ACTION_NAME_TO_PROTO_NAME[action_name]
        actual_proto_name = recsys_pb2.ActionName.Name(enum_val)
        assert actual_proto_name == expected_proto_name, (
            f"Python 行为 {action_name}（下标 {py_idx}）被映射到 proto {actual_proto_name}，"
            f"期望 {expected_proto_name}"
        )


# ==================== 2. UAS → 历史特征 ====================


def _make_uas(records):
    return recsys_pb2.UserActionSequence(
        user_id="1",
        user_actions_data=recsys_pb2.UserActionSequenceDataContainer(
            ordered_aggregated_user_actions_list=recsys_pb2.AggregatedUserActionList(
                aggregated_user_actions=records,
                aggregation_provider="test",
            )
        ),
    )


def _make_record(tweet_id, author_id, active_enum_values=(), surface=1):
    mask = [False] * 19
    for v in active_enum_values:
        mask[v] = True
    return recsys_pb2.AggregatedUserAction(
        tweet_id=tweet_id,
        author_id=author_id,
        impressed_time_ms=1_700_000_000_000,
        action_mask=mask,
        product_surface=surface,
    )


def test_uas_to_history_maps_action_mask_to_python_order():
    """proto action_mask 的位下标是 ActionName 枚举值，转换后必须落在 Python ACTIONS 下标上。"""
    fav = recsys_pb2.ActionName.Value("SERVER_TWEET_FAV")          # 枚举 1
    quote = recsys_pb2.ActionName.Value("SERVER_TWEET_QUOTE")      # 枚举 4，Python 下标 11
    report = recsys_pb2.ActionName.Value("CLIENT_TWEET_REPORT")    # 枚举 18，Python 下标 17

    post_id, author_id = "69def6d4f0c8754f5c2fc994", "602e867f0de2d061ee418407"
    uas = _make_uas([_make_record(post_id, author_id, active_enum_values=[fav, quote, report])])
    history = uas_to_history(uas)

    row = history.actions[0, 0]
    assert row[ACTIONS.index("favorite_score")] == 1.0
    assert row[ACTIONS.index("quote_score")] == 1.0
    assert row[ACTIONS.index("report_score")] == 1.0
    # 其余行为位必须为 0（quote 枚举 4 若被错映射到 photo_expand 会在这里暴露）
    assert row.sum() == 3.0

    # 哈希必须与训练侧同源（data_preprocessor.hash_id_to_ints），且直接对业务字符串 ID
    # 取哈希——中间不存在任何整数映射，训练样本和线上请求查的是同一嵌入行。
    assert list(history.post_hashes[0, 0]) == hash_id_to_ints(post_id, NUM_HASHES, TABLE_SIZE)
    assert list(history.author_hashes[0, 0]) == hash_id_to_ints(
        author_id, NUM_HASHES, TABLE_SIZE
    )
    assert history.post_hashes[0, 0].min() > 0
    assert history.product_surface[0, 0] == 1


def test_uas_to_history_pads_and_truncates():
    """不足 HISTORY_LEN 补零 padding；超过时保留最近的（序列尾部）。"""
    # 只有 2 条：位置 0/1 有值，其余全零 padding
    uas = _make_uas([_make_record("1", "101"), _make_record("2", "102")])
    history = uas_to_history(uas)
    assert history.post_hashes[0, 1].min() > 0
    assert history.post_hashes[0, 2:].sum() == 0
    assert history.actions[0, 2:].sum() == 0

    # 40 条：应保留最后 HISTORY_LEN 条，第 0 位对应第 40-HISTORY_LEN 条记录
    records = [_make_record(f"post-{i}", "101") for i in range(40)]
    history = uas_to_history(_make_uas(records))
    first_kept_id = f"post-{40 - HISTORY_LEN}"
    assert list(history.post_hashes[0, 0]) == hash_id_to_ints(first_kept_id, NUM_HASHES, TABLE_SIZE)


def test_uas_to_history_handles_empty_sequence():
    """空序列不报错，返回全零特征（对应推荐服务侧 UAS 缺失的场景）。"""
    history = uas_to_history(None)
    assert history.post_hashes.sum() == 0
    assert history.actions.sum() == 0

    history = uas_to_history(_make_uas([]))
    assert history.post_hashes.sum() == 0


def test_servicer_echoes_business_string_ids():
    """候选 ID 以业务字符串进出 gRPC 层，servicer 不得做任何整数转换。"""
    from services.grpc_gateway import create_servicers
    from services.inference_types import CandidatePrediction
    from services.recsys_proto import load_proto_modules

    _, recsys_pb2_grpc = load_proto_modules()
    seen = {}

    class _Ranker:
        model_version = "unit"

        def predict(self, user_id, uas, candidates):
            seen["user_id"], seen["candidates"] = user_id, list(candidates)
            return [
                CandidatePrediction(action_probs=np.full(len(ACTIONS), 0.5))
                for _ in candidates
            ]

    class _Context:
        def set_trailing_metadata(self, metadata):
            seen["metadata"] = dict(metadata)

    servicer, _ = create_servicers(recsys_pb2, recsys_pb2_grpc, _Ranker(), None)
    request = recsys_pb2.PredictNextActionsRequest(
        user_id="5506dd82fbe78e7de77976ca",
        candidates=[
            recsys_pb2.TweetInfo(
                tweet_id="69def6d4f0c8754f5c2fc994", author_id="602e867f0de2d061ee418407"
            )
        ],
    )
    response = servicer.PredictNextActions(request, _Context())

    assert seen["user_id"] == "5506dd82fbe78e7de77976ca"
    assert seen["candidates"] == [("69def6d4f0c8754f5c2fc994", "602e867f0de2d061ee418407")]
    assert "id-mapping-version" not in seen["metadata"]
    assert seen["metadata"]["feature-schema"] == "phoenix-string-id-actions-v2"
    returned = response.distribution_sets[0].candidate_distributions[0].candidate
    assert returned.tweet_id == "69def6d4f0c8754f5c2fc994"
    assert returned.author_id == "602e867f0de2d061ee418407"


def test_default_supported_actions_are_the_v1_head_set():
    """没有 checkpoint metadata 时广播的 head 集合必须是 model_contract 的唯一定义。

    home-mixer 的 `REQUIRED_SUPPORTED_ACTIONS` 是同一决策的另一份拷贝（决策记录 §1.3：
    v1 = 点赞 1、评论 2、举报 18）。这里锁住 Python 侧的两处：常量本身和 servicer 默认值。
    """
    from services.grpc_gateway import create_servicers
    from services.inference_types import CandidatePrediction
    from services.model_contract import NONZERO_WEIGHT_ACTION_ENUMS, supported_actions_header

    assert NONZERO_WEIGHT_ACTION_ENUMS == (1, 2, 18)
    assert supported_actions_header() == "1,2,18"

    _, recsys_pb2_grpc = load_proto_modules()

    class _Ranker:
        model_version = "unit"

        def predict(self, user_id, uas, candidates):
            return [
                CandidatePrediction(action_probs=np.full(len(ACTIONS), 0.5))
                for _ in candidates
            ]

    class _Context:
        def __init__(self):
            self.metadata = None

        def set_trailing_metadata(self, metadata):
            self.metadata = dict(metadata)

    context = _Context()
    servicer, _ = create_servicers(recsys_pb2, recsys_pb2_grpc, _Ranker(), None)
    response = servicer.PredictNextActions(
        recsys_pb2.PredictNextActionsRequest(candidates=[recsys_pb2.TweetInfo(tweet_id="p", author_id="a")]),
        context,
    )

    assert context.metadata["supported-actions"] == supported_actions_header()
    distribution = response.distribution_sets[0].candidate_distributions[0]
    # 只有 v1 head 携带真实概率，其余位是 MIN_PROB 占位（click=6 是最容易被误开的一个）。
    assert distribution.top_log_probs[18] == pytest.approx(math.log(0.5))
    assert distribution.top_log_probs[6] == pytest.approx(math.log(1e-9))


def test_servicer_filters_unobserved_actions():
    """训练只观测部分行为时，gRPC 不得把其他 head 当成有效预测。"""
    from services.grpc_gateway import MIN_PROB, create_servicers
    from services.inference_types import CandidatePrediction

    _, recsys_pb2_grpc = load_proto_modules()

    class _Ranker:
        model_version = "unit"

        def predict(self, user_id, uas, candidates):
            return [
                CandidatePrediction(action_probs=np.full(len(ACTIONS), 0.5))
                for _ in candidates
            ]

    class _Context:
        def __init__(self):
            self.metadata = None

        def set_trailing_metadata(self, metadata):
            self.metadata = dict(metadata)

    context = _Context()
    servicer, _ = create_servicers(
        recsys_pb2,
        recsys_pb2_grpc,
        _Ranker(),
        None,
        supported_action_enums=[1, 2],
        continuous_dwell_supported=False,
    )
    response = servicer.PredictNextActions(
        recsys_pb2.PredictNextActionsRequest(candidates=[recsys_pb2.TweetInfo(tweet_id="p", author_id="a")]),
        context,
    )

    distribution = response.distribution_sets[0].candidate_distributions[0]
    assert context.metadata["supported-actions"] == "1,2"
    assert distribution.top_log_probs[1] == pytest.approx(math.log(0.5))
    assert distribution.top_log_probs[3] == pytest.approx(math.log(MIN_PROB))
    assert distribution.continuous_actions_values[1] == 0.0


# ==================== 3. checkpoint 加载 ====================


def test_npz_checkpoint_roundtrip(tmp_path):
    """训练脚本按 "{模块路径}/{参数名}" flatten 保存，加载后必须还原两层结构。

    Haiku 模块路径本身含 "/"（如 phoenix_model/decoder_layer_0/linear），
    还原时必须从右侧只切一次，否则参数树整体错位、模型加载后输出错乱。
    """
    from services.model_registry import create_model_registry

    params = {
        "phoenix_model/decoder_layer_0/linear": {
            "w": np.arange(6, dtype=np.float32).reshape(2, 3),
            "b": np.zeros(3, dtype=np.float32),
        },
        "phoenix_model/embedding": {
            "table": np.ones((4, 2), dtype=np.float32),
        },
    }
    # 按训练脚本（scripts/train_ranker.py::save_checkpoint）的格式保存
    flat = {f"{mod}/{name}": arr for mod, sub in params.items() for name, arr in sub.items()}
    path = tmp_path / "model_params_step1.npz"
    np.savez(path, **flat)

    loaded = create_model_registry(str(path)).get_params()

    assert set(loaded.keys()) == set(params.keys())
    for mod, sub in params.items():
        assert set(loaded[mod].keys()) == set(sub.keys())
        for name, arr in sub.items():
            np.testing.assert_allclose(np.asarray(loaded[mod][name]), arr)


# ==================== 4. model-version 绑定产物内容 ====================


def test_checkpoint_model_version_is_bound_to_artifact_content(tmp_path):
    """home-mixer 钉的 model-version 必须随参数或嵌入表内容变化，而不是随文件名。

    以前是 basename：两次训练都停在 step 200 就同名，只换嵌入表也看不出来，召回索引
    的版本校验会误通过。这里锁住：标签取自 bundle 目录 / 文件 stem，摘要来自数组内容——
    不是文件字节，因为 npz 是带时间戳的 zip、且 bundle 的嵌入文件还多存了 Adagrad 累加器。
    """
    import time

    from services.model_contract import RANDOM_MODEL_VERSION, checkpoint_model_version

    rng = np.random.default_rng(0)
    weights_v1 = {"transformer/linear/w": rng.normal(size=(4, 3)).astype(np.float32)}
    weights_v2 = {"transformer/linear/w": rng.normal(size=(4, 3)).astype(np.float32)}
    tables_v1 = {
        key: rng.normal(size=(5, 2)).astype(np.float32)
        for key in ("user_emb_table", "post_emb_table", "author_emb_table")
    }
    tables_v2 = dict(tables_v1, post_emb_table=rng.normal(size=(5, 2)).astype(np.float32))

    bundle = tmp_path / "step-000200"
    bundle.mkdir()
    params = bundle / "model_params.npz"
    tables = bundle / "embedding_tables.npz"
    np.savez(params, **weights_v1)
    # bundle 里的嵌入文件带累加器（train_ranker.save_embedding_state 的布局）。
    np.savez(tables, **tables_v1, user_emb_acc=np.ones(5, dtype=np.float32))

    version = checkpoint_model_version(params, tables)
    label, digest = version.split("@")
    assert label == "step-000200", "bundle 布局用目录名做标签，不再是 model_params.npz"
    assert len(digest) == 12 and all(c in "0123456789abcdef" for c in digest)
    assert version != RANDOM_MODEL_VERSION
    assert checkpoint_model_version(params, tables) == version, "确定性"

    # 同样的数组重新保存（zip 时间戳变了）→ 版本不变。
    time.sleep(1.1)
    np.savez(params, **weights_v1)
    assert checkpoint_model_version(params, tables) == version

    # train_retrieval 写出的"只有三张表"的副本必须与 bundle 的嵌入文件算出同一版本，
    # 否则按训练日志里的提示建索引会被网关拒绝。
    tables_only = tmp_path / "embedding_tables.npz"
    np.savez(tables_only, **tables_v1)
    assert checkpoint_model_version(params, tables_only) == version

    # 只换嵌入表 → 新版本（旧实现看不见）。
    np.savez(tables, **tables_v2, user_emb_acc=np.ones(5, dtype=np.float32))
    assert checkpoint_model_version(params, tables) != version

    # 另一次训练、同样的 step 目录名、不同参数 → 不同版本（旧实现会撞名）。
    other = tmp_path / "run-b" / "step-000200"
    other.mkdir(parents=True)
    np.savez(other / "model_params.npz", **weights_v2)
    np.savez(other / "embedding_tables.npz", **tables_v1)
    assert (
        checkpoint_model_version(other / "model_params.npz", other / "embedding_tables.npz")
        != version
    )

    # 平铺格式（召回 checkpoint）用文件 stem 做标签，嵌入表同样参与摘要。
    flat = tmp_path / "retrieval_params_step200.npz"
    np.savez(flat, **weights_v2)
    assert checkpoint_model_version(flat).startswith("retrieval_params_step200@")
    assert checkpoint_model_version(flat, tables_only) != checkpoint_model_version(flat)


def test_engine_model_version_is_random_without_checkpoint_and_metadata_mismatch_is_fatal():
    from services.grpc_gateway import (
        EmbeddingTables,
        check_ranker_model_version,
        resolve_model_version,
    )

    assert resolve_model_version(None, EmbeddingTables(None)) == "random"

    # 旧 metadata 没有 model_version：跳过；有且一致：通过；有且不一致：拒绝启动。
    check_ranker_model_version(None, "step-000200@abc")
    check_ranker_model_version({"step": 200}, "step-000200@abc")
    check_ranker_model_version({"model_version": "step-000200@abc"}, "step-000200@abc")
    with pytest.raises(ValueError, match="model_version"):
        check_ranker_model_version({"model_version": "step-000200@def"}, "step-000200@abc")


# ==================== 5. ObjectId-shaped string contract ====================


def test_demo_object_id_is_stable_and_not_time_encoded():
    first = demo_object_id(42)
    assert first == demo_object_id(42)
    assert first != demo_object_id(43)
    assert len(first) == 24
    assert all(char in "0123456789abcdef" for char in first)


def test_retrieval_demo_corpus_uses_object_id_shaped_authors():
    from services.grpc_gateway import demo_author_id

    assert demo_author_id(0) == "0000000000000000000000c9"
    assert demo_author_id(40) == demo_author_id(0)
    assert len(demo_author_id(1)) == 24
    assert all(char in "0123456789abcdef" for char in demo_author_id(1))
