"""召回索引文件合同 + 网关加载 / 热替换 + 离线构建脚本的输入校验。

这条链路的失败模式都是"静默的"：索引里混进非法 ID 会让 home-mixer 边界丢弃候选而不报错；
模型和索引版本错配会让检索分数无意义但请求照常返回。所以每个转换点都要有显式拒绝。
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from services.retrieval_index import (
    INDEX_SCHEMA_VERSION,
    RetrievalIndex,
    RetrievalIndexError,
    is_object_id,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import build_retrieval_index as builder  # noqa: E402


def oid(index: int) -> str:
    return f"{index:024x}"


def make_index(ids, model_version="random", built_at_ms=1_700_000_000_000, dim=4):
    rng = np.random.default_rng(len(ids))
    return RetrievalIndex(
        post_ids=tuple(oid(i) for i in ids),
        author_ids=tuple(oid(1000 + i) for i in ids),
        embeddings=rng.normal(size=(len(ids), dim)).astype(np.float32),
        model_version=model_version,
        built_at_ms=built_at_ms,
    )


# ==================== 1. 文件合同 ====================


def test_object_id_shape_matches_home_mixer_boundary():
    assert is_object_id("69def6d4f0c8754f5c2fc994")
    assert not is_object_id("69DEF6D4F0C8754F5C2FC994"), "大写 hex 在 Rust 侧会被拒绝"
    assert not is_object_id("0" * 24), "nil ObjectId 不是合法帖子"
    assert not is_object_id("123")
    assert not is_object_id("69def6d4f0c8754f5c2fc99")


def test_index_rejects_ids_and_shapes_home_mixer_would_drop():
    good = make_index([1, 2, 3])
    with pytest.raises(RetrievalIndexError, match="post_id"):
        RetrievalIndex(("103",) + good.post_ids[1:], good.author_ids, good.embeddings, "m", 1)
    with pytest.raises(RetrievalIndexError, match="author_id"):
        RetrievalIndex(good.post_ids, ("not-a-id",) + good.author_ids[1:], good.embeddings, "m", 1)
    with pytest.raises(RetrievalIndexError, match="duplicate"):
        RetrievalIndex(
            (good.post_ids[0],) + good.post_ids[:2], good.author_ids, good.embeddings, "m", 1
        )
    with pytest.raises(RetrievalIndexError, match="same length"):
        RetrievalIndex(good.post_ids[:2], good.author_ids, good.embeddings, "m", 1)
    with pytest.raises(RetrievalIndexError, match="float32"):
        RetrievalIndex(good.post_ids, good.author_ids, good.embeddings.astype(np.float64), "m", 1)
    nan = good.embeddings.copy()
    nan[0, 0] = np.nan
    with pytest.raises(RetrievalIndexError, match="NaN"):
        RetrievalIndex(good.post_ids, good.author_ids, nan, "m", 1)
    with pytest.raises(RetrievalIndexError, match="model_version"):
        RetrievalIndex(good.post_ids, good.author_ids, good.embeddings, " ", 1)


def test_index_roundtrips_atomically(tmp_path):
    index = make_index([5, 6, 7], model_version="retrieval_params_step200.npz")
    path = tmp_path / "nested" / "index.npz"
    assert index.save(path) == path
    assert [p.name for p in path.parent.iterdir()] == ["index.npz"], "临时文件必须被 rename 掉"

    loaded = RetrievalIndex.load(path)
    assert loaded.post_ids == index.post_ids
    assert loaded.author_ids == index.author_ids
    assert loaded.model_version == index.model_version
    assert loaded.built_at_ms == index.built_at_ms
    np.testing.assert_array_equal(loaded.embeddings, index.embeddings)
    assert loaded.embeddings.dtype == np.float32


def test_index_load_rejects_missing_fields_and_unknown_schema(tmp_path):
    path = tmp_path / "bad.npz"
    np.savez(path, post_ids=np.asarray(["a"]))
    with pytest.raises(RetrievalIndexError, match="missing fields"):
        RetrievalIndex.load(path)

    index = make_index([1])
    index.save(path)
    with np.load(path) as data:
        fields = {name: data[name] for name in data.files}
    fields["schema_version"] = np.int64(INDEX_SCHEMA_VERSION + 1)
    np.savez(path, **fields)
    with pytest.raises(RetrievalIndexError, match="schema_version"):
        RetrievalIndex.load(path)

    with pytest.raises(RetrievalIndexError, match="cannot read"):
        RetrievalIndex.load(tmp_path / "does-not-exist.npz")


# ==================== 2. 网关加载与热替换 ====================


@pytest.fixture(scope="module")
def tables():
    from services.grpc_gateway import EmbeddingTables

    return EmbeddingTables(None)


@pytest.fixture(scope="module")
def encoder(tables):
    from services.grpc_gateway import RetrievalEngine

    # corpus_size=0：只当编码器用，不合成演示候选池。
    return RetrievalEngine(tables, None, corpus_size=0)


def encode_index(encoder, ids, model_version=None, built_at_ms=1_700_000_000_000):
    post_ids = [oid(i) for i in ids]
    author_ids = [oid(1000 + i) for i in ids]
    return RetrievalIndex(
        post_ids=tuple(post_ids),
        author_ids=tuple(author_ids),
        embeddings=encoder.encode_posts(post_ids, author_ids),
        model_version=model_version or encoder.model_version,
        built_at_ms=built_at_ms,
    )


def bump_mtime(path: Path) -> None:
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))


def test_empty_engine_returns_nothing_instead_of_demo_ids(encoder):
    assert encoder.corpus_size == 0
    assert encoder.retrieve("5506dd82fbe78e7de77976ca", None, 10) == []


def test_encode_posts_is_deterministic_and_normalized(encoder):
    post_ids = [oid(1), oid(2), oid(3)]
    author_ids = [oid(11), oid(12), oid(13)]
    first = encoder.encode_posts(post_ids, author_ids)
    second = encoder.encode_posts(post_ids, author_ids)
    assert first.shape == (3, 128)
    assert first.dtype == np.float32
    np.testing.assert_allclose(first, second)
    np.testing.assert_allclose(np.linalg.norm(first, axis=1), 1.0, atol=1e-3)
    # 同一帖子无论落在哪个分块位置都得到同一向量（分块编码不能泄漏位置）。
    many = encoder.encode_posts([oid(i) for i in range(40)], [oid(100 + i) for i in range(40)])
    alone = encoder.encode_posts([oid(39)], [oid(139)])
    np.testing.assert_allclose(many[39], alone[0], atol=1e-5)


def test_engine_serves_real_ids_from_index_and_hot_swaps_on_mtime(tmp_path, tables, encoder):
    from services.grpc_gateway import CorpusRefresher, RetrievalEngine

    path = tmp_path / "retrieval_index.npz"
    encode_index(encoder, [1, 2, 3, 4, 5]).save(path)

    engine = RetrievalEngine(tables, None, 2000, str(path))
    assert engine.corpus_size == 5, "提供索引时不得再合成演示候选池"
    assert engine.corpus_version == f"{engine.model_version}@1700000000000:5"

    results = engine.retrieve("5506dd82fbe78e7de77976ca", None, 100)
    assert len(results) == 5, "top_k 受候选池大小限制"
    assert {post for post, _, _ in results} == {oid(i) for i in [1, 2, 3, 4, 5]}
    for post_id, author_id, score in results:
        assert author_id == oid(1000 + int(post_id, 16)), "作者必须与帖子一一对应"
        assert np.isfinite(score)
    assert [s for _, _, s in results] == sorted((s for _, _, s in results), reverse=True)

    # 文件未变：不重载。
    assert engine.refresh_from_path() is False

    # 索引被离线任务重建：mtime 变化触发热替换，旧 ID 全部消失。
    encode_index(encoder, [7, 8, 9], built_at_ms=1_700_000_100_000).save(path)
    bump_mtime(path)
    refresher = CorpusRefresher(engine, interval_seconds=60)
    assert refresher.refresh_once() is True
    assert engine.corpus_size == 3
    assert engine.corpus_version.endswith("@1700000100000:3")
    assert {post for post, _, _ in engine.retrieve("u", None, 10)} == {oid(7), oid(8), oid(9)}

    # 错误模型版本的索引被拒绝，旧候选池保留，刷新线程不会因此退出。
    encode_index(encoder, [10], model_version="some-other-checkpoint").save(path)
    bump_mtime(path)
    with pytest.raises(RetrievalIndexError, match="checkpoint"):
        engine.refresh_from_path()
    assert refresher.refresh_once() is False
    assert engine.corpus_size == 3

    # 半个文件 / 损坏文件同样保留旧池。
    path.write_bytes(b"not an npz")
    bump_mtime(path)
    assert refresher.refresh_once() is False
    assert engine.corpus_size == 3


def test_engine_startup_fails_on_bad_index_instead_of_serving_empty(tmp_path, tables, encoder):
    from services.grpc_gateway import RetrievalEngine

    missing = tmp_path / "missing.npz"
    with pytest.raises(RetrievalIndexError):
        RetrievalEngine(tables, None, 2000, str(missing))

    mismatched = tmp_path / "mismatched.npz"
    encode_index(encoder, [1], model_version="trained-elsewhere").save(mismatched)
    with pytest.raises(RetrievalIndexError, match="checkpoint"):
        RetrievalEngine(tables, None, 2000, str(mismatched))


def test_retrieval_metadata_reports_corpus_version():
    pytest.importorskip("grpc_tools", reason="需要 grpcio-tools 生成 proto 桩代码")
    from services.grpc_gateway import create_servicers
    from services.recsys_proto import load_proto_modules

    recsys_pb2, recsys_pb2_grpc = load_proto_modules()

    class _Retrieval:
        model_version = "unit"
        corpus_version = "unit@1:3"

        def retrieve(self, user_id, uas, max_results):
            return [("69def6d4f0c8754f5c2fc994", "602e867f0de2d061ee418407", 0.5)]

    class _Context:
        def set_trailing_metadata(self, metadata):
            self.metadata = dict(metadata)

    context = _Context()
    _, servicer = create_servicers(recsys_pb2, recsys_pb2_grpc, None, _Retrieval())
    response = servicer.Retrieve(recsys_pb2.RetrieveRequest(user_id="u", max_results=5), context)
    assert context.metadata["corpus-version"] == "unit@1:3"
    assert context.metadata["model-version"] == "unit"
    returned = response.top_k_candidates[0].candidates[0].candidate
    assert returned.tweet_id == "69def6d4f0c8754f5c2fc994"


# ==================== 3. 离线构建脚本的输入校验 ====================


def test_builder_keeps_only_hydratable_fresh_unique_posts():
    now = 1_700_000_000_000
    hour = 3_600_000
    df = pd.DataFrame(
        {
            "post_id": [oid(1), oid(1), "bad", oid(3), oid(4), oid(5)],
            "author_id": [oid(11), oid(11), oid(12), "0" * 24, oid(14), oid(15)],
            "created_at_ms": [now - hour, now - hour, now, now, now - 49 * hour, None],
        }
    )
    post_ids, author_ids, dropped = builder.select_posts(df, max_age_hours=48, reference_ms=now)
    assert post_ids == [oid(1), oid(5)], "重复、非法、过期的都要丢；缺发布时间的保留"
    assert author_ids == [oid(11), oid(15)]
    assert dropped == {
        "invalid_post_id": 1,
        "invalid_author_id": 1,
        "duplicate": 1,
        "too_old": 1,
    }

    # 不过滤帖龄时过期帖保留。
    post_ids, _, dropped = builder.select_posts(df, max_age_hours=0, reference_ms=now)
    assert oid(4) in post_ids and dropped["too_old"] == 0

    with pytest.raises(SystemExit):
        builder.select_posts(pd.DataFrame({"post_id": [oid(1)]}), 48, now)
