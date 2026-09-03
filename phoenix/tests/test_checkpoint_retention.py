"""`_should_keep` 决定 checkpoint 清理时哪些目录受保护，误判会删掉该留的 checkpoint。

df7fbaa 起判定改为直接读 metadata 的 checkpoint_index（不再用 step // checkpoint_every_n
推算），并新增 index 缺失时的保护分支，上游没有为这个函数留测试。
"""

import pytest

from xrex.driver import hooks
from xrex.utils.metadata import COMPLETED_FILENAME, write_checkpoint_metadata

RUN_ID = "run-0"


@pytest.fixture(autouse=True)
def _clear_should_keep_cache():
    hooks._should_keep_cache.clear()
    yield
    hooks._should_keep_cache.clear()


def _make_checkpoint(tmp_path, checkpoint_index):
    checkpoint_path = tmp_path / RUN_ID
    checkpoint_path.mkdir()
    write_checkpoint_metadata(
        checkpoint_path / "metadata.json",
        elapsed_samples=1024,
        checkpoint_index=checkpoint_index,
        checkpoint_expiry="",
        elapsed_tokens=None,
    )
    return checkpoint_path


def test_keep_every_n_disabled_keeps_nothing(tmp_path):
    _make_checkpoint(tmp_path, checkpoint_index=10)
    assert hooks._should_keep(str(tmp_path), RUN_ID, 0) is False
    assert hooks._should_keep(str(tmp_path), RUN_ID, -1) is False


def test_checkpoint_index_on_the_interval_is_kept(tmp_path):
    _make_checkpoint(tmp_path, checkpoint_index=10)
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is True


def test_checkpoint_index_off_the_interval_is_not_kept(tmp_path):
    _make_checkpoint(tmp_path, checkpoint_index=11)
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is False


def test_missing_metadata_is_kept(tmp_path):
    (tmp_path / RUN_ID).mkdir()
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is True


def test_completed_marker_without_index_is_kept(tmp_path):
    # 旧格式 checkpoint 只有 completed 文件，read_metadata_file 回退时 checkpoint_index 为 None。
    checkpoint_path = tmp_path / RUN_ID
    checkpoint_path.mkdir()
    (checkpoint_path / COMPLETED_FILENAME).write_text("1024")
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is True


def test_unknown_index_is_reevaluated_once_metadata_appears(tmp_path):
    checkpoint_path = tmp_path / RUN_ID
    checkpoint_path.mkdir()
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is True

    write_checkpoint_metadata(
        checkpoint_path / "metadata.json",
        elapsed_samples=1024,
        checkpoint_index=11,
        checkpoint_expiry="",
        elapsed_tokens=None,
    )
    assert hooks._should_keep(str(tmp_path), RUN_ID, 5) is False
