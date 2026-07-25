from grox.contracts import WorkItem, WorkResult, WorkStatus


def test_work_item_json_round_trip():
    original = WorkItem(
        id="post-1",
        eligibilities=frozenset({"metadata"}),
        attributes={"text": "Hello", "language": "en"},
    )

    restored = WorkItem.from_dict(original.to_dict())

    assert restored == original


def test_work_result_json_round_trip():
    original = WorkResult(
        id="post-1",
        plan="metadata",
        status=WorkStatus.SUCCESS,
        outputs={"measure": {"length": 5}},
        task_statuses={},
        errors={},
        started_at="2026-07-22T00:00:00+00:00",
        finished_at="2026-07-22T00:00:01+00:00",
    )

    restored = WorkResult.from_dict(original.to_dict())

    assert restored == original
