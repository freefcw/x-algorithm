from __future__ import annotations

import json
from pathlib import Path

from services.model_contract import object_id_to_u64_hash

GOLDEN = Path(__file__).resolve().parents[2] / "testdata" / "object_id_u64_hash.json"


def test_object_id_to_u64_hash_matches_shared_golden_file() -> None:
    rows = json.loads(GOLDEN.read_text())
    assert rows
    for row in rows:
        assert object_id_to_u64_hash(row["object_id"]) == row["to_u64_hash"]


def test_object_id_to_u64_hash_rejects_uppercase_and_short_ids() -> None:
    try:
        object_id_to_u64_hash("E305C05A62CD1EF55823CD86")
        raise AssertionError("uppercase must be rejected")
    except ValueError:
        pass

    for malformed in (
        "e305c05a62cd1ef55823cd8 ",  # trailing whitespace
        "e305c05a62cd1ef5 5823cd86",  # embedded separator
        "e305c05a62cd1ef55823cd8g",  # non-hex character
    ):
        try:
            object_id_to_u64_hash(malformed)
            raise AssertionError(f"malformed id must be rejected: {malformed!r}")
        except ValueError:
            pass
    try:
        object_id_to_u64_hash("abc")
        raise AssertionError("short id must be rejected")
    except ValueError:
        pass
