import json

from grox.demo import run_demo


def test_demo_writes_deterministic_metadata_without_model_outputs(tmp_path):
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "output.json"
    input_path.write_text(
        json.dumps(
            {
                "id": "post-1",
                "eligibilities": ["demo_metadata"],
                "attributes": {"text": "  Hello   world  "},
            }
        ),
        encoding="utf-8",
    )

    result = run_demo(input_path, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert result.status.value == "success"
    assert written["outputs"]["normalize"] == {"normalized_text": "Hello world"}
    assert written["outputs"]["measure"] == {"character_count": 11, "word_count": 2}
    serialized = json.dumps(written).lower()
    assert "safety_label" not in serialized
    assert "embedding" not in serialized
    assert "spam_verdict" not in serialized
