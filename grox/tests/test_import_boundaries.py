from pathlib import Path


def test_runtime_has_no_private_or_recommendation_layer_imports():
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (Path(__file__).parents[1] / "src").rglob("*.py")
    )

    forbidden = [
        "grok_sampler",
        "strato_http",
        "kafka_cli",
        "thrifts.",
        "monitor.",
        "home_mixer",
        "phoenix",
        "grox.prompts",
    ]
    assert all(name not in source for name in forbidden)
