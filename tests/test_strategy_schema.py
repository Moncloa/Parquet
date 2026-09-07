from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from parquet.models import MarketAnalysis


def _object_schemas(node: object) -> Iterator[dict[str, Any]]:
    if isinstance(node, dict):
        if node.get("type") == "object" and isinstance(node.get("properties"), dict):
            yield node
        for value in node.values():
            yield from _object_schemas(value)
    elif isinstance(node, list):
        for value in node:
            yield from _object_schemas(value)


def test_market_analysis_schema_is_strict_for_codex() -> None:
    schema = MarketAnalysis.model_json_schema()
    objects = list(_object_schemas(schema))

    assert objects
    for object_schema in objects:
        properties = object_schema["properties"]
        assert isinstance(properties, dict)
        assert object_schema.get("additionalProperties") is False
        assert object_schema.get("required") == list(properties)

    next_review = schema["properties"]["next_review"]
    assert isinstance(next_review, dict)
    assert "default" not in next_review
