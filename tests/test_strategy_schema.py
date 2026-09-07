from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from parquet.models import MarketAnalysis


def _schema_dicts(node: object) -> Iterator[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _schema_dicts(value)
    elif isinstance(node, list):
        for value in node:
            yield from _schema_dicts(value)


def _object_schemas(node: object) -> Iterator[dict[str, Any]]:
    for schema in _schema_dicts(node):
        if schema.get("type") == "object" and isinstance(schema.get("properties"), dict):
            yield schema


def test_market_analysis_schema_is_strict_for_codex() -> None:
    schema = MarketAnalysis.model_json_schema()
    objects = list(_object_schemas(schema))

    assert objects
    for object_schema in objects:
        properties = object_schema["properties"]
        assert isinstance(properties, dict)
        assert object_schema.get("additionalProperties") is False
        assert object_schema.get("required") == list(properties)

    # Structured Outputs rejects defaults, including non-null Pydantic defaults
    # such as WatchItem.on_trigger = REASSESS.
    for node in _schema_dicts(schema):
        assert "default" not in node
        if "$ref" in node:
            assert set(node) == {"$ref"}

    watch_item = schema["$defs"]["WatchItem"]
    assert isinstance(watch_item, dict)
    on_trigger = watch_item["properties"]["on_trigger"]
    assert on_trigger == {"$ref": "#/$defs/TriggerAction"}
