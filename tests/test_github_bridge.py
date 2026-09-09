from datetime import UTC, datetime

import httpx
import pytest

from parquet.bridge.github import ANALYSIS_MARKER, GitHubBridge


def test_parse_analysis_comment() -> None:
    now = datetime.now(UTC).isoformat()
    body = f'''{ANALYSIS_MARKER}\n```json
{{
  "schema_version": 1,
  "analysis_id": "a-1",
  "generated_at": "{now}",
  "market_regime": "risk_on",
  "watch": [],
  "trade_proposals": []
}}
```'''
    parsed = GitHubBridge.parse_analysis_comment(body)
    assert parsed is not None
    assert parsed.analysis_id == "a-1"


@pytest.mark.asyncio
async def test_comments_paginate_and_remember_last_runtime_page(tmp_path) -> None:
    token_file = tmp_path / "github_token"
    token_file.write_text("test-token", encoding="utf-8")
    comments = [{"id": value, "body": f"comment-{value}"} for value in range(1, 206)]
    requested_pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        per_page = int(request.url.params.get("per_page", "100"))
        requested_pages.append(page)
        start = (page - 1) * per_page
        return httpx.Response(200, json=comments[start : start + per_page])

    bridge = GitHubBridge(
        "Moncloa/Parquet",
        2,
        token_file,
        transport=httpx.MockTransport(handler),
    )

    first = await bridge.comments()
    assert [item["id"] for item in first] == list(range(1, 206))
    assert requested_pages == [1, 2, 3]

    comments.extend(
        [
            {"id": 206, "body": "comment-206"},
            {"id": 207, "body": "comment-207"},
        ]
    )
    requested_pages.clear()

    second = await bridge.comments()
    assert [item["id"] for item in second] == list(range(201, 208))
    assert requested_pages == [3]
