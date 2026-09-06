from datetime import UTC, datetime

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
