from datetime import UTC, datetime

import pytest

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy import _validate_analysis_for_request


def _request() -> ReviewRequest:
    now = datetime.now(UTC)
    return ReviewRequest(
        request_id="request-secret-filter",
        requested_at=now,
        reason="test",
        symbols=[],
        context={},
    )


def _analysis(request: ReviewRequest, *, summary: str, sources: list[str]) -> MarketAnalysis:
    return MarketAnalysis(
        schema_version=1,
        analysis_id="analysis-secret-filter",
        review_request_id=request.request_id,
        generated_at=datetime.now(UTC),
        market_regime="test",
        summary=summary,
        sources=sources,
        watch=[],
        trade_proposals=[],
        next_review=None,
    )


def test_strategy_secret_scan_allows_risk_hyphen_text_and_urls() -> None:
    request = _request()
    analysis = _analysis(
        request,
        summary="Macro risk-market-regime-deterioration remains the main concern.",
        sources=[
            "https://example.com/markets/risk-market-regime-deterioration-continues"
        ],
    )

    _validate_analysis_for_request(analysis, request)


def test_strategy_secret_scan_still_blocks_openai_style_key() -> None:
    request = _request()
    fake_key = "sk-" + "a" * 32
    analysis = _analysis(
        request,
        summary=f"Unexpected credential-like token {fake_key}",
        sources=["https://example.com/market"],
    )

    with pytest.raises(RuntimeError, match="credential-like"):
        _validate_analysis_for_request(analysis, request)
