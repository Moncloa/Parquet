from datetime import UTC, datetime

from parquet.models import ReviewRequest
from parquet.strategy_policy import opportunity_strategy_prompt


def _request() -> ReviewRequest:
    now = datetime.now(UTC)
    return ReviewRequest(
        request_id="policy-test",
        requested_at=now,
        reason="validation",
        symbols=["ANDE", "BBWI", "GOLD"],
        context={
            "market_data": {
                "quotes": {
                    "ANDE": {"bid": 70.34, "ask": 71.03, "stale": False},
                    "BBWI": {"bid": 18.58, "ask": 18.59, "stale": False},
                    "GOLD": {"bid": 4422.72, "ask": 4422.92, "stale": False},
                },
                "history": {
                    "ANDE": {"metrics": {"change_pct_5m": -0.70}},
                    "BBWI": {"metrics": {"change_pct_15m": 1.2}},
                },
            }
        },
    )


def test_policy_seeks_opportunities_without_forcing_trades() -> None:
    prompt = opportunity_strategy_prompt(_request())

    assert "Do not start from NO TRADE" in prompt
    assert "NO TRADE is a valid outcome" in prompt
    assert "Compare at least the best three viable candidates" in prompt
    assert "price levels MAY and SHOULD be derived" in prompt
    assert "does NOT need to have traded previously" in prompt
    assert "nearby macro release is NOT a blanket veto" in prompt
    assert "NO TRADE does not require an empty watch list" in prompt
    assert "Never propose or watch Airbus / AIR.PA" in prompt
    assert '"request_id": "policy-test"' in prompt
