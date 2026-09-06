from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from parquet.models import Side, TradeProposal


def test_buy_requires_stop_below_entry() -> None:
    with pytest.raises(ValidationError):
        TradeProposal(symbol="NSDQ100", side=Side.BUY, entry=100, stop_loss=101, confidence=0.8, expires_at=datetime.now(timezone.utc) + timedelta(minutes=5))
