"""Tests for the Kalshi auto-resolver."""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from kalshi_weather.signals.models import Signal
from kalshi_weather.signals.resolver import _parse_outcome, resolve_pending_signals
from kalshi_weather.signals.tracker import SignalTracker


def _make_signal(market_id="KXHIGHNY-26MAR07-B55", direction="YES", model_prob=0.70, market_prob=0.50):
    edge = model_prob - market_prob
    return Signal(
        market_id=market_id,
        question=f"Question for {market_id}",
        market_type="temperature",
        location="New York, NY",
        model_prob=model_prob,
        market_prob=market_prob,
        edge=edge,
        kelly_fraction=0.05,
        confidence=0.80,
        direction=direction,
        lead_time_hours=24.0,
        sources=["ECMWF"],
        details="test",
        timestamp=datetime.now(timezone.utc),
    )


@pytest.fixture
def tracker():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        with patch("kalshi_weather.signals.tracker.get_settings") as mock_settings:
            settings = mock_settings.return_value
            settings.db_path = db_path
            settings.database_url = ""
            t = SignalTracker()
            yield t


class TestParseOutcome:
    def test_yes_won_settled(self):
        """Settled market with result='yes' -> outcome 1."""
        raw = {"status": "settled", "result": "yes"}
        assert _parse_outcome(raw) == 1

    def test_no_won_settled(self):
        """Settled market with result='no' -> outcome 0."""
        raw = {"status": "settled", "result": "no"}
        assert _parse_outcome(raw) == 0

    def test_determined_yes(self):
        """Determined market with result='yes' -> outcome 1."""
        raw = {"status": "determined", "result": "yes"}
        assert _parse_outcome(raw) == 1

    def test_determined_no(self):
        """Determined market with result='no' -> outcome 0."""
        raw = {"status": "determined", "result": "no"}
        assert _parse_outcome(raw) == 0

    def test_settlement_value_100(self):
        """Fallback: settlement_value 100 -> outcome 1."""
        raw = {"status": "settled", "settlement_value": 100}
        assert _parse_outcome(raw) == 1

    def test_settlement_value_0(self):
        """Fallback: settlement_value 0 -> outcome 0."""
        raw = {"status": "settled", "settlement_value": 0}
        assert _parse_outcome(raw) == 0

    def test_open_market(self):
        """Open market should return None."""
        raw = {"status": "open"}
        assert _parse_outcome(raw) is None

    def test_active_market(self):
        """Active market should return None."""
        raw = {"status": "active"}
        assert _parse_outcome(raw) is None

    def test_empty_status(self):
        """Empty status should return None."""
        raw = {"status": ""}
        assert _parse_outcome(raw) is None

    def test_missing_status(self):
        """Missing status should return None."""
        raw = {}
        assert _parse_outcome(raw) is None

    def test_settled_no_result(self):
        """Settled but no result or settlement_value -> None."""
        raw = {"status": "settled"}
        assert _parse_outcome(raw) is None

    def test_settled_ambiguous_settlement_value(self):
        """Settled with mid-range settlement_value -> None."""
        raw = {"status": "settled", "settlement_value": 50}
        assert _parse_outcome(raw) is None


@pytest.mark.asyncio
async def test_resolve_yes_won(tracker):
    """Settled YES market: direction YES is correct."""
    await tracker.log_signal(_make_signal("KXHIGHNY-26MAR07-B55", direction="YES"))

    settled_market = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "status": "settled",
        "result": "yes",
        "settlement_timer_expiration_time": "2026-03-08T00:00:00Z",
    }

    with patch("kalshi_weather.signals.resolver.SignalTracker", return_value=tracker), \
         patch("kalshi_weather.signals.resolver.fetch_market_by_ticker", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.signals.resolver.get_settings") as mock_settings:
        mock_settings.return_value.telegram_enabled = False
        mock_fetch.return_value = settled_market

        results = await resolve_pending_signals()

    assert len(results) == 1
    assert results[0]["outcome"] == 1
    assert results[0]["direction"] == "YES"
    assert results[0]["correct"] is True


@pytest.mark.asyncio
async def test_resolve_no_won(tracker):
    """Settled NO market: direction NO is correct."""
    await tracker.log_signal(_make_signal("KXHIGHNY-26MAR07-B55", direction="NO", model_prob=0.30))

    settled_market = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "status": "settled",
        "result": "no",
        "settlement_timer_expiration_time": "2026-03-08T00:00:00Z",
    }

    with patch("kalshi_weather.signals.resolver.SignalTracker", return_value=tracker), \
         patch("kalshi_weather.signals.resolver.fetch_market_by_ticker", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.signals.resolver.get_settings") as mock_settings:
        mock_settings.return_value.telegram_enabled = False
        mock_fetch.return_value = settled_market

        results = await resolve_pending_signals()

    assert len(results) == 1
    assert results[0]["outcome"] == 0
    assert results[0]["direction"] == "NO"
    assert results[0]["correct"] is True


@pytest.mark.asyncio
async def test_resolve_still_open(tracker):
    """Open market should not be resolved."""
    await tracker.log_signal(_make_signal("KXHIGHNY-26MAR07-B55"))

    open_market = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "status": "open",
        "yes_price": 55,
        "no_price": 45,
    }

    with patch("kalshi_weather.signals.resolver.SignalTracker", return_value=tracker), \
         patch("kalshi_weather.signals.resolver.fetch_market_by_ticker", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.signals.resolver.get_settings") as mock_settings:
        mock_settings.return_value.telegram_enabled = False
        mock_fetch.return_value = open_market

        results = await resolve_pending_signals()

    assert len(results) == 0


@pytest.mark.asyncio
async def test_resolve_no_result_yet(tracker):
    """Settled market without result field should be skipped."""
    await tracker.log_signal(_make_signal("KXHIGHNY-26MAR07-B55"))

    ambiguous_market = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "status": "settled",
        # No result field, no settlement_value
    }

    with patch("kalshi_weather.signals.resolver.SignalTracker", return_value=tracker), \
         patch("kalshi_weather.signals.resolver.fetch_market_by_ticker", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.signals.resolver.get_settings") as mock_settings:
        mock_settings.return_value.telegram_enabled = False
        mock_fetch.return_value = ambiguous_market

        results = await resolve_pending_signals()

    assert len(results) == 0


@pytest.mark.asyncio
async def test_resolve_no_pending(tracker):
    """No unresolved signals should mean no API calls."""
    with patch("kalshi_weather.signals.resolver.SignalTracker", return_value=tracker), \
         patch("kalshi_weather.signals.resolver.fetch_market_by_ticker", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.signals.resolver.get_settings") as mock_settings:
        mock_settings.return_value.telegram_enabled = False

        results = await resolve_pending_signals()

    assert len(results) == 0
    mock_fetch.assert_not_called()
