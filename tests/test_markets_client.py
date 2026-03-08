"""Tests for Kalshi REST API client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_weather.markets.client import fetch_weather_markets, raw_to_weather_market


@pytest.mark.asyncio
async def test_fetch_weather_markets(kalshi_api_response):
    """Test fetching weather markets from Kalshi API for configured series."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"markets": kalshi_api_response, "cursor": ""}
    mock_resp.raise_for_status = MagicMock()

    with patch("kalshi_weather.markets.client.HttpClient") as MockClient, \
         patch("kalshi_weather.markets.client.get_settings") as mock_settings:
        settings = mock_settings.return_value
        settings.kalshi_api_url = "https://api.elections.kalshi.com/trade-api/v2"
        settings.kalshi_weather_series = ["KXHIGHNY"]

        instance = AsyncMock()
        instance.get = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        markets = await fetch_weather_markets()

    assert len(markets) == 4
    assert markets[0]["ticker"] == "KXHIGHNY-26MAR07-B55"


@pytest.mark.asyncio
async def test_fetch_pagination():
    """Test that pagination stops when no cursor is returned."""
    page1_markets = [
        {"ticker": f"KXHIGHNY-26MAR07-T{i}", "series_ticker": "KXHIGHNY"}
        for i in range(200)
    ]
    page2_markets = [
        {"ticker": "KXHIGHNY-26MAR07-B99", "series_ticker": "KXHIGHNY"}
    ]

    mock_resp1 = MagicMock()
    mock_resp1.json.return_value = {"markets": page1_markets, "cursor": "page2cursor"}
    mock_resp2 = MagicMock()
    mock_resp2.json.return_value = {"markets": page2_markets, "cursor": ""}

    with patch("kalshi_weather.markets.client.HttpClient") as MockClient, \
         patch("kalshi_weather.markets.client.get_settings") as mock_settings:
        settings = mock_settings.return_value
        settings.kalshi_api_url = "https://api.elections.kalshi.com/trade-api/v2"
        settings.kalshi_weather_series = ["KXHIGHNY"]

        instance = AsyncMock()
        instance.get = AsyncMock(side_effect=[mock_resp1, mock_resp2])
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        markets = await fetch_weather_markets()

    assert len(markets) == 201


def test_raw_to_weather_market_with_prices():
    """Test conversion of raw Kalshi market data with cent prices."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "High temperature in NYC above 55F on Mar 7?",
        "subtitle": "New York City high temperature market",
        "yes_price": 65,
        "no_price": 35,
        "status": "open",
        "volume": 50000,
        "close_time": "2026-03-08T00:00:00Z",
    }

    market = raw_to_weather_market(raw)

    assert market.market_id == "KXHIGHNY-26MAR07-B55"
    assert market.event_ticker == "KXHIGHNY-26MAR07"
    assert market.series_ticker == "KXHIGHNY"
    assert market.question == "High temperature in NYC above 55F on Mar 7?"
    assert abs(market.outcome_yes_price - 0.65) < 0.001
    assert abs(market.outcome_no_price - 0.35) < 0.001
    assert market.volume == 50000.0
    assert market.end_date is not None
    assert abs(market.market_prob - 0.65) < 0.001


def test_raw_to_weather_market_no_prices():
    """Test conversion when no price data is available (defaults to 50 cents)."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "Market question",
        "subtitle": "",
    }

    market = raw_to_weather_market(raw)

    assert abs(market.outcome_yes_price - 0.50) < 0.001
    assert abs(market.outcome_no_price - 0.50) < 0.001


def test_raw_to_weather_market_normalized_prices():
    """Test that cent prices (>1) are normalized to 0-1."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "Market question",
        "subtitle": "",
        "yes_price": 75,
        "no_price": 25,
    }

    market = raw_to_weather_market(raw)

    assert abs(market.outcome_yes_price - 0.75) < 0.001
    assert abs(market.outcome_no_price - 0.25) < 0.001


def test_raw_to_weather_market_already_normalized():
    """Test that prices already in 0-1 range are not double-divided."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "Market question",
        "subtitle": "",
        "yes_price": 0.55,
        "no_price": 0.45,
    }

    market = raw_to_weather_market(raw)

    assert abs(market.outcome_yes_price - 0.55) < 0.001
    assert abs(market.outcome_no_price - 0.45) < 0.001


def test_raw_to_weather_market_status_active():
    """Test that status 'open' maps to active=True."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "Market question",
        "subtitle": "",
        "status": "open",
    }

    market = raw_to_weather_market(raw)
    assert market.active is True


def test_raw_to_weather_market_status_closed():
    """Test that status 'settled' maps to active=False."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "Market question",
        "subtitle": "",
        "status": "settled",
    }

    market = raw_to_weather_market(raw)
    assert market.active is False
