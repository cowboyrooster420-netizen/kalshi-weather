"""Tests for the full pipeline with mocked dependencies."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from kalshi_weather.markets.models import Comparison, MarketParams, MarketType, WeatherMarket
from kalshi_weather.pipeline import (
    fetch_weather_data,
    generate_signals,
    run_forecasts,
    run_pipeline,
    scan_markets,
)
from kalshi_weather.weather.models import EnsembleForecast


@pytest.fixture
def mock_raw_markets(kalshi_api_response):
    return kalshi_api_response


@pytest.fixture
def mock_weather_markets(now):
    """Pre-built weather markets with params and lat/lon."""
    return [
        WeatherMarket(
            market_id="KXHIGHPHX-26MAR08-B100",
            event_ticker="KXHIGHPHX-26MAR08",
            series_ticker="KXHIGHPHX",
            question="High temp in Phoenix above 100F on Mar 8?",
            description="Phoenix high temperature market",
            outcome_yes_price=0.35,
            outcome_no_price=0.65,
            params=MarketParams(
                market_type=MarketType.TEMPERATURE,
                location="Phoenix, AZ",
                lat_lon=(33.45, -112.07),
                threshold=100.0,
                comparison=Comparison.ABOVE,
                unit="F",
                target_date=now + timedelta(hours=24),
                daily_aggregation="max",
            ),
        ),
        WeatherMarket(
            market_id="KXHIGHHOU-26MAR08-B70",
            event_ticker="KXHIGHHOU-26MAR08",
            series_ticker="KXHIGHHOU",
            question="High temp in Houston above 70F on Mar 8?",
            description="Houston high temperature market",
            outcome_yes_price=0.40,
            outcome_no_price=0.60,
            params=MarketParams(
                market_type=MarketType.TEMPERATURE,
                location="Houston, TX",
                lat_lon=(29.76, -95.37),
                threshold=70.0,
                comparison=Comparison.ABOVE,
                unit="F",
                target_date=now + timedelta(hours=48),
                daily_aggregation="max",
            ),
        ),
    ]


def _make_ensemble(lat, lon, source, n_members, now):
    """Create a synthetic ensemble for testing."""
    np.random.seed(42 if source == "ecmwf" else 43)
    times = [now + timedelta(hours=i) for i in range(48)]
    n = len(times)
    return EnsembleForecast(
        source=source,
        lat=lat,
        lon=lon,
        times=times,
        temperature_2m=np.random.normal(35, 3, (n, n_members)),
        precipitation=np.maximum(0, np.random.exponential(2, (n, n_members)) - 1),
    )


@pytest.mark.asyncio
async def test_scan_markets_parses_kalshi_tickers(kalshi_api_response):
    """scan_markets should fetch Kalshi weather markets and parse tickers."""

    with patch("kalshi_weather.pipeline.fetch_weather_markets", new_callable=AsyncMock) as mock_fetch, \
         patch("kalshi_weather.pipeline.parse_kalshi_market", new_callable=AsyncMock) as mock_parse:

        mock_fetch.return_value = kalshi_api_response

        mock_parse.return_value = MarketParams(
            market_type=MarketType.TEMPERATURE,
            location="New York, NY",
            daily_aggregation="max",
        )

        markets = await scan_markets()

    assert len(markets) == 4  # All 4 Kalshi weather markets
    # Each should have been parsed
    assert mock_parse.call_count == 4


@pytest.mark.asyncio
async def test_fetch_weather_data_groups_by_location(mock_weather_markets, now):
    """Weather data should be fetched once per unique location."""
    fetch_count = 0

    async def mock_fetch_both(lat, lon):
        nonlocal fetch_count
        fetch_count += 1
        ecmwf = _make_ensemble(lat, lon, "ecmwf", 51, now)
        gfs = _make_ensemble(lat, lon, "gfs", 31, now)
        return gfs, ecmwf

    async def mock_fetch_noaa(lat, lon):
        return None

    with patch("kalshi_weather.pipeline.fetch_both_ensembles", side_effect=mock_fetch_both), \
         patch("kalshi_weather.pipeline.fetch_noaa_forecast", side_effect=mock_fetch_noaa), \
         patch("kalshi_weather.pipeline.fetch_hrrr", new_callable=AsyncMock, return_value=None):
        weather_data = await fetch_weather_data(mock_weather_markets)

    # Two different locations -> two fetches
    assert fetch_count == 2
    assert len(weather_data) == 2


@pytest.mark.asyncio
async def test_fetch_weather_data_deduplicates(now):
    """Markets at the same location should share one fetch."""
    markets = [
        WeatherMarket(
            market_id=f"KXHIGHPHX-26MAR08-B{50 + i}",
            event_ticker="KXHIGHPHX-26MAR08",
            series_ticker="KXHIGHPHX",
            question=f"High temp in Phoenix above {50 + i}F?",
            description="",
            outcome_yes_price=0.5,
            outcome_no_price=0.5,
            params=MarketParams(
                market_type=MarketType.TEMPERATURE,
                location="Phoenix, AZ",
                lat_lon=(33.45, -112.07),
                threshold=float(50 + i),
            ),
        )
        for i in range(5)
    ]

    fetch_count = 0

    async def mock_fetch_both(lat, lon):
        nonlocal fetch_count
        fetch_count += 1
        ecmwf = _make_ensemble(lat, lon, "ecmwf", 51, now)
        gfs = _make_ensemble(lat, lon, "gfs", 31, now)
        return gfs, ecmwf

    async def mock_fetch_noaa(lat, lon):
        return None

    with patch("kalshi_weather.pipeline.fetch_both_ensembles", side_effect=mock_fetch_both), \
         patch("kalshi_weather.pipeline.fetch_noaa_forecast", side_effect=mock_fetch_noaa), \
         patch("kalshi_weather.pipeline.fetch_hrrr", new_callable=AsyncMock, return_value=None):
        weather_data = await fetch_weather_data(markets)

    # All 5 markets share one location -> only one fetch
    assert fetch_count == 1
    assert len(weather_data) == 1


@pytest.mark.asyncio
async def test_fetch_weather_data_handles_errors(mock_weather_markets, now):
    """Fetch errors should be caught, not crash pipeline."""

    async def mock_fetch_both(lat, lon):
        raise Exception("Open-Meteo down")

    async def mock_fetch_noaa(lat, lon):
        return None

    with patch("kalshi_weather.pipeline.fetch_both_ensembles", side_effect=mock_fetch_both), \
         patch("kalshi_weather.pipeline.fetch_noaa_forecast", side_effect=mock_fetch_noaa), \
         patch("kalshi_weather.pipeline.fetch_hrrr", new_callable=AsyncMock, return_value=None):
        weather_data = await fetch_weather_data(mock_weather_markets)

    # Should still have entries (with None values), not crash
    assert len(weather_data) == 2
    for key, val in weather_data.items():
        assert val == (None, None, None, None)


@pytest.mark.asyncio
async def test_run_forecasts(mock_weather_markets, now):
    """Forecast models should produce estimates for each market."""
    lat, lon = 33.45, -112.07
    key = (round(lat, 2), round(lon, 2))
    ecmwf = _make_ensemble(lat, lon, "ecmwf", 51, now)
    gfs = _make_ensemble(lat, lon, "gfs", 31, now)

    lat2, lon2 = 29.76, -95.37
    key2 = (round(lat2, 2), round(lon2, 2))
    ecmwf2 = _make_ensemble(lat2, lon2, "ecmwf", 51, now)
    gfs2 = _make_ensemble(lat2, lon2, "gfs", 31, now)

    weather_data = {
        key: (gfs, ecmwf, None, None),
        key2: (gfs2, ecmwf2, None, None),
    }

    with patch("kalshi_weather.pipeline.get_settings") as mock_settings:
        mock_settings.return_value.enabled_market_types = ["temperature", "precipitation", "hurricane"]
        results = await run_forecasts(mock_weather_markets, weather_data)

    assert len(results) == 2
    for market, estimate in results:
        assert 0 < estimate.probability < 1
        assert estimate.confidence > 0


@pytest.mark.asyncio
async def test_generate_signals_filters_by_edge(now):
    """Only signals with sufficient edge should be generated."""
    from kalshi_weather.forecasting.base import ProbabilityEstimate

    market1 = WeatherMarket(
        market_id="KXHIGHNY-26MAR07-B55",
        event_ticker="KXHIGHNY-26MAR07",
        series_ticker="KXHIGHNY",
        question="High temp in NYC above 55F?",
        description="",
        outcome_yes_price=0.50, outcome_no_price=0.50,
        params=MarketParams(market_type=MarketType.TEMPERATURE, location="New York, NY"),
    )
    market2 = WeatherMarket(
        market_id="KXHIGHATL-26MAR07-B70",
        event_ticker="KXHIGHATL-26MAR07",
        series_ticker="KXHIGHATL",
        question="High temp in Atlanta above 70F?",
        description="",
        outcome_yes_price=0.70, outcome_no_price=0.30,
        params=MarketParams(market_type=MarketType.TEMPERATURE, location="Atlanta, GA"),
    )

    # 2% edge -- below threshold
    est1 = ProbabilityEstimate(
        probability=0.52, raw_probability=0.52,
        confidence=0.8, lead_time_hours=24,
    )
    # Large NO edge -- above threshold, model_prob below max_model_prob
    est2 = ProbabilityEstimate(
        probability=0.10, raw_probability=0.10,
        confidence=0.8, lead_time_hours=24,
    )

    results = [(market1, est1), (market2, est2)]

    with patch("kalshi_weather.pipeline.get_settings") as mock_pipe_settings, \
         patch("kalshi_weather.signals.analyzer.get_settings") as mock_ana_settings:
        mock_pipe_settings.return_value.first_signal_only = False
        mock_ana_settings.return_value.no_only = True
        mock_ana_settings.return_value.min_edge = 0.10
        mock_ana_settings.return_value.min_confidence = 0.30
        mock_ana_settings.return_value.min_lead_time_hours = 12.0
        mock_ana_settings.return_value.min_market_prob = 0.30
        mock_ana_settings.return_value.first_signal_only = False
        mock_ana_settings.return_value.kelly_fraction = 0.25
        mock_ana_settings.return_value.max_model_prob = 0.50
        mock_ana_settings.return_value.min_kelly_bet = 0.0
        signals = await generate_signals(results)

    assert len(signals) == 1
    assert signals[0].market_id == "KXHIGHATL-26MAR07-B70"


@pytest.mark.asyncio
async def test_run_pipeline_end_to_end(kalshi_api_response, now):
    """Full pipeline end-to-end with all dependencies mocked."""

    async def mock_parse(raw):
        return MarketParams(
            market_type=MarketType.TEMPERATURE,
            location="Phoenix, AZ",
            lat_lon=(33.45, -112.07),
            threshold=100.0,
            comparison=Comparison.ABOVE,
            unit="F",
            target_date=now + timedelta(hours=24),
            daily_aggregation="max",
        )

    ecmwf = _make_ensemble(33.45, -112.07, "ecmwf", 51, now)
    gfs = _make_ensemble(33.45, -112.07, "gfs", 31, now)

    with patch("kalshi_weather.pipeline.fetch_weather_markets", new_callable=AsyncMock) as mock_fetch_markets, \
         patch("kalshi_weather.pipeline.parse_kalshi_market", side_effect=mock_parse), \
         patch("kalshi_weather.pipeline.fetch_both_ensembles", new_callable=AsyncMock) as mock_fetch_ens, \
         patch("kalshi_weather.pipeline.fetch_noaa_forecast", new_callable=AsyncMock) as mock_fetch_noaa, \
         patch("kalshi_weather.pipeline.fetch_hrrr", new_callable=AsyncMock) as mock_fetch_hrrr, \
         patch("kalshi_weather.signals.tracker.SignalTracker.log_signals", new_callable=AsyncMock) as mock_log, \
         patch("kalshi_weather.signals.resolver.resolve_pending_signals", new_callable=AsyncMock) as mock_resolve:

        mock_fetch_markets.return_value = kalshi_api_response
        mock_fetch_ens.return_value = (gfs, ecmwf)
        mock_fetch_noaa.return_value = None
        mock_fetch_hrrr.return_value = None
        mock_log.return_value = [1, 2, 3]
        mock_resolve.return_value = []

        signals = await run_pipeline()

    # Should have found weather markets and produced some signals (or not, depending on edge)
    assert isinstance(signals, list)
    # All signals should have valid fields
    for s in signals:
        assert 0 <= s.model_prob <= 1
        assert 0 <= s.market_prob <= 1
        assert s.direction in ("YES", "NO")
        assert s.kelly_fraction >= 0
