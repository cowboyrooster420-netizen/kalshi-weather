"""Tests for Kalshi weather market ticker parser.

Kalshi tickers encode structured data directly:
  Series:  KXHIGHNY      -> high temp, New York
  Event:   KXHIGHNY-26MAR07  -> high temp, New York, March 7 2026
  Market:  KXHIGHNY-26MAR07-B55  -> above 55F
           KXHIGHNY-26MAR07-T55  -> between 55-56F (bucket)
           KXHIGHNY-26MAR07-U55  -> under 55F
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from kalshi_weather.markets.models import Comparison, MarketType
from kalshi_weather.markets.parser import (
    _extract_series_prefix,
    _parse_date_component,
    _parse_threshold_component,
    parse_kalshi_market,
)


# --- Date component parsing tests ---


class TestParseDateComponent:
    def test_standard_date(self):
        dt = _parse_date_component("26MAR07")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 7

    def test_january(self):
        dt = _parse_date_component("26JAN15")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 15

    def test_december(self):
        dt = _parse_date_component("25DEC25")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 12
        assert dt.day == 25

    def test_february_leap_year(self):
        dt = _parse_date_component("28FEB29")
        assert dt is not None
        assert dt.year == 2028
        assert dt.month == 2
        assert dt.day == 29

    def test_invalid_month(self):
        dt = _parse_date_component("26XXX07")
        assert dt is None

    def test_invalid_format(self):
        dt = _parse_date_component("not-a-date")
        assert dt is None

    def test_empty_string(self):
        dt = _parse_date_component("")
        assert dt is None


# --- Threshold component parsing tests ---


class TestParseThresholdComponent:
    def test_above_positive(self):
        result = _parse_threshold_component("B55")
        assert result is not None
        comparison, threshold, upper = result
        assert comparison == Comparison.ABOVE
        assert threshold == 55.0
        assert upper is None

    def test_under_positive(self):
        result = _parse_threshold_component("U30")
        assert result is not None
        comparison, threshold, upper = result
        assert comparison == Comparison.BELOW
        assert threshold == 30.0
        assert upper is None

    def test_between_bucket(self):
        result = _parse_threshold_component("T45")
        assert result is not None
        comparison, threshold, upper = result
        assert comparison == Comparison.BETWEEN
        assert threshold == 45.0
        assert upper == 46.0

    def test_above_high_value(self):
        result = _parse_threshold_component("B100")
        assert result is not None
        comparison, threshold, _ = result
        assert comparison == Comparison.ABOVE
        assert threshold == 100.0

    def test_negative_threshold(self):
        result = _parse_threshold_component("U-5")
        assert result is not None
        comparison, threshold, _ = result
        assert comparison == Comparison.BELOW
        assert threshold == -5.0

    def test_negative_between(self):
        result = _parse_threshold_component("T-10")
        assert result is not None
        comparison, threshold, upper = result
        assert comparison == Comparison.BETWEEN
        assert threshold == -10.0
        assert upper == -9.0

    def test_invalid_prefix(self):
        result = _parse_threshold_component("X55")
        assert result is None

    def test_no_number(self):
        result = _parse_threshold_component("B")
        assert result is None

    def test_empty_string(self):
        result = _parse_threshold_component("")
        assert result is None


# --- Series prefix extraction tests ---


class TestExtractSeriesPrefix:
    def test_kxhigh_ny(self):
        result = _extract_series_prefix("KXHIGHNY")
        assert result == ("KXHIGH", "NY")

    def test_kxlow_chi(self):
        result = _extract_series_prefix("KXLOWCHI")
        assert result == ("KXLOW", "CHI")

    def test_kxrain_hou(self):
        result = _extract_series_prefix("KXRAINHOU")
        assert result == ("KXRAIN", "HOU")

    def test_kxhigh_three_letter_city(self):
        result = _extract_series_prefix("KXHIGHATL")
        assert result == ("KXHIGH", "ATL")

    def test_unknown_prefix(self):
        result = _extract_series_prefix("PRES")
        assert result is None

    def test_prefix_only_no_city(self):
        result = _extract_series_prefix("KXHIGH")
        assert result is None


# --- Full ticker parsing tests ---


@pytest.mark.asyncio
async def test_parse_high_temp_above():
    """Parse KXHIGHNY-26MAR07-B55 -> high temp, NYC, above 55F."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "High temperature in NYC above 55F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (40.7128, -74.0060)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.TEMPERATURE
    assert params.daily_aggregation == "max"
    assert params.location == "New York, NY"
    assert params.comparison == Comparison.ABOVE
    assert params.threshold == 55.0
    assert params.unit == "F"
    assert params.target_date is not None
    assert params.target_date.year == 2026
    assert params.target_date.month == 3
    assert params.target_date.day == 7
    assert params.lat_lon == (40.7128, -74.0060)


@pytest.mark.asyncio
async def test_parse_high_temp_bucket():
    """Parse KXHIGHCHI-26MAR07-T40 -> high temp, Chicago, between 40-41F."""
    raw = {
        "ticker": "KXHIGHCHI-26MAR07-T40",
        "event_ticker": "KXHIGHCHI-26MAR07",
        "series_ticker": "KXHIGHCHI",
        "title": "High temperature in Chicago between 40-41F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (41.8781, -87.6298)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.TEMPERATURE
    assert params.daily_aggregation == "max"
    assert params.location == "Chicago, IL"
    assert params.comparison == Comparison.BETWEEN
    assert params.threshold == 40.0
    assert params.threshold_upper == 41.0


@pytest.mark.asyncio
async def test_parse_high_temp_under():
    """Parse KXHIGHATL-26MAR07-U50 -> high temp, Atlanta, under 50F."""
    raw = {
        "ticker": "KXHIGHATL-26MAR07-U50",
        "event_ticker": "KXHIGHATL-26MAR07",
        "series_ticker": "KXHIGHATL",
        "title": "High temperature in Atlanta under 50F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (33.749, -84.388)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.TEMPERATURE
    assert params.comparison == Comparison.BELOW
    assert params.threshold == 50.0
    assert params.location == "Atlanta, GA"


@pytest.mark.asyncio
async def test_parse_low_temp():
    """Parse KXLOWNY-26MAR07-U25 -> low temp, NYC, under 25F."""
    raw = {
        "ticker": "KXLOWNY-26MAR07-U25",
        "event_ticker": "KXLOWNY-26MAR07",
        "series_ticker": "KXLOWNY",
        "title": "Low temperature in NYC under 25F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (40.7128, -74.0060)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.TEMPERATURE
    assert params.daily_aggregation == "min"
    assert params.comparison == Comparison.BELOW
    assert params.threshold == 25.0


@pytest.mark.asyncio
async def test_parse_rain_market():
    """Parse KXRAINHOU-26MAR-B5 -> precipitation, Houston, above 5."""
    raw = {
        "ticker": "KXRAINHOU-26MAR-B5",
        "event_ticker": "KXRAINHOU-26MAR",
        "series_ticker": "KXRAINHOU",
        "title": "Rainfall in Houston above 5 inches in March?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (29.7604, -95.3698)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.PRECIPITATION
    assert params.location == "Houston, TX"
    assert params.comparison == Comparison.ABOVE
    assert params.threshold == 5.0


@pytest.mark.asyncio
async def test_parse_negative_threshold():
    """Parse KXLOWCHI-26JAN15-T-10 -> low temp, Chicago, between -10 and -9F."""
    raw = {
        "ticker": "KXLOWCHI-26JAN15-T-10",
        "event_ticker": "KXLOWCHI-26JAN15",
        "series_ticker": "KXLOWCHI",
        "title": "Low temperature in Chicago between -10 and -9F on Jan 15?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (41.8781, -87.6298)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.threshold == -10.0
    assert params.threshold_upper == -9.0
    assert params.comparison == Comparison.BETWEEN


@pytest.mark.asyncio
async def test_parse_geocoding_failure():
    """If geocoding fails, lat_lon should be None but params still returned."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07-B55",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "High temperature in NYC above 55F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = None
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.lat_lon is None
    assert params.location == "New York, NY"


@pytest.mark.asyncio
async def test_parse_unknown_city_code():
    """Unknown city code should use the raw code as location."""
    raw = {
        "ticker": "KXHIGHZZZ-26MAR07-B55",
        "event_ticker": "KXHIGHZZZ-26MAR07",
        "series_ticker": "KXHIGHZZZ",
        "title": "High temperature in ZZZ above 55F?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = None
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.location == "ZZZ"


@pytest.mark.asyncio
async def test_parse_invalid_ticker():
    """Completely invalid ticker should return None."""
    raw = {
        "ticker": "NOTAWEATHERTICKER",
        "event_ticker": "NOTAWEATHERTICKER",
        "series_ticker": "NOTAWEATHER",
        "title": "Not a weather market",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = None
        params = await parse_kalshi_market(raw)

    assert params is None


@pytest.mark.asyncio
async def test_parse_ticker_without_threshold():
    """Ticker with only series and date (no threshold part) should still parse."""
    raw = {
        "ticker": "KXHIGHNY-26MAR07",
        "event_ticker": "KXHIGHNY-26MAR07",
        "series_ticker": "KXHIGHNY",
        "title": "High temperature in NYC 60F on Mar 7?",
        "subtitle": "",
    }

    with patch("kalshi_weather.markets.parser.geocode", new_callable=AsyncMock) as mock_geo:
        mock_geo.return_value = (40.7128, -74.0060)
        params = await parse_kalshi_market(raw)

    assert params is not None
    assert params.market_type == MarketType.TEMPERATURE
    assert params.target_date is not None
    # Threshold may be None or extracted from title
