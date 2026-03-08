"""Tests for Weather Underground airport history page scraper."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_weather.weather.wunderground import (
    WUDailyObs,
    _parse_temp_text,
    fetch_airport_daily,
)
from kalshi_weather.weather.stations import Station


# ---------- _parse_temp_text ----------


def test_parse_temp_integer():
    assert _parse_temp_text("72") == 72.0


def test_parse_temp_with_degree():
    assert _parse_temp_text("72°") == 72.0


def test_parse_temp_negative():
    assert _parse_temp_text("-5") == -5.0


def test_parse_temp_decimal():
    assert _parse_temp_text("72.5") == 72.5


def test_parse_temp_with_unit():
    assert _parse_temp_text("72°F") == 72.0


def test_parse_temp_with_comma():
    assert _parse_temp_text("1,072") == 1072.0


def test_parse_temp_empty():
    assert _parse_temp_text("") is None


def test_parse_temp_na():
    assert _parse_temp_text("N/A") is None


def test_parse_temp_dashes():
    assert _parse_temp_text("--") is None


def test_parse_temp_whitespace():
    assert _parse_temp_text("  72  ") == 72.0


def test_parse_temp_negative_decimal():
    assert _parse_temp_text("-12.3°") == -12.3


# ---------- fetch_airport_daily ----------


_TEST_STATION = Station(
    icao="KATL", city="atlanta",
    lat_lon=(33.661, -84.399), timezone="America/New_York",
    history_path="us/ga/atlanta/KATL",
)


@pytest.mark.asyncio
async def test_fetch_airport_daily_success():
    """Returns WUDailyObs when page renders correctly."""
    mock_page = AsyncMock()

    # Simulate successful selector wait
    mock_page.wait_for_selector = AsyncMock()

    # Build mock DOM: one row with "Max Temp" and one with "Min Temp"
    max_row = AsyncMock()
    max_header = AsyncMock()
    max_header.inner_text = AsyncMock(return_value="Max Temperature")
    max_row.query_selector = AsyncMock(side_effect=lambda sel: {
        "td:first-child": max_header,
        "td:nth-child(2)": _mock_value_cell("85"),
    }.get(sel))

    min_row = AsyncMock()
    min_header = AsyncMock()
    min_header.inner_text = AsyncMock(return_value="Min Temperature")
    min_row.query_selector = AsyncMock(side_effect=lambda sel: {
        "td:first-child": min_header,
        "td:nth-child(2)": _mock_value_cell("62"),
    }.get(sel))

    mock_page.query_selector_all = AsyncMock(return_value=[max_row, min_row])

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    result = await fetch_airport_daily(_TEST_STATION, date(2026, 2, 15), mock_browser)

    assert result is not None
    assert result.station_id == "KATL"
    assert result.date == date(2026, 2, 15)
    # 85°F ≈ 29.44°C, 62°F ≈ 16.67°C
    assert 29.0 < result.high_temp_c < 30.0
    assert 16.0 < result.low_temp_c < 17.0


@pytest.mark.asyncio
async def test_fetch_airport_daily_timeout():
    """Returns None when the history table never renders."""
    mock_page = AsyncMock()
    mock_page.wait_for_selector = AsyncMock(side_effect=Exception("Timeout"))

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    result = await fetch_airport_daily(_TEST_STATION, date(2026, 2, 15), mock_browser)

    assert result is None


@pytest.mark.asyncio
async def test_fetch_airport_daily_no_temps():
    """Returns None when temps can't be parsed from the DOM."""
    mock_page = AsyncMock()
    mock_page.wait_for_selector = AsyncMock()
    # No matching rows
    mock_page.query_selector_all = AsyncMock(return_value=[])
    # Fallback also returns nothing
    mock_page.query_selector = AsyncMock(return_value=None)

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    result = await fetch_airport_daily(_TEST_STATION, date(2026, 2, 15), mock_browser)

    assert result is None


@pytest.mark.asyncio
async def test_fetch_airport_daily_page_error():
    """Returns None when page.goto raises."""
    mock_page = AsyncMock()
    mock_page.goto = AsyncMock(side_effect=Exception("Network error"))

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    result = await fetch_airport_daily(_TEST_STATION, date(2026, 2, 15), mock_browser)

    assert result is None


# ---------- helpers ----------


def _mock_value_cell(temp_text: str) -> AsyncMock:
    """Create a mock <td> cell containing a .wu-value-to span."""
    wu_span = AsyncMock()
    wu_span.inner_text = AsyncMock(return_value=temp_text)
    cell = AsyncMock()
    cell.query_selector = AsyncMock(return_value=wu_span)
    return cell
