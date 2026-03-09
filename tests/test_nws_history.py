"""Tests for the NCEI/NWS history data fetcher."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_weather.calibration.nws_history import NWSDailyObs, fetch_nws_history
from kalshi_weather.weather.stations import Station

_TEST_STATION = Station(
    icao="KATL",
    city="atlanta",
    lat_lon=(33.640, -84.410),
    timezone="America/New_York",
    ghcn_id="USW00013874",
)


# ---------- NWSDailyObs ----------

def test_nws_daily_obs_fields():
    obs = NWSDailyObs(
        station_id="KATL",
        date=date(2026, 2, 15),
        high_temp_c=20.0,
        low_temp_c=5.0,
    )
    assert obs.station_id == "KATL"
    assert obs.date == date(2026, 2, 15)
    assert obs.high_temp_c == 20.0
    assert obs.low_temp_c == 5.0


# ---------- fetch_nws_history ----------

@pytest.mark.asyncio
async def test_fetch_nws_history_success():
    """Returns NWSDailyObs list from valid NCEI JSON response."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"DATE": "2026-02-15", "STATION": "USW00013874", "TMAX": "20.0", "TMIN": "5.0"},
        {"DATE": "2026-02-16", "STATION": "USW00013874", "TMAX": "22.5", "TMIN": "7.2"},
    ]

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 2, 15), date(2026, 2, 16),
        )

    assert len(result) == 2
    assert result[0].station_id == "KATL"
    assert result[0].date == date(2026, 2, 15)
    assert result[0].high_temp_c == 20.0
    assert result[0].low_temp_c == 5.0
    assert result[1].high_temp_c == 22.5


@pytest.mark.asyncio
async def test_fetch_nws_history_missing_tmin():
    """Skips records with missing TMIN."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"DATE": "2026-02-15", "STATION": "USW00013874", "TMAX": "20.0", "TMIN": "5.0"},
        {"DATE": "2026-02-16", "STATION": "USW00013874", "TMAX": "22.5"},
    ]

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 2, 15), date(2026, 2, 16),
        )

    assert len(result) == 1
    assert result[0].date == date(2026, 2, 15)


@pytest.mark.asyncio
async def test_fetch_nws_history_api_error():
    """Returns empty list on API failure."""
    mock_client = AsyncMock()
    mock_client.get.side_effect = Exception("Connection error")
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 2, 15), date(2026, 2, 16),
        )

    assert result == []


@pytest.mark.asyncio
async def test_fetch_nws_history_empty_response():
    """Returns empty list when NCEI returns no records."""
    mock_response = MagicMock()
    mock_response.json.return_value = []

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 2, 15), date(2026, 2, 16),
        )

    assert result == []


@pytest.mark.asyncio
async def test_fetch_nws_history_negative_temps():
    """Handles negative temperatures correctly."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"DATE": "2026-01-15", "STATION": "USW00013874", "TMAX": "-3.9", "TMIN": "-11.7"},
    ]

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 1, 15), date(2026, 1, 15),
        )

    assert len(result) == 1
    assert result[0].high_temp_c == pytest.approx(-3.9)
    assert result[0].low_temp_c == pytest.approx(-11.7)


@pytest.mark.asyncio
async def test_fetch_nws_history_sorted_by_date():
    """Results are sorted by date even if API returns them out of order."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"DATE": "2026-02-17", "STATION": "USW00013874", "TMAX": "18.0", "TMIN": "3.0"},
        {"DATE": "2026-02-15", "STATION": "USW00013874", "TMAX": "20.0", "TMIN": "5.0"},
        {"DATE": "2026-02-16", "STATION": "USW00013874", "TMAX": "22.0", "TMIN": "7.0"},
    ]

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("kalshi_weather.calibration.nws_history.HttpClient", return_value=mock_client):
        result = await fetch_nws_history(
            _TEST_STATION, date(2026, 2, 15), date(2026, 2, 17),
        )

    assert len(result) == 3
    assert result[0].date == date(2026, 2, 15)
    assert result[1].date == date(2026, 2, 16)
    assert result[2].date == date(2026, 2, 17)
