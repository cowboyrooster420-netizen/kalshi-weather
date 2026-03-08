"""Shared test fixtures."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from kalshi_weather.forecasting.base import ProbabilityEstimate
from kalshi_weather.markets.models import Comparison, MarketParams, MarketType, WeatherMarket
from kalshi_weather.signals.models import Signal
from kalshi_weather.weather.models import EnsembleForecast, NOAAAlert, NOAAForecast, NOAAPeriod


@pytest.fixture
def now():
    return datetime.now(timezone.utc)


@pytest.fixture
def ecmwf_forecast(now):
    """Synthetic ECMWF ensemble: 51 members, 48 hours, temp ~35C."""
    np.random.seed(42)
    times = [now + timedelta(hours=i) for i in range(48)]
    n = len(times)
    return EnsembleForecast(
        source="ecmwf",
        lat=33.45,
        lon=-112.07,
        times=times,
        temperature_2m=np.random.normal(35, 3, (n, 51)),
        precipitation=np.maximum(0, np.random.exponential(2, (n, 51)) - 1),
    )


@pytest.fixture
def gfs_forecast(now):
    """Synthetic GFS ensemble: 31 members, 48 hours, temp ~34C."""
    np.random.seed(43)
    times = [now + timedelta(hours=i) for i in range(48)]
    n = len(times)
    return EnsembleForecast(
        source="gfs",
        lat=33.45,
        lon=-112.07,
        times=times,
        temperature_2m=np.random.normal(34, 3.5, (n, 31)),
        precipitation=np.maximum(0, np.random.exponential(2.5, (n, 31)) - 1),
    )


@pytest.fixture
def noaa_forecast(now):
    """Synthetic NOAA forecast with periods and alerts."""
    return NOAAForecast(
        lat=33.45,
        lon=-112.07,
        office="PSR",
        grid_x=100,
        grid_y=50,
        periods=[
            NOAAPeriod(
                start_time=now + timedelta(hours=i),
                end_time=now + timedelta(hours=i + 1),
                temperature=95.0 + i * 0.5,
                temperature_unit="F",
                wind_speed="10 mph",
                short_forecast="Sunny",
                detailed_forecast="Sunny with a high near 95.",
                precipitation_probability=10.0,
            )
            for i in range(48)
        ],
        alerts=[],
    )


@pytest.fixture
def noaa_forecast_with_hurricane_alert(noaa_forecast, now):
    """NOAA forecast with an active hurricane alert."""
    noaa_forecast.alerts = [
        NOAAAlert(
            event="Hurricane Warning",
            headline="Hurricane Warning in effect",
            severity="Extreme",
            certainty="Observed",
            onset=now,
            expires=now + timedelta(days=2),
            description="A major hurricane is approaching.",
        )
    ]
    return noaa_forecast


@pytest.fixture
def temperature_params(now):
    """Market params: Will Phoenix exceed 100F tomorrow?"""
    return MarketParams(
        market_type=MarketType.TEMPERATURE,
        location="Phoenix, AZ",
        lat_lon=(33.45, -112.07),
        threshold=100.0,
        comparison=Comparison.ABOVE,
        unit="F",
        target_date=now + timedelta(hours=24),
    )


@pytest.fixture
def precipitation_params(now):
    """Market params: Will Houston get more than 2 inches of rain?"""
    return MarketParams(
        market_type=MarketType.PRECIPITATION,
        location="Houston, TX",
        lat_lon=(29.76, -95.37),
        threshold=2.0,
        comparison=Comparison.ABOVE,
        unit="in",
        target_date=now + timedelta(hours=24),
    )


@pytest.fixture
def hurricane_params(now):
    """Market params: Will a hurricane hit Florida?"""
    return MarketParams(
        market_type=MarketType.HURRICANE,
        location="Miami, FL",
        lat_lon=(25.76, -80.19),
        target_date=now + timedelta(days=7),
    )


@pytest.fixture
def sample_weather_market(temperature_params):
    """A sample weather market for testing."""
    return WeatherMarket(
        market_id="KXHIGHPHX-26MAR08-B100",
        event_ticker="KXHIGHPHX-26MAR08",
        series_ticker="KXHIGHPHX",
        question="Will Phoenix exceed 100F tomorrow?",
        description="Temperature market for Phoenix",
        outcome_yes_price=0.35,
        outcome_no_price=0.65,
        params=temperature_params,
        volume=50000.0,
    )


@pytest.fixture
def sample_estimate():
    """A sample probability estimate for testing."""
    return ProbabilityEstimate(
        probability=0.55,
        raw_probability=0.52,
        confidence=0.85,
        lead_time_hours=24.0,
        sources_used=["ECMWF (51 members)", "GFS (31 members)"],
        details="ECMWF: p=0.56 | GFS: p=0.53",
    )


@pytest.fixture
def sample_signal(now):
    """A sample signal for testing formatters."""
    return Signal(
        market_id="KXHIGHPHX-26MAR08-B100",
        question="Will Phoenix exceed 100F tomorrow?",
        market_type="temperature",
        location="Phoenix, AZ",
        model_prob=0.55,
        market_prob=0.35,
        edge=0.20,
        kelly_fraction=0.08,
        confidence=0.85,
        direction="YES",
        lead_time_hours=24.0,
        sources=["ECMWF (51 members)", "GFS (31 members)"],
        details="ECMWF: p=0.56 | GFS: p=0.53",
        timestamp=now,
    )


# --- Mock Kalshi API response fixtures ---


@pytest.fixture
def kalshi_api_response():
    """Mock Kalshi API response with weather and non-weather markets."""
    return [
        {
            "ticker": "KXHIGHNY-26MAR07-B55",
            "event_ticker": "KXHIGHNY-26MAR07",
            "series_ticker": "KXHIGHNY",
            "title": "High temperature in NYC above 55F on Mar 7?",
            "subtitle": "New York City high temperature market",
            "yes_price": 25,
            "no_price": 75,
            "status": "open",
            "volume": 100000,
            "close_time": "2026-03-08T00:00:00Z",
        },
        {
            "ticker": "KXHIGHHOU-26MAR07-B70",
            "event_ticker": "KXHIGHHOU-26MAR07",
            "series_ticker": "KXHIGHHOU",
            "title": "High temperature in Houston above 70F on Mar 7?",
            "subtitle": "Houston high temperature market",
            "yes_price": 40,
            "no_price": 60,
            "status": "open",
            "volume": 50000,
        },
        {
            "ticker": "KXHIGHCHI-26MAR07-T40",
            "event_ticker": "KXHIGHCHI-26MAR07",
            "series_ticker": "KXHIGHCHI",
            "title": "High temperature in Chicago between 40-41F on Mar 7?",
            "subtitle": "Chicago high temperature market",
            "yes_price": 15,
            "no_price": 85,
            "status": "open",
            "volume": 30000,
        },
        {
            "ticker": "KXHIGHATL-26MAR07-U50",
            "event_ticker": "KXHIGHATL-26MAR07",
            "series_ticker": "KXHIGHATL",
            "title": "High temperature in Atlanta under 50F on Mar 7?",
            "subtitle": "Atlanta high temperature market",
            "yes_price": 35,
            "no_price": 65,
            "status": "open",
            "volume": 75000,
        },
    ]


# Backward compatibility alias
@pytest.fixture
def gamma_api_response(kalshi_api_response):
    """Backward compat alias for kalshi_api_response."""
    return kalshi_api_response


@pytest.fixture
def openmeteo_ecmwf_response():
    """Mock Open-Meteo ECMWF ensemble API response."""
    import time

    base_time = int(time.time())
    times = [base_time + i * 3600 for i in range(48)]
    hourly = {"time": times}

    np.random.seed(42)
    for i in range(1, 52):
        member = f"member{i:02d}"
        hourly[f"temperature_2m_{member}"] = [
            round(35 + np.random.normal(0, 3), 2) for _ in range(48)
        ]
        hourly[f"precipitation_{member}"] = [
            round(max(0, np.random.exponential(1) - 0.5), 2) for _ in range(48)
        ]

    return {
        "latitude": 33.45,
        "longitude": -112.07,
        "hourly": hourly,
    }


@pytest.fixture
def openmeteo_gfs_response():
    """Mock Open-Meteo GFS ensemble API response."""
    import time

    base_time = int(time.time())
    times = [base_time + i * 3600 for i in range(48)]
    hourly = {"time": times}

    np.random.seed(43)
    for i in range(1, 32):
        member = f"member{i:02d}"
        hourly[f"temperature_2m_{member}"] = [
            round(34 + np.random.normal(0, 3.5), 2) for _ in range(48)
        ]
        hourly[f"precipitation_{member}"] = [
            round(max(0, np.random.exponential(1.2) - 0.5), 2) for _ in range(48)
        ]

    return {
        "latitude": 33.45,
        "longitude": -112.07,
        "hourly": hourly,
    }


@pytest.fixture
def noaa_points_response():
    """Mock NOAA /points API response."""
    return {
        "properties": {
            "gridId": "PSR",
            "gridX": 100,
            "gridY": 50,
            "forecast": "https://api.weather.gov/gridpoints/PSR/100,50/forecast",
        }
    }


@pytest.fixture
def noaa_hourly_response(now):
    """Mock NOAA hourly forecast response."""
    periods = []
    for i in range(48):
        t = now + timedelta(hours=i)
        periods.append({
            "startTime": t.isoformat(),
            "endTime": (t + timedelta(hours=1)).isoformat(),
            "temperature": 95 + i * 0.5,
            "temperatureUnit": "F",
            "windSpeed": "10 mph",
            "shortForecast": "Sunny",
            "detailedForecast": "Sunny with a high near 95.",
            "probabilityOfPrecipitation": {"value": 10},
        })
    return {"properties": {"periods": periods}}


@pytest.fixture
def noaa_alerts_response():
    """Mock NOAA alerts response (no active alerts)."""
    return {"features": []}


@pytest.fixture
def noaa_alerts_response_with_hurricane(now):
    """Mock NOAA alerts response with an active hurricane alert."""
    return {
        "features": [
            {
                "properties": {
                    "event": "Hurricane Warning",
                    "headline": "Hurricane Warning in effect",
                    "severity": "Extreme",
                    "certainty": "Observed",
                    "onset": now.isoformat(),
                    "expires": (now + timedelta(days=2)).isoformat(),
                    "description": "A major hurricane is approaching.",
                }
            }
        ]
    }
