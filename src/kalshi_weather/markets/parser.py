"""Kalshi weather market ticker parser.

Kalshi weather tickers encode structured data directly:
  Series:  KXHIGHNY      -> high temp, New York
  Event:   KXHIGHNY-26MAR07  -> high temp, New York, March 7 2026
  Market:  KXHIGHNY-26MAR07-B55  -> above 55F
           KXHIGHNY-26MAR07-T55  -> between 55-56F (bucket)
           KXHIGHNY-26MAR07-U55  -> under 55F

No LLM needed — tickers are fully structured.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from kalshi_weather.markets.models import Comparison, MarketParams, MarketType
from kalshi_weather.weather.geocoding import geocode

logger = logging.getLogger(__name__)

# Known city codes used by Kalshi weather series
KALSHI_CITY_CODES: dict[str, str] = {
    "NY": "New York, NY",
    "NYC": "New York, NY",
    "CHI": "Chicago, IL",
    "MIA": "Miami, FL",
    "LAX": "Los Angeles, CA",
    "DEN": "Denver, CO",
    "ATL": "Atlanta, GA",
    "DAL": "Dallas, TX",
    "SEA": "Seattle, WA",
    "HOU": "Houston, TX",
    "PHX": "Phoenix, AZ",
    "SFO": "San Francisco, CA",
    "BOS": "Boston, MA",
    "DCA": "Washington, DC",
    "MSP": "Minneapolis, MN",
    "DTW": "Detroit, MI",
    "PHL": "Philadelphia, PA",
    "AUS": "Austin, TX",
    "LAS": "Las Vegas, NV",
    "STL": "St. Louis, MO",
}

# Series prefix -> (MarketType, daily_aggregation)
_SERIES_TYPE_MAP: dict[str, tuple[MarketType, str | None]] = {
    "KXHIGH": (MarketType.TEMPERATURE, "max"),
    "KXLOW": (MarketType.TEMPERATURE, "min"),
    "KXRAIN": (MarketType.PRECIPITATION, None),
}

# Date component: 26MAR07 -> year=2026, month=3, day=7
_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

_DATE_PATTERN = re.compile(r"(\d{2})([A-Z]{3})(\d{2})")

# Threshold component: B55 (above/at-or-above 55), T55 (between 55-56), U55 (under 55)
_THRESHOLD_PATTERN = re.compile(r"^([BTU])(-?\d+)$")


def _parse_date_component(date_str: str) -> datetime | None:
    """Parse '26MAR07' -> datetime(2026, 3, 7)."""
    m = _DATE_PATTERN.match(date_str)
    if not m:
        return None
    year = 2000 + int(m.group(1))
    month = _MONTH_MAP.get(m.group(2))
    day = int(m.group(3))
    if month is None:
        return None
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_threshold_component(
    thresh_str: str,
) -> tuple[Comparison, float, float | None] | None:
    """Parse 'B55' -> (ABOVE, 55.0, None), 'T55' -> (BETWEEN, 55.0, 56.0)."""
    m = _THRESHOLD_PATTERN.match(thresh_str)
    if not m:
        return None
    prefix = m.group(1)
    value = float(m.group(2))
    if prefix == "B":
        return Comparison.ABOVE, value, None
    elif prefix == "U":
        return Comparison.BELOW, value, None
    elif prefix == "T":
        return Comparison.BETWEEN, value, value + 1.0
    return None


def _extract_series_prefix(series: str) -> tuple[str, str] | None:
    """Extract series prefix and city code from series ticker.

    'KXHIGHNY' -> ('KXHIGH', 'NY')
    'KXRAINCHI' -> ('KXRAIN', 'CHI')
    """
    for prefix in sorted(_SERIES_TYPE_MAP.keys(), key=len, reverse=True):
        if series.startswith(prefix):
            city_code = series[len(prefix):]
            if city_code:
                return prefix, city_code
    return None


async def parse_kalshi_market(raw: dict) -> MarketParams | None:
    """Parse a raw Kalshi market dict into structured MarketParams.

    Uses the ticker structure for parsing. No LLM needed.
    Returns None if parsing fails.
    """
    ticker = raw.get("ticker", "")
    title = raw.get("title", "")
    subtitle = raw.get("subtitle", "")

    # Split ticker: "KXHIGHNY-26MAR07-B55" -> ["KXHIGHNY", "26MAR07", "B55"]
    parts = ticker.split("-")
    if not parts:
        return None

    # Parse series prefix + city code
    result = _extract_series_prefix(parts[0])
    if not result:
        logger.debug("Cannot parse series from ticker: %s", ticker)
        return None

    series_prefix, city_code = result
    market_type, daily_aggregation = _SERIES_TYPE_MAP[series_prefix]

    # Resolve city name
    location = KALSHI_CITY_CODES.get(city_code, city_code)

    # Parse date
    target_date = None
    target_date_str = ""
    if len(parts) >= 2:
        target_date = _parse_date_component(parts[1])
        target_date_str = parts[1]

    # Parse threshold
    comparison = Comparison.ABOVE
    threshold = None
    threshold_upper = None
    unit = "F"  # Kalshi US markets use Fahrenheit

    if len(parts) >= 3:
        thresh_result = _parse_threshold_component(parts[2])
        if thresh_result:
            comparison, threshold, threshold_upper = thresh_result
    else:
        # Fall back to extracting from title text
        temp_match = re.search(
            r"(\d+(?:\.\d+)?)\s*°?\s*([FfCc])", title + " " + subtitle
        )
        if temp_match:
            threshold = float(temp_match.group(1))
            unit = temp_match.group(2).upper()

    # Geocode the location
    lat_lon = await geocode(location)

    return MarketParams(
        market_type=market_type,
        location=location,
        lat_lon=lat_lon,
        threshold=threshold,
        threshold_upper=threshold_upper,
        comparison=comparison,
        unit=unit,
        target_date=target_date,
        target_date_str=target_date_str,
        daily_aggregation=daily_aggregation,
    )
