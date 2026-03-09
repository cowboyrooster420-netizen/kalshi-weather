"""NCEI Data Service API client for NWS observed daily temperatures.

Fetches daily TMAX/TMIN from the NOAA NCEI Access Data Service.
This is the authoritative source for NWS Daily Climate Report (CLI)
temperatures, which Kalshi uses for market settlement.

API docs: https://www.ncei.noaa.gov/access/services/data/v1
Free, no authentication required.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from kalshi_weather.common.http import HttpClient
from kalshi_weather.weather.stations import Station

logger = logging.getLogger(__name__)

_NCEI_URL = "https://www.ncei.noaa.gov/access/services/data/v1"


@dataclass
class NWSDailyObs:
    """Daily high/low observation from the NWS (via NCEI)."""

    station_id: str     # ICAO code, e.g. "KATL"
    date: date
    high_temp_c: float  # Daily max in Celsius
    low_temp_c: float   # Daily min in Celsius


async def fetch_nws_history(
    station: Station,
    start: date,
    end: date,
) -> list[NWSDailyObs]:
    """Fetch daily TMAX/TMIN from the NCEI daily-summaries dataset.

    Uses the station's GHCN ID to query the NCEI Access Data Service.
    Returns temperatures in Celsius.

    Args:
        station: Station with icao and ghcn_id.
        start: Start date (inclusive).
        end: End date (inclusive).

    Returns:
        List of daily observations sorted by date. Days with missing
        TMAX or TMIN are omitted.
    """
    params = {
        "dataset": "daily-summaries",
        "dataTypes": "TMAX,TMIN",
        "stations": station.ghcn_id,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "format": "json",
        "units": "metric",
    }

    async with HttpClient() as client:
        try:
            resp = await client.get(_NCEI_URL, params=params)
            records = resp.json()
        except Exception as exc:
            logger.warning(
                "Failed to fetch NCEI data for %s (%s): %s",
                station.icao, station.ghcn_id, exc,
            )
            return []

    if not isinstance(records, list):
        logger.warning(
            "Unexpected NCEI response for %s: expected list, got %s",
            station.icao, type(records).__name__,
        )
        return []

    observations: list[NWSDailyObs] = []
    for record in records:
        try:
            obs_date = date.fromisoformat(record["DATE"])
            tmax_str = record.get("TMAX")
            tmin_str = record.get("TMIN")
            if tmax_str is None or tmin_str is None:
                continue
            observations.append(NWSDailyObs(
                station_id=station.icao,
                date=obs_date,
                high_temp_c=float(tmax_str),
                low_temp_c=float(tmin_str),
            ))
        except (ValueError, TypeError, KeyError) as exc:
            logger.debug(
                "Skipping invalid NCEI record for %s: %s",
                station.icao, exc,
            )

    observations.sort(key=lambda o: o.date)

    logger.info(
        "Fetched %d/%d days of NWS data for %s (%s to %s)",
        len(observations), (end - start).days + 1,
        station.icao, start, end,
    )
    return observations
