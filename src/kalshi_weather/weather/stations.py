"""Weather Underground airport station registry.

Maps airport ICAO codes used by Polymarket weather markets to their
metadata (location, coordinates, timezone, WU history page path).

Polymarket markets resolve on WU airport history pages (e.g.
``/history/daily/ca/mississauga/CYYZ``), not PWS dashboard pages.

IMPORTANT: Station coordinates MUST match the authoritative values in
geocoding.py's _STATION_COORDS dict. Those are the stations that
Polymarket markets actually resolve on.
"""

from __future__ import annotations

from dataclasses import dataclass

from kalshi_weather.common.types import LatLon


@dataclass(frozen=True)
class Station:
    """A Weather Underground airport weather station."""

    icao: str           # Airport ICAO code, e.g. "KATL"
    city: str           # e.g. "atlanta"
    lat_lon: LatLon     # (lat, lon)
    timezone: str       # IANA timezone, e.g. "America/New_York"
    history_path: str   # WU history page path, e.g. "us/ga/atlanta/KATL"


# All 14 airport stations referenced by Polymarket weather markets.
# Coordinates sourced from geocoding.py _STATION_COORDS.
STATIONS: dict[str, Station] = {
    "KATL": Station(
        icao="KATL", city="atlanta",
        lat_lon=(33.640, -84.410), timezone="America/New_York",
        history_path="us/ga/atlanta/KATL",
    ),
    "KLGA": Station(
        icao="KLGA", city="new york",
        lat_lon=(40.760, -73.860), timezone="America/New_York",
        history_path="us/ny/new-york-city/KLGA",
    ),
    "KDFW": Station(
        icao="KDFW", city="dallas",
        lat_lon=(32.850, -96.870), timezone="America/Chicago",
        history_path="us/tx/dallas/KDFW",
    ),
    "KMIA": Station(
        icao="KMIA", city="miami",
        lat_lon=(25.850, -80.240), timezone="America/New_York",
        history_path="us/fl/miami/KMIA",
    ),
    "KORD": Station(
        icao="KORD", city="chicago",
        lat_lon=(41.980, -87.910), timezone="America/Chicago",
        history_path="us/il/chicago/KORD",
    ),
    "KSEA": Station(
        icao="KSEA", city="seattle",
        lat_lon=(47.440, -122.300), timezone="America/Los_Angeles",
        history_path="us/wa/seattle/KSEA",
    ),
    "CYYZ": Station(
        icao="CYYZ", city="toronto",
        lat_lon=(43.710, -79.660), timezone="America/Toronto",
        history_path="ca/mississauga/CYYZ",
    ),
    "EGLL": Station(
        icao="EGLL", city="london",
        lat_lon=(51.510, 0.030), timezone="Europe/London",
        history_path="gb/london/EGLL",
    ),
    "LFPG": Station(
        icao="LFPG", city="paris",
        lat_lon=(49.020, 2.590), timezone="Europe/Paris",
        history_path="fr/paris/LFPG",
    ),
    "SAEZ": Station(
        icao="SAEZ", city="buenos aires",
        lat_lon=(-34.790, -58.520), timezone="America/Argentina/Buenos_Aires",
        history_path="ar/buenos-aires/SAEZ",
    ),
    "SBGR": Station(
        icao="SBGR", city="sao paulo",
        lat_lon=(-23.420, -46.480), timezone="America/Sao_Paulo",
        history_path="br/guarulhos/SBGR",
    ),
    "NZWN": Station(
        icao="NZWN", city="wellington",
        lat_lon=(-41.320, 174.800), timezone="Pacific/Auckland",
        history_path="nz/wellington/NZWN",
    ),
    "RKSI": Station(
        icao="RKSI", city="incheon",
        lat_lon=(37.490, 126.490), timezone="Asia/Seoul",
        history_path="kr/incheon/RKSI",
    ),
    "LTAC": Station(
        icao="LTAC", city="ankara",
        lat_lon=(40.240, 33.030), timezone="Europe/Istanbul",
        history_path="tr/ankara/LTAC",
    ),
    "VILK": Station(
        icao="VILK", city="lucknow",
        lat_lon=(26.770, 80.880), timezone="Asia/Kolkata",
        history_path="in/lucknow/VILK",
    ),
    "EDDM": Station(
        icao="EDDM", city="munich",
        lat_lon=(48.350, 11.780), timezone="Europe/Berlin",
        history_path="de/munich/EDDM",
    ),
}

# City name aliases for lookup — maps alternative names to canonical city.
_CITY_ALIASES: dict[str, str] = {
    "new york city": "new york",
    "nyc": "new york",
    "new york, ny": "new york",
    "atlanta, ga": "atlanta",
    "dallas, tx": "dallas",
    "miami, fl": "miami",
    "chicago, il": "chicago",
    "seattle, wa": "seattle",
    "toronto, on": "toronto",
    "london, uk": "london",
    "paris, fr": "paris",
    "buenos aires, ar": "buenos aires",
    "sao paulo, br": "sao paulo",
    "são paulo": "sao paulo",
    "são paulo, br": "sao paulo",
    "wellington, nz": "wellington",
    "incheon, kr": "incheon",
    "seoul": "incheon",
    "seoul, kr": "incheon",
    "ankara, tr": "ankara",
    "lucknow, in": "lucknow",
    "munich, de": "munich",
    "münchen": "munich",
}

# Build reverse index: city name → Station (for O(1) exact lookup).
_CITY_INDEX: dict[str, Station] = {}
for _station in STATIONS.values():
    _CITY_INDEX[_station.city] = _station
for _alias, _canonical in _CITY_ALIASES.items():
    if _canonical in _CITY_INDEX:
        _CITY_INDEX[_alias] = _CITY_INDEX[_canonical]


def station_for_location(city: str) -> Station | None:
    """Find a station by city name (case-insensitive).

    Supports exact match, alias resolution, and input-prefix matching
    (e.g. "new york city" matches "new york"). Does NOT allow the
    station city to prefix-match short inputs (e.g. "d" won't match
    "dallas").

    Examples:
        station_for_location("atlanta") → KATL
        station_for_location("Atlanta, GA") → KATL
        station_for_location("new york city") → KLGA
        station_for_location("Seoul, KR") → RKSI
    """
    normalized = city.lower().strip()

    # Exact match first (including comma aliases like "atlanta, ga")
    match = _CITY_INDEX.get(normalized)
    if match is not None:
        return match

    # Strip state/country suffix and retry
    if "," in normalized:
        normalized = normalized.split(",")[0].strip()
        match = _CITY_INDEX.get(normalized)
        if match is not None:
            return match

    # Input-prefix match: "new york city" starts with "new york".
    # Only allow the INPUT to be longer than the station city, not shorter.
    # This prevents "d" from matching "dallas".
    for station_city, station in _CITY_INDEX.items():
        if normalized.startswith(station_city) and len(normalized) > len(station_city):
            return station

    return None
