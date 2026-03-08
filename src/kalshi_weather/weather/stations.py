"""Weather Underground airport station registry.

Maps airport ICAO codes used by Kalshi weather markets to their
metadata (location, coordinates, timezone, WU history page path).

Kalshi weather markets are US-only, so all stations are domestic airports.
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


# All airport stations for Kalshi weather market cities.
# Ordered to match the KALSHI_CITY_CODES in markets/parser.py.
STATIONS: dict[str, Station] = {
    # --- 10 default series cities ---
    "KLGA": Station(
        icao="KLGA", city="new york",
        lat_lon=(40.760, -73.860), timezone="America/New_York",
        history_path="us/ny/new-york-city/KLGA",
    ),
    "KORD": Station(
        icao="KORD", city="chicago",
        lat_lon=(41.980, -87.910), timezone="America/Chicago",
        history_path="us/il/chicago/KORD",
    ),
    "KMIA": Station(
        icao="KMIA", city="miami",
        lat_lon=(25.850, -80.240), timezone="America/New_York",
        history_path="us/fl/miami/KMIA",
    ),
    "KLAX": Station(
        icao="KLAX", city="los angeles",
        lat_lon=(33.940, -118.410), timezone="America/Los_Angeles",
        history_path="us/ca/los-angeles/KLAX",
    ),
    "KDEN": Station(
        icao="KDEN", city="denver",
        lat_lon=(39.850, -104.670), timezone="America/Denver",
        history_path="us/co/denver/KDEN",
    ),
    "KATL": Station(
        icao="KATL", city="atlanta",
        lat_lon=(33.640, -84.410), timezone="America/New_York",
        history_path="us/ga/atlanta/KATL",
    ),
    "KDFW": Station(
        icao="KDFW", city="dallas",
        lat_lon=(32.850, -96.870), timezone="America/Chicago",
        history_path="us/tx/dallas/KDFW",
    ),
    "KSEA": Station(
        icao="KSEA", city="seattle",
        lat_lon=(47.440, -122.300), timezone="America/Los_Angeles",
        history_path="us/wa/seattle/KSEA",
    ),
    "KIAH": Station(
        icao="KIAH", city="houston",
        lat_lon=(29.980, -95.340), timezone="America/Chicago",
        history_path="us/tx/houston/KIAH",
    ),
    "KPHX": Station(
        icao="KPHX", city="phoenix",
        lat_lon=(33.440, -112.010), timezone="America/Phoenix",
        history_path="us/az/phoenix/KPHX",
    ),
    # --- Additional Kalshi cities ---
    "KSFO": Station(
        icao="KSFO", city="san francisco",
        lat_lon=(37.620, -122.370), timezone="America/Los_Angeles",
        history_path="us/ca/san-francisco/KSFO",
    ),
    "KBOS": Station(
        icao="KBOS", city="boston",
        lat_lon=(42.360, -71.010), timezone="America/New_York",
        history_path="us/ma/boston/KBOS",
    ),
    "KDCA": Station(
        icao="KDCA", city="washington",
        lat_lon=(38.850, -77.040), timezone="America/New_York",
        history_path="us/va/arlington/KDCA",
    ),
    "KMSP": Station(
        icao="KMSP", city="minneapolis",
        lat_lon=(44.880, -93.220), timezone="America/Chicago",
        history_path="us/mn/minneapolis/KMSP",
    ),
    "KDTW": Station(
        icao="KDTW", city="detroit",
        lat_lon=(42.210, -83.350), timezone="America/Detroit",
        history_path="us/mi/detroit/KDTW",
    ),
    "KPHL": Station(
        icao="KPHL", city="philadelphia",
        lat_lon=(39.870, -75.240), timezone="America/New_York",
        history_path="us/pa/philadelphia/KPHL",
    ),
    "KAUS": Station(
        icao="KAUS", city="austin",
        lat_lon=(30.190, -97.670), timezone="America/Chicago",
        history_path="us/tx/austin/KAUS",
    ),
    "KLAS": Station(
        icao="KLAS", city="las vegas",
        lat_lon=(36.080, -115.150), timezone="America/Los_Angeles",
        history_path="us/nv/las-vegas/KLAS",
    ),
    "KSTL": Station(
        icao="KSTL", city="st. louis",
        lat_lon=(38.750, -90.370), timezone="America/Chicago",
        history_path="us/mo/st-louis/KSTL",
    ),
}

# City name aliases for lookup — maps alternative names to canonical city.
_CITY_ALIASES: dict[str, str] = {
    # New York
    "new york city": "new york",
    "nyc": "new york",
    "new york, ny": "new york",
    # Chicago
    "chicago, il": "chicago",
    # Miami
    "miami, fl": "miami",
    # Los Angeles
    "los angeles, ca": "los angeles",
    "la": "los angeles",
    "lax": "los angeles",
    # Denver
    "denver, co": "denver",
    # Atlanta
    "atlanta, ga": "atlanta",
    # Dallas
    "dallas, tx": "dallas",
    "dallas-fort worth": "dallas",
    "dfw": "dallas",
    # Seattle
    "seattle, wa": "seattle",
    # Houston
    "houston, tx": "houston",
    # Phoenix
    "phoenix, az": "phoenix",
    # San Francisco
    "san francisco, ca": "san francisco",
    "sf": "san francisco",
    # Boston
    "boston, ma": "boston",
    # Washington DC
    "washington, dc": "washington",
    "washington dc": "washington",
    "dc": "washington",
    "dca": "washington",
    # Minneapolis
    "minneapolis, mn": "minneapolis",
    "minneapolis-st. paul": "minneapolis",
    "msp": "minneapolis",
    # Detroit
    "detroit, mi": "detroit",
    # Philadelphia
    "philadelphia, pa": "philadelphia",
    "philly": "philadelphia",
    # Austin
    "austin, tx": "austin",
    # Las Vegas
    "las vegas, nv": "las vegas",
    "vegas": "las vegas",
    # St. Louis
    "st. louis, mo": "st. louis",
    "saint louis": "st. louis",
    "stl": "st. louis",
}

# Build reverse index: city name -> Station (for O(1) exact lookup).
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
        station_for_location("atlanta") -> KATL
        station_for_location("Atlanta, GA") -> KATL
        station_for_location("new york city") -> KLGA
        station_for_location("Phoenix, AZ") -> KPHX
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
    for station_city, station in _CITY_INDEX.items():
        if normalized.startswith(station_city) and len(normalized) > len(station_city):
            return station

    return None
