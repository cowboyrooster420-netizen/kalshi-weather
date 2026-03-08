"""Location name to lat/lon geocoding using geopy Nominatim."""

from __future__ import annotations

import logging
from collections import OrderedDict

from geopy.adapters import AioHTTPAdapter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from geopy.geocoders import Nominatim

from kalshi_weather.common.types import LatLon

logger = logging.getLogger(__name__)

# Airport coordinates for all Kalshi weather market cities.
# These must match the STATIONS dict in stations.py.
_STATION_COORDS: dict[str, LatLon] = {
    # New York — KLGA
    "new york":          (40.760, -73.860),
    "new york city":     (40.760, -73.860),
    "new york, ny":      (40.760, -73.860),
    "nyc":               (40.760, -73.860),
    # Chicago — KORD
    "chicago":           (41.980, -87.910),
    "chicago, il":       (41.980, -87.910),
    # Miami — KMIA
    "miami":             (25.850, -80.240),
    "miami, fl":         (25.850, -80.240),
    # Los Angeles — KLAX
    "los angeles":       (33.940, -118.410),
    "los angeles, ca":   (33.940, -118.410),
    "la":                (33.940, -118.410),
    # Denver — KDEN
    "denver":            (39.850, -104.670),
    "denver, co":        (39.850, -104.670),
    # Atlanta — KATL
    "atlanta":           (33.640, -84.410),
    "atlanta, ga":       (33.640, -84.410),
    # Dallas — KDFW
    "dallas":            (32.850, -96.870),
    "dallas, tx":        (32.850, -96.870),
    "dallas-fort worth": (32.850, -96.870),
    # Seattle — KSEA
    "seattle":           (47.440, -122.300),
    "seattle, wa":       (47.440, -122.300),
    # Houston — KIAH
    "houston":           (29.980, -95.340),
    "houston, tx":       (29.980, -95.340),
    # Phoenix — KPHX
    "phoenix":           (33.440, -112.010),
    "phoenix, az":       (33.440, -112.010),
    # San Francisco — KSFO
    "san francisco":     (37.620, -122.370),
    "san francisco, ca": (37.620, -122.370),
    "sf":                (37.620, -122.370),
    # Boston — KBOS
    "boston":             (42.360, -71.010),
    "boston, ma":         (42.360, -71.010),
    # Washington DC — KDCA
    "washington":        (38.850, -77.040),
    "washington, dc":    (38.850, -77.040),
    "washington dc":     (38.850, -77.040),
    "dc":                (38.850, -77.040),
    # Minneapolis — KMSP
    "minneapolis":       (44.880, -93.220),
    "minneapolis, mn":   (44.880, -93.220),
    # Detroit — KDTW
    "detroit":           (42.210, -83.350),
    "detroit, mi":       (42.210, -83.350),
    # Philadelphia — KPHL
    "philadelphia":      (39.870, -75.240),
    "philadelphia, pa":  (39.870, -75.240),
    "philly":            (39.870, -75.240),
    # Austin — KAUS
    "austin":            (30.190, -97.670),
    "austin, tx":        (30.190, -97.670),
    # Las Vegas — KLAS
    "las vegas":         (36.080, -115.150),
    "las vegas, nv":     (36.080, -115.150),
    "vegas":             (36.080, -115.150),
    # St. Louis — KSTL
    "st. louis":         (38.750, -90.370),
    "st. louis, mo":     (38.750, -90.370),
    "saint louis":       (38.750, -90.370),
}

# Bounded LRU cache for geocoding results (oldest evicted first)
_MAX_CACHE_SIZE = 256
_cache: OrderedDict[str, LatLon | None] = OrderedDict()


def _cache_put(key: str, value: LatLon | None) -> None:
    """Insert into bounded cache, evicting oldest if full."""
    _cache[key] = value
    _cache.move_to_end(key)
    if len(_cache) > _MAX_CACHE_SIZE:
        _cache.popitem(last=False)


async def geocode(location: str) -> LatLon | None:
    """Convert a location name to (lat, lon).

    Uses Nominatim (free, no API key). Results are cached in memory
    with a max size of 256 entries (LRU eviction).
    Returns None if the location cannot be resolved.
    """
    normalized = location.strip().lower()

    # Fast path: known Kalshi weather station
    station = _STATION_COORDS.get(normalized)
    if station is not None:
        return station

    if normalized in _cache:
        _cache.move_to_end(normalized)
        return _cache[normalized]

    try:
        async with Nominatim(
            user_agent="kalshi-weather",
            adapter_factory=AioHTTPAdapter,
        ) as geolocator:
            result = await geolocator.geocode(location)
            if result is None:
                logger.debug("Geocoding returned no results for %r", location)
                _cache_put(normalized, None)
                return None
            latlon: LatLon = (result.latitude, result.longitude)
            _cache_put(normalized, latlon)
            return latlon
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as exc:
        logger.warning("Geocoding service error for %r: %s", location, exc)
        _cache_put(normalized, None)
        return None
    except (ValueError, TypeError) as exc:
        logger.warning("Geocoding parse error for %r: %s", location, exc)
        _cache_put(normalized, None)
        return None
