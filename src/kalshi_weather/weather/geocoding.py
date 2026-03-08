"""Location name to lat/lon geocoding using geopy Nominatim."""

from __future__ import annotations

import logging
from collections import OrderedDict

from geopy.adapters import AioHTTPAdapter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from geopy.geocoders import Nominatim

from kalshi_weather.common.types import LatLon

logger = logging.getLogger(__name__)

# Weather Underground airport coordinates for Polymarket cities.
# Markets resolve on WU airport history pages (e.g. /history/daily/.../CYYZ).
_STATION_COORDS: dict[str, LatLon] = {
    "atlanta":       (33.640, -84.410),     # KATL
    "atlanta, ga":   (33.640, -84.410),
    "new york":      (40.760, -73.860),     # KLGA
    "new york city":  (40.760, -73.860),
    "new york, ny":  (40.760, -73.860),
    "nyc":           (40.760, -73.860),
    "dallas":        (32.850, -96.870),     # KDFW
    "dallas, tx":    (32.850, -96.870),
    "miami":         (25.850, -80.240),     # KMIA
    "miami, fl":     (25.850, -80.240),
    "chicago":       (41.980, -87.910),     # KORD
    "chicago, il":   (41.980, -87.910),
    "seattle":       (47.440, -122.300),    # KSEA
    "seattle, wa":   (47.440, -122.300),
    "toronto":       (43.710, -79.660),     # CYYZ
    "toronto, on":   (43.710, -79.660),
    "london":        (51.510, 0.030),       # EGLL
    "london, uk":    (51.510, 0.030),
    "paris":         (49.020, 2.590),       # LFPG
    "paris, fr":     (49.020, 2.590),
    "buenos aires":  (-34.790, -58.520),    # SAEZ
    "buenos aires, ar": (-34.790, -58.520),
    "sao paulo":     (-23.420, -46.480),    # SBGR
    "são paulo":     (-23.420, -46.480),
    "sao paulo, br": (-23.420, -46.480),
    "são paulo, br": (-23.420, -46.480),
    "wellington":    (-41.320, 174.800),    # NZWN
    "wellington, nz": (-41.320, 174.800),
    "incheon":       (37.490, 126.490),     # RKSI
    "incheon, kr":   (37.490, 126.490),
    "seoul":         (37.490, 126.490),     # resolves at Incheon (RKSI)
    "seoul, kr":     (37.490, 126.490),
    "ankara":        (40.240, 33.030),      # LTAC
    "ankara, tr":    (40.240, 33.030),
    "lucknow":       (26.770, 80.880),     # VILK
    "lucknow, in":   (26.770, 80.880),
    "munich":        (48.350, 11.780),     # EDDM
    "munich, de":    (48.350, 11.780),
    "münchen":       (48.350, 11.780),
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

    # Fast path: known Polymarket weather station
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
