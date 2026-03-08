"""Kalshi weather market classifier.

On Kalshi, weather markets are already categorized by series ticker
(KXHIGH*, KXLOW*, KXRAIN*), so no LLM classification is needed.
This module provides a simple filter based on series prefix.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Series prefixes that are weather markets
_WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "KXRAIN")


def is_weather_market(raw: dict) -> bool:
    """Check if a Kalshi market is weather-related.

    Simply checks if the series_ticker or ticker starts with
    a known weather prefix.
    """
    series = raw.get("series_ticker", "")
    ticker = raw.get("ticker", "")

    for prefix in _WEATHER_PREFIXES:
        if series.startswith(prefix) or ticker.startswith(prefix):
            return True
    return False
