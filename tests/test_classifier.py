"""Tests for Kalshi weather market classifier.

On Kalshi, weather markets are identified by series_ticker prefix
(KXHIGH*, KXLOW*, KXRAIN*), so classification is a simple prefix check
rather than regex/LLM analysis of free-text questions.
"""

from __future__ import annotations

import pytest

from kalshi_weather.markets.classifier import is_weather_market


# --- Weather market prefix tests ---


class TestIsWeatherMarket:
    def test_kxhigh_series(self):
        """KXHIGH series should be classified as weather."""
        raw = {"series_ticker": "KXHIGHNY", "ticker": "KXHIGHNY-26MAR07-B55"}
        assert is_weather_market(raw) is True

    def test_kxlow_series(self):
        """KXLOW series should be classified as weather."""
        raw = {"series_ticker": "KXLOWCHI", "ticker": "KXLOWCHI-26MAR07-U20"}
        assert is_weather_market(raw) is True

    def test_kxrain_series(self):
        """KXRAIN series should be classified as weather."""
        raw = {"series_ticker": "KXRAINHOU", "ticker": "KXRAINHOU-26MAR-B5"}
        assert is_weather_market(raw) is True

    def test_non_weather_series(self):
        """Non-weather series should be rejected."""
        raw = {"series_ticker": "PRES", "ticker": "PRES-26NOV04-R"}
        assert is_weather_market(raw) is False

    def test_crypto_series(self):
        """Crypto/finance series should be rejected."""
        raw = {"series_ticker": "BTC", "ticker": "BTC-26MAR07-B100K"}
        assert is_weather_market(raw) is False

    def test_empty_series_with_weather_ticker(self):
        """If series_ticker is empty, fall back to ticker prefix."""
        raw = {"series_ticker": "", "ticker": "KXHIGHNY-26MAR07-B55"}
        assert is_weather_market(raw) is True

    def test_empty_both(self):
        """Empty series_ticker and ticker should be rejected."""
        raw = {"series_ticker": "", "ticker": ""}
        assert is_weather_market(raw) is False

    def test_missing_series_ticker(self):
        """Missing series_ticker key should be handled gracefully."""
        raw = {"ticker": "KXHIGHNY-26MAR07-B55"}
        assert is_weather_market(raw) is True

    def test_missing_ticker(self):
        """Missing ticker key should be handled gracefully."""
        raw = {"series_ticker": "KXHIGHNY"}
        assert is_weather_market(raw) is True

    def test_kxhigh_various_cities(self):
        """KXHIGH with different city codes should all be weather."""
        cities = ["NY", "CHI", "MIA", "LAX", "DEN", "ATL", "DAL", "SEA", "HOU", "PHX"]
        for city in cities:
            raw = {"series_ticker": f"KXHIGH{city}", "ticker": f"KXHIGH{city}-26MAR07-B55"}
            assert is_weather_market(raw) is True, f"Expected weather for city {city}"

    def test_partial_prefix_not_weather(self):
        """Partial prefix like 'KX' alone should not match."""
        raw = {"series_ticker": "KX", "ticker": "KX-26MAR07-B55"}
        assert is_weather_market(raw) is False

    def test_case_sensitivity(self):
        """Prefixes should be case-sensitive (Kalshi uses uppercase)."""
        raw = {"series_ticker": "kxhighny", "ticker": "kxhighny-26MAR07-B55"}
        assert is_weather_market(raw) is False

    def test_political_market(self):
        """Political markets should not be classified as weather."""
        raw = {
            "series_ticker": "PRES",
            "ticker": "PRES-26NOV04-D",
            "title": "Will the candidate win the election?",
        }
        assert is_weather_market(raw) is False

    def test_sports_market(self):
        """Sports markets should not be classified as weather."""
        raw = {
            "series_ticker": "NFL",
            "ticker": "NFL-SUPERBOWL-KC",
            "title": "Will Kansas City win the Super Bowl?",
        }
        assert is_weather_market(raw) is False
