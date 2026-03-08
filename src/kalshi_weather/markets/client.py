"""Kalshi REST API client (read-only market data)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from kalshi_weather.common.http import HttpClient
from kalshi_weather.config import get_settings

logger = logging.getLogger(__name__)


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


async def fetch_weather_markets(
    status: str = "open",
) -> list[dict]:
    """Fetch all active weather markets from the Kalshi API.

    Iterates over configured weather series prefixes and paginates
    through each one.  Returns raw market dicts.
    """
    settings = get_settings()
    all_markets: list[dict] = []

    async with HttpClient(base_url=settings.kalshi_api_url) as client:
        for series_prefix in settings.kalshi_weather_series:
            cursor: str | None = None
            while True:
                params: dict = {
                    "series_ticker": series_prefix,
                    "status": status,
                    "limit": 200,
                }
                if cursor:
                    params["cursor"] = cursor

                try:
                    resp = await client.get("/markets", params=params)
                except httpx.HTTPStatusError as exc:
                    logger.warning(
                        "Kalshi API HTTP %d for series %s",
                        exc.response.status_code,
                        series_prefix,
                    )
                    break

                data = resp.json()
                markets = data.get("markets", [])
                if not markets:
                    break

                all_markets.extend(markets)

                cursor = data.get("cursor")
                if not cursor:
                    break

                await asyncio.sleep(0.25)

    logger.info("Fetched %d weather markets from Kalshi", len(all_markets))
    return all_markets


async def fetch_market_by_ticker(ticker: str) -> dict | None:
    """Fetch a single market by ticker.

    Returns the raw market dict, or None on error.
    """
    settings = get_settings()
    try:
        async with HttpClient(base_url=settings.kalshi_api_url) as client:
            resp = await client.get(f"/markets/{ticker}")
            data = resp.json()
            return data.get("market", data)
    except Exception:
        logger.warning("Failed to fetch market %s", ticker, exc_info=True)
        return None


def raw_to_weather_market(raw: dict) -> "WeatherMarket":
    """Convert a raw Kalshi API market dict to a WeatherMarket.

    Kalshi prices are in cents (1-99); we normalize to 0-1.
    """
    from kalshi_weather.markets.models import WeatherMarket

    ticker = raw.get("ticker", "")
    yes_price = raw.get("yes_price", 50)
    no_price = raw.get("no_price", 50)

    # Kalshi prices are in cents; normalize to 0-1
    if isinstance(yes_price, (int, float)) and yes_price > 1:
        yes_price = yes_price / 100.0
        no_price = no_price / 100.0

    # Clamp to valid range
    yes_price = max(0.0, min(1.0, float(yes_price)))
    no_price = max(0.0, min(1.0, float(no_price)))

    # Parse close time
    close_time = _parse_iso(
        raw.get("close_time")
        or raw.get("expected_expiration_time")
        or raw.get("expiration_time")
    )

    return WeatherMarket(
        market_id=ticker,
        event_ticker=raw.get("event_ticker", ""),
        series_ticker=raw.get("series_ticker", ""),
        question=raw.get("title", raw.get("subtitle", "")),
        description=raw.get("subtitle", ""),
        outcome_yes_price=yes_price,
        outcome_no_price=no_price,
        end_date=close_time,
        volume=float(raw.get("volume", 0) or 0),
        active=raw.get("status", "") in ("open", "active"),
    )
