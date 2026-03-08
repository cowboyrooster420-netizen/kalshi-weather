"""Weather Underground airport history page scraper.

Scrapes daily high/low temperatures from WU's airport history pages
using Playwright (headless Chromium). These pages are Angular SPAs —
temperature data is loaded via JavaScript, not embedded in static HTML.

Polymarket resolves on these airport history pages, NOT the PWS
dashboard pages.

Target URL pattern:
    https://www.wunderground.com/history/daily/{history_path}/date/{YYYY-M-D}
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta

from kalshi_weather.common.types import fahrenheit_to_celsius
from kalshi_weather.weather.stations import Station

logger = logging.getLogger(__name__)

_WU_BASE = "https://www.wunderground.com"


@dataclass
class WUDailyObs:
    """Daily high/low observation from a WU airport history page."""

    station_id: str
    date: date
    high_temp_c: float
    low_temp_c: float


_MAX_RETRIES = 2


async def fetch_airport_daily(
    station: Station,
    target_date: date,
    browser,
) -> WUDailyObs | None:
    """Fetch a single day's high/low from a WU airport history page.

    Uses a Playwright browser instance to render the Angular SPA and
    extract temperature data from the DOM. Retries up to _MAX_RETRIES
    times on transient failures.

    Args:
        station: Station with icao and history_path.
        target_date: The date to fetch.
        browser: A Playwright Browser instance (reused across calls).

    Returns:
        WUDailyObs with temps in Celsius, or None if the page can't
        be fetched or parsed.
    """
    date_str = f"{target_date.year}-{target_date.month}-{target_date.day}"
    url = f"{_WU_BASE}/history/daily/{station.history_path}/date/{date_str}"

    for attempt in range(_MAX_RETRIES + 1):
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # Wait for the Angular daily summary table rows to render.
            try:
                await page.wait_for_selector(
                    "lib-city-history-summary table tbody tr",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "Timeout waiting for summary table for %s on %s (attempt %d)",
                    station.icao, target_date, attempt + 1,
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(1.0)
                    continue
                return None

            # Extract High/Low temps from the daily summary table.
            # Summary rows use <th> headers ("High Temp", "Low Temp")
            # with the "Actual" value in the first <td>.
            high_f = await _extract_summary_temp(page, "High Temp")
            low_f = await _extract_summary_temp(page, "Low Temp")

            if high_f is None or low_f is None:
                # Fallback: grab first two values from the summary table.
                high_f, low_f = await _extract_table_temps(page)

            if high_f is None or low_f is None:
                logger.warning(
                    "Could not parse temps for %s on %s",
                    station.icao, target_date,
                )
                return None

            return WUDailyObs(
                station_id=station.icao,
                date=target_date,
                high_temp_c=fahrenheit_to_celsius(high_f),
                low_temp_c=fahrenheit_to_celsius(low_f),
            )
        except Exception as exc:
            logger.warning(
                "Failed to fetch airport page for %s on %s (attempt %d): %s",
                station.icao, target_date, attempt + 1, exc,
            )
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(1.0)
                continue
            return None
        finally:
            await page.close()

    return None  # unreachable, but satisfies type checker


async def _extract_summary_temp(page, label: str) -> float | None:
    """Extract a temperature value from the daily summary table row.

    The WU summary table uses ``<th>`` for row headers (e.g. "High Temp",
    "Low Temp") and ``<td>`` elements for values. The first ``<td>`` in
    each row is the "Actual" value.
    """
    try:
        rows = await page.query_selector_all(
            "lib-city-history-summary table tbody tr"
        )
        for row in rows:
            header = await row.query_selector("th")
            if header is None:
                continue
            text = (await header.inner_text()).strip()
            if label.lower() not in text.lower():
                continue
            # The "Actual" value is the first <td> in the row.
            value_cell = await row.query_selector("td")
            if value_cell is None:
                continue
            val_text = await value_cell.inner_text()
            return _parse_temp_text(val_text)
    except Exception as exc:
        logger.debug("Error extracting %s temp from summary table: %s", label, exc)
    return None


async def _extract_table_temps(page) -> tuple[float | None, float | None]:
    """Fallback: extract high/low from the first two data rows of the summary.

    The summary table's first tbody row is "High Temp" and the second
    is "Low Temp". Each row's first <td> contains the "Actual" value.
    Values are filtered to a plausible Fahrenheit range (-60 to 140).
    """
    try:
        rows = await page.query_selector_all(
            "lib-city-history-summary table tbody tr"
        )
        temps: list[float] = []
        for row in rows[:3]:  # Only first 3 rows (High, Low, Avg)
            cell = await row.query_selector("td")
            if cell is None:
                continue
            text = await cell.inner_text()
            val = _parse_temp_text(text)
            if val is not None and -60.0 <= val <= 140.0:
                temps.append(val)

        if len(temps) >= 2:
            return max(temps), min(temps)
    except Exception as exc:
        logger.debug("Error extracting temps from table fallback: %s", exc)
    return None, None


def _parse_temp_text(text: str) -> float | None:
    """Parse a temperature string like '72', '72°', '-5' to a float."""
    text = text.strip().replace("°", "").replace(",", "")
    match = re.search(r"-?\d+\.?\d*", text)
    if match:
        try:
            return float(match.group())
        except ValueError:
            pass
    return None


async def fetch_airport_history(
    station: Station,
    start: date,
    end: date,
    *,
    max_concurrent: int = 5,
) -> list[WUDailyObs]:
    """Fetch daily observations for a date range using Playwright.

    Launches a single headless Chromium browser and creates concurrent
    page tabs (bounded by semaphore) to fetch each day's data.

    Args:
        station: Station with icao and history_path.
        start: Start date (inclusive).
        end: End date (inclusive).
        max_concurrent: Max parallel browser tabs.

    Returns:
        List of successfully parsed observations sorted by date (may be
        shorter than the date range if some days fail to parse).
    """
    from playwright.async_api import async_playwright

    semaphore = asyncio.Semaphore(max_concurrent)

    # Build list of all dates
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            async def _fetch_one(target_date: date) -> WUDailyObs | None:
                async with semaphore:
                    await asyncio.sleep(0.5)  # Rate-limit to avoid WU blocking
                    return await fetch_airport_daily(station, target_date, browser)

            results = await asyncio.gather(*[_fetch_one(d) for d in days])
        finally:
            await browser.close()

    observations = [obs for obs in results if obs is not None]
    observations.sort(key=lambda o: o.date)

    logger.info(
        "Fetched %d/%d days for %s (%s to %s)",
        len(observations), (end - start).days + 1,
        station.icao, start, end,
    )
    return observations
