"""Telegram Bot API notifications for high-edge signals and run summaries."""

from __future__ import annotations

import logging

from kalshi_weather.common.http import HttpClient
from kalshi_weather.config import get_settings
from kalshi_weather.signals.formatters import format_telegram_signal, format_telegram_summary
from kalshi_weather.signals.models import Signal
from kalshi_weather.signals.tracker import SignalTracker

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Send trading signal alerts via Telegram Bot API.

    All errors are logged but never raised — notifications must not break
    the pipeline.
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        settings = get_settings()
        self._bot_token = bot_token or settings.telegram_bot_token
        self._chat_id = chat_id or settings.telegram_chat_id
        self._client: HttpClient | None = None

    @property
    def _enabled(self) -> bool:
        return bool(self._bot_token and self._chat_id)

    async def _get_client(self) -> HttpClient:
        if self._client is None:
            self._client = HttpClient(base_url="https://api.telegram.org")
        return self._client

    async def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Send a message via the Telegram Bot API.

        Returns True if the message was sent successfully, False otherwise.
        """
        if not self._enabled:
            logger.debug("Telegram not configured, skipping message")
            return False

        try:
            client = await self._get_client()
            await client.post(
                f"/bot{self._bot_token}/sendMessage",
                json={
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                },
            )
            return True
        except Exception:
            logger.warning("Failed to send Telegram message", exc_info=True)
            return False

    async def notify_signal(
        self,
        signal: Signal,
        signal_number: int | None = None,
        first_edge: float | None = None,
    ) -> bool:
        """Format and send a single high-edge signal alert."""
        text = format_telegram_signal(signal, signal_number=signal_number, first_edge=first_edge)
        return await self.send_message(text)

    async def notify_summary(self, signals: list[Signal]) -> bool:
        """Format and send a run summary."""
        text = format_telegram_summary(signals)
        return await self.send_message(text)

    async def notify(
        self,
        signals: list[Signal],
        high_edge_threshold: float | None = None,
    ) -> None:
        """Send individual alerts for every signal, sorted by edge size.

        Args:
            signals: All signals from the pipeline run.
            high_edge_threshold: Unused, kept for API compatibility.
        """
        if not self._enabled:
            return

        # Look up prior signal counts so we can tag NEW vs UPDATE
        prior: dict[str, tuple[int, float]] = {}
        try:
            tracker = SignalTracker()
            prior = await tracker.get_prior_signals_summary(
                [s.market_id for s in signals],
            )
            await tracker.close()
        except Exception:
            logger.warning("Failed to fetch prior signals for NEW/UPDATE tagging", exc_info=True)

        # Send individual alerts for every signal, sorted by edge descending
        sorted_signals = sorted(
            signals,
            key=lambda s: abs(s.edge),
            reverse=True,
        )
        for signal in sorted_signals:
            count, first_edge = prior.get(signal.market_id, (None, None))
            await self.notify_signal(signal, signal_number=count, first_edge=first_edge)

        if self._client is not None:
            await self._client.close()
            self._client = None

        logger.info(
            "Telegram: sent %d signal alert(s)",
            len(sorted_signals),
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
