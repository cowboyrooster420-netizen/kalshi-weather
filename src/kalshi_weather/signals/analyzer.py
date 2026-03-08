"""Edge calculation and Kelly criterion sizing."""

from __future__ import annotations

from datetime import datetime, timezone

from kalshi_weather.config import get_settings
from kalshi_weather.forecasting.base import ProbabilityEstimate
from kalshi_weather.markets.models import WeatherMarket
from kalshi_weather.signals.models import Signal


def compute_kelly(
    model_prob: float,
    market_prob: float,
    fraction: float = 0.50,
    confidence: float = 1.0,
) -> float:
    """Compute fractional Kelly criterion bet size.

    Kelly = (p * (odds + 1) - 1) / odds
    where odds = (1 - market_prob) / market_prob for YES bets
    and odds = market_prob / (1 - market_prob) for NO bets

    Args:
        model_prob: Our estimated probability
        market_prob: Market-implied probability
        fraction: Kelly fraction (0.50 = half-Kelly)
        confidence: Model confidence, further scales the Kelly size

    Returns:
        Recommended bet size as fraction of bankroll (0 if no edge)
    """
    edge = model_prob - market_prob

    if abs(edge) < 1e-6:
        return 0.0

    if edge > 0:
        # Bet YES: odds are payout ratio for YES
        if market_prob >= 0.999:
            return 0.0
        odds = (1.0 - market_prob) / market_prob
        p = model_prob
    else:
        # Bet NO: odds are payout ratio for NO
        if market_prob <= 0.001:
            return 0.0
        odds = market_prob / (1.0 - market_prob)
        p = 1.0 - model_prob

    # Full Kelly: f = (p * (odds + 1) - 1) / odds
    full_kelly = (p * (odds + 1) - 1) / odds

    if full_kelly <= 0:
        return 0.0

    # Apply fraction and confidence scaling, cap at 50% of bankroll
    return min(full_kelly * fraction * confidence, 0.50)


def generate_signal(
    market: WeatherMarket,
    estimate: ProbabilityEstimate,
    prior_market_ids: set[str] | None = None,
) -> Signal | None:
    """Generate a trading signal from a market and probability estimate.

    Returns None if the edge is below the minimum threshold.
    """
    settings = get_settings()

    model_prob = estimate.probability
    market_prob = market.market_prob
    edge = model_prob - market_prob

    # Skip fallback 50% predictions — the model had no real estimate
    if abs(model_prob - 0.5) < 1e-4:
        return None

    if estimate.lead_time_hours < settings.min_lead_time_hours:
        return None

    if abs(edge) < settings.min_edge:
        return None

    if estimate.confidence < settings.min_confidence:
        return None

    direction = "YES" if edge > 0 else "NO"

    if direction == "YES" and settings.no_only:
        return None

    if direction == "NO" and model_prob > settings.max_model_prob:
        return None

    if market_prob < settings.min_market_prob:
        return None

    if settings.first_signal_only and prior_market_ids and market.market_id in prior_market_ids:
        return None

    kelly = compute_kelly(
        model_prob=model_prob,
        market_prob=market_prob,
        fraction=settings.kelly_fraction,
        confidence=estimate.confidence,
    )

    if kelly < settings.min_kelly_bet:
        return None

    location = ""
    market_type = "unknown"
    if market.params:
        location = market.params.location or ""
        market_type = market.params.market_type.value

    return Signal(
        market_id=market.market_id,
        question=market.question,
        market_type=market_type,
        location=location,
        model_prob=round(model_prob, 4),
        market_prob=round(market_prob, 4),
        edge=round(edge, 4),
        kelly_fraction=round(kelly, 4),
        confidence=round(estimate.confidence, 4),
        direction=direction,
        lead_time_hours=round(estimate.lead_time_hours, 1),
        sources=estimate.sources_used,
        details=estimate.details,
        timestamp=datetime.now(timezone.utc),
    )
