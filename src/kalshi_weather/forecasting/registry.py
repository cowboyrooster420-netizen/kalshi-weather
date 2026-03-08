"""MarketType → ForecastModel dispatch registry."""

from __future__ import annotations

from kalshi_weather.forecasting.base import ForecastModel
from kalshi_weather.forecasting.hurricane import HurricaneModel
from kalshi_weather.forecasting.precipitation import PrecipitationModel
from kalshi_weather.forecasting.temperature import TemperatureModel
from kalshi_weather.markets.models import MarketType

_REGISTRY: dict[MarketType, ForecastModel] = {
    MarketType.TEMPERATURE: TemperatureModel(),
    MarketType.PRECIPITATION: PrecipitationModel(),
    MarketType.HURRICANE: HurricaneModel(),
}


def get_model(market_type: MarketType) -> ForecastModel | None:
    """Get the forecast model for a given market type.

    Returns None for UNKNOWN market types.
    """
    return _REGISTRY.get(market_type)
