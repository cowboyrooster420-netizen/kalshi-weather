"""Tests for signal analyzer (edge calculation + Kelly criterion)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from kalshi_weather.forecasting.base import ProbabilityEstimate
from kalshi_weather.markets.models import Comparison, MarketParams, MarketType, WeatherMarket
from kalshi_weather.signals.analyzer import compute_kelly, generate_signal


class TestComputeKelly:
    def test_positive_edge_yes(self):
        """Positive edge → bet YES, Kelly > 0."""
        k = compute_kelly(0.6, 0.4, fraction=0.25, confidence=1.0)
        assert k > 0

    def test_negative_edge_no(self):
        """Negative edge → bet NO, Kelly > 0."""
        k = compute_kelly(0.3, 0.5, fraction=0.25, confidence=1.0)
        assert k > 0

    def test_no_edge_zero(self):
        """No edge → Kelly is 0."""
        k = compute_kelly(0.5, 0.5)
        assert k == 0.0

    def test_quarter_kelly_smaller(self):
        """Quarter Kelly should be smaller than full Kelly."""
        full = compute_kelly(0.7, 0.4, fraction=1.0, confidence=1.0)
        quarter = compute_kelly(0.7, 0.4, fraction=0.25, confidence=1.0)
        # Quarter Kelly is either 25% of full Kelly or capped at 0.25
        assert quarter <= full
        assert quarter <= 0.25

    def test_confidence_scaling(self):
        """Lower confidence should reduce Kelly."""
        high_conf = compute_kelly(0.7, 0.4, fraction=0.25, confidence=1.0)
        low_conf = compute_kelly(0.7, 0.4, fraction=0.25, confidence=0.5)
        assert abs(low_conf - high_conf * 0.5) < 0.001

    def test_extreme_market_prob_yes(self):
        """Kelly should be 0 when market_prob >= 0.999."""
        k = compute_kelly(1.0, 0.999, fraction=0.25, confidence=1.0)
        assert k == 0.0

    def test_extreme_market_prob_no(self):
        """Kelly should be 0 when market_prob <= 0.001."""
        k = compute_kelly(0.0, 0.001, fraction=0.25, confidence=1.0)
        assert k == 0.0

    def test_kelly_never_negative(self):
        """Kelly should never be negative."""
        for model_p in [0.1, 0.3, 0.5, 0.7, 0.9]:
            for market_p in [0.1, 0.3, 0.5, 0.7, 0.9]:
                k = compute_kelly(model_p, market_p)
                assert k >= 0, f"Negative Kelly: model={model_p}, market={market_p}, k={k}"

    def test_small_edge_small_kelly(self):
        """Small edge should produce small Kelly."""
        k = compute_kelly(0.51, 0.50, fraction=0.25, confidence=1.0)
        assert k < 0.01  # Very small


class TestGenerateSignal:
    def test_generates_yes_signal(self, sample_weather_market, sample_estimate):
        """Should generate YES signal when model_prob > market_prob."""
        with _mock_settings(no_only=False):
            signal = generate_signal(sample_weather_market, sample_estimate)

        assert signal is not None
        assert signal.direction == "YES"
        assert signal.edge > 0
        assert signal.kelly_fraction > 0
        assert signal.market_id == "test-market-001"

    def test_generates_no_signal(self):
        """Should generate NO signal when model_prob < market_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.10, raw_probability=0.10,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings():
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "NO"
        assert signal.edge < 0

    def test_no_signal_below_threshold(self):
        """Should return None when edge is below min_edge (10%)."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.50, outcome_no_price=0.50,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.52, raw_probability=0.52,  # Only 2% edge
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings():
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_signal_at_exact_threshold(self):
        """Edge exactly at threshold should not generate signal."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.45, outcome_no_price=0.55,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.50, raw_probability=0.50,  # Exactly 5% edge
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings():
            signal = generate_signal(market, estimate)

        # 0.05 < 0.10, so below min_edge → no signal
        assert signal is None

    def test_no_signal_below_min_confidence(self):
        """Should return None when confidence is below min_confidence (0.30)."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.30, outcome_no_price=0.70,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.60, raw_probability=0.60,  # 30% edge, but low confidence
            confidence=0.20, lead_time_hours=24,
        )

        with _mock_settings():
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_signal_fields_populated(self, sample_weather_market, sample_estimate):
        """All signal fields should be properly populated."""
        with _mock_settings(no_only=False):
            signal = generate_signal(sample_weather_market, sample_estimate)

        assert signal is not None
        assert signal.market_type == "temperature"
        assert signal.location == "Phoenix, AZ"
        assert signal.confidence > 0
        assert signal.lead_time_hours == 24.0
        assert len(signal.sources) > 0
        assert signal.timestamp is not None

    def test_signal_no_params(self):
        """Market without params should still generate signal."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.30, outcome_no_price=0.70,
            params=None,  # No parsed params
        )
        estimate = ProbabilityEstimate(
            probability=0.60, raw_probability=0.60,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(no_only=False):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.location == ""
        assert signal.market_type == "unknown"


def _mock_settings(**overrides):
    """Return a patched get_settings context manager with sensible defaults."""
    defaults = dict(
        no_only=False,
        min_edge=0.10,
        min_confidence=0.30,
        min_lead_time_hours=12.0,
        min_market_prob=0.0,
        first_signal_only=False,
        kelly_fraction=0.25,
        max_model_prob=0.50,
        min_kelly_bet=0.0,
    )
    defaults.update(overrides)
    p = patch("kalshi_weather.signals.analyzer.get_settings")

    class _Ctx:
        def __enter__(self_inner):
            mock = p.__enter__()
            for k, v in defaults.items():
                setattr(mock.return_value, k, v)
            return mock

        def __exit__(self_inner, *args):
            return p.__exit__(*args)

    return _Ctx()


class TestNoOnlyFilter:
    """Tests for the no_only config flag."""

    def test_yes_signal_suppressed_when_no_only(self):
        """YES direction should be suppressed when no_only=True."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.35, outcome_no_price=0.65,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.60, raw_probability=0.60,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(no_only=True):
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_yes_signal_allowed_when_no_only_false(self):
        """YES direction should be allowed when no_only=False."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.35, outcome_no_price=0.65,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.60, raw_probability=0.60,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(no_only=False):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "YES"

    def test_no_signal_unaffected_by_no_only(self):
        """NO direction should pass regardless of no_only setting."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(no_only=True):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "NO"


class TestMinMarketProbFilter:
    """Tests for the min_market_prob config flag."""

    def test_signal_suppressed_below_min_market_prob(self):
        """Signal should be suppressed when market_prob < min_market_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.20, outcome_no_price=0.80,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.05, raw_probability=0.05,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(min_market_prob=0.30):
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_signal_allowed_at_min_market_prob(self):
        """Signal should pass when market_prob == min_market_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.30, outcome_no_price=0.70,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.10, raw_probability=0.10,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(min_market_prob=0.30):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "NO"

    def test_signal_allowed_above_min_market_prob(self):
        """Signal should pass when market_prob > min_market_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(min_market_prob=0.30):
            signal = generate_signal(market, estimate)

        assert signal is not None


class TestFirstSignalOnlyFilter:
    """Tests for the first_signal_only config flag."""

    def test_signal_suppressed_when_market_in_prior_ids(self):
        """Should suppress signal when market already has a prior signal."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(first_signal_only=True):
            signal = generate_signal(market, estimate, prior_market_ids={"m1"})

        assert signal is None

    def test_signal_allowed_when_market_not_in_prior_ids(self):
        """Should allow signal when market has no prior signal."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(first_signal_only=True):
            signal = generate_signal(market, estimate, prior_market_ids={"m2", "m3"})

        assert signal is not None
        assert signal.direction == "NO"

    def test_signal_allowed_when_first_signal_only_disabled(self):
        """Should allow signal even if market is in prior_ids when disabled."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(first_signal_only=False):
            signal = generate_signal(market, estimate, prior_market_ids={"m1"})

        assert signal is not None

    def test_signal_allowed_when_prior_ids_empty(self):
        """Should allow signal when prior_market_ids is empty set."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(first_signal_only=True):
            signal = generate_signal(market, estimate, prior_market_ids=set())

        assert signal is not None


class TestMaxModelProbFilter:
    """Tests for the max_model_prob config (NO-direction only)."""

    def test_no_signal_suppressed_above_max_model_prob(self):
        """NO signal should be suppressed when model_prob > max_model_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.40, raw_probability=0.40,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(max_model_prob=0.20):
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_no_signal_allowed_at_max_model_prob(self):
        """NO signal should pass when model_prob == max_model_prob (boundary)."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.20, raw_probability=0.20,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(max_model_prob=0.20):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "NO"

    def test_no_signal_allowed_below_max_model_prob(self):
        """NO signal should pass when model_prob < max_model_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.10, raw_probability=0.10,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(max_model_prob=0.20):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "NO"

    def test_yes_signal_unaffected_by_max_model_prob(self):
        """YES signal should not be filtered by max_model_prob."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.35, outcome_no_price=0.65,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.60, raw_probability=0.60,
            confidence=0.8, lead_time_hours=24,
        )

        with _mock_settings(no_only=False, max_model_prob=0.20):
            signal = generate_signal(market, estimate)

        assert signal is not None
        assert signal.direction == "YES"


class TestMinKellyBetFilter:
    """Tests for the min_kelly_bet config."""

    def test_signal_suppressed_below_min_kelly_bet(self):
        """Signal should be suppressed when computed kelly < min_kelly_bet."""
        # Small edge → small kelly
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.15, raw_probability=0.15,
            confidence=0.8, lead_time_hours=24,
        )

        # Use a high min_kelly_bet to ensure filtering
        with _mock_settings(min_kelly_bet=0.50):
            signal = generate_signal(market, estimate)

        assert signal is None

    def test_signal_allowed_above_min_kelly_bet(self):
        """Signal should pass when computed kelly > min_kelly_bet."""
        # Large edge → large kelly
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.05, raw_probability=0.05,
            confidence=0.9, lead_time_hours=24,
        )

        with _mock_settings(min_kelly_bet=0.05):
            signal = generate_signal(market, estimate)

        assert signal is not None

    def test_signal_allowed_at_exact_min_kelly_bet(self):
        """Signal should pass when computed kelly == min_kelly_bet (boundary)."""
        market = WeatherMarket(
            market_id="m1", event_ticker="", series_ticker="",
            question="Q", description="",
            outcome_yes_price=0.70, outcome_no_price=0.30,
            params=MarketParams(market_type=MarketType.TEMPERATURE, location="X"),
        )
        estimate = ProbabilityEstimate(
            probability=0.05, raw_probability=0.05,
            confidence=0.9, lead_time_hours=24,
        )

        # Compute the exact kelly this would produce, then set min_kelly_bet to it
        kelly = compute_kelly(0.05, 0.70, fraction=0.25, confidence=0.9)

        with _mock_settings(min_kelly_bet=kelly):
            signal = generate_signal(market, estimate)

        # kelly == min_kelly_bet → 'kelly < min_kelly_bet' is False → passes
        assert signal is not None
