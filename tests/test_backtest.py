from datetime import datetime, timedelta, timezone
from math import pi, sin

from app.backtest import evaluate_journal, walk_forward_backtest


def synthetic_prices(days: int = 180, daily_change: float = 0.003):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    values = []
    for offset in range(days):
        price *= 1 + daily_change
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        values.append([timestamp, price])
    return values


def test_walk_forward_backtest_uses_completed_horizons():
    result = walk_forward_backtest(synthetic_prices(), horizon_days=7)

    assert result["model"]["method"] == "walk_forward"
    assert result["summary"]["samples"] > 50
    assert result["summary"]["directional_accuracy"] > 60
    assert result["summary"]["active_directional_accuracy"] == 100.0
    assert result["summary"]["signal_coverage_pct"] > 50
    assert result["summary"]["beats_baseline"] is True
    assert len(result["recent_results"]) == 8
    assert result["recent_results"][0]["forecast_at"] < result["recent_results"][0]["evaluated_at"]


def test_future_price_change_does_not_change_earlier_prediction():
    prices = synthetic_prices()
    original = walk_forward_backtest(prices, horizon_days=7)
    changed = [row[:] for row in prices]
    changed[-1][1] *= 2
    shocked = walk_forward_backtest(changed, horizon_days=7)

    original_prior = original["recent_results"][1]
    shocked_prior = shocked["recent_results"][1]
    assert original_prior["forecast_at"] == shocked_prior["forecast_at"]
    assert original_prior["predicted_change_pct"] == shocked_prior["predicted_change_pct"]


def test_journal_evaluation_marks_completed_and_pending_records():
    records = [
        {
            "id": 1,
            "coin_id": "bitcoin",
            "symbol": "BTC",
            "horizon_days": 7,
            "model_version": "2.0.0",
            "generated_at": "2025-02-01T00:00:00+00:00",
            "due_at": "2025-02-08T00:00:00+00:00",
            "base_price": 100.0,
            "target_price": 103.0,
            "expected_change_pct": 3.0,
            "direction_key": "bullish",
            "confidence": 65,
            "indicators": {},
        },
        {
            "id": 2,
            "coin_id": "bitcoin",
            "symbol": "BTC",
            "horizon_days": 7,
            "model_version": "2.0.0",
            "generated_at": "2027-02-01T00:00:00+00:00",
            "due_at": "2027-02-08T00:00:00+00:00",
            "base_price": 100.0,
            "target_price": 103.0,
            "expected_change_pct": 3.0,
            "direction_key": "bullish",
            "confidence": 65,
            "indicators": {},
        },
    ]

    evaluated = evaluate_journal(records, synthetic_prices())

    assert evaluated[0]["status"] == "evaluated"
    assert evaluated[0]["actual_price"] is not None
    assert evaluated[1]["status"] == "pending"
    assert evaluated[1]["actual_price"] is None


def test_backtest_reports_reserved_challenger_metrics():
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    prices = []
    for offset in range(760):
        price *= 1 + 0.001 + sin(2 * pi * offset / 36) * 0.008
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        prices.append([timestamp, price])

    result = walk_forward_backtest(
        prices,
        horizon_days=7,
        max_samples=12,
        minimum_refit_days=60,
    )
    challenger = result["summary"]["probability"]["challenger"]

    assert challenger is not None
    assert challenger["samples"] == 12
    assert 0 <= challenger["brier_score"] <= 1
    assert challenger["model_usage"]
