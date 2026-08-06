from datetime import datetime, timedelta, timezone

from app.forecast import build_forecast


def synthetic_prices(days: int = 120, daily_change: float = 0.002):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    values = []
    for offset in range(days):
        price *= 1 + daily_change
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        values.append([timestamp, price])
    return values


def test_forecast_contract_for_rising_market():
    forecast = build_forecast(synthetic_prices(), horizon_days=7)

    assert forecast["direction_key"] == "bullish"
    assert forecast["expected_change_pct"] > 0
    assert 42 <= forecast["confidence"] <= 82
    assert forecast["support"] < forecast["target_price"]
    assert len(forecast["series"]) == 60
    assert forecast["indicators"]["rsi"] is not None
    assert forecast["base_price"] > 0
    assert forecast["model_version"] == "5.0.0"
    assert forecast["regime"]["key"] == "trend"
    assert forecast["prediction_interval"]["lower_price"] <= forecast["target_price"]
    assert forecast["prediction_interval"]["upper_price"] >= forecast["target_price"]
    assert forecast["ensemble"]["holdout_samples"] > 0
    assert forecast["specialist"]["family"] == "Huber Gradient Boosting"
    probability = forecast["probability_forecast"]
    assert probability["event"]["target_return_pct"] == 1.0
    assert 0 <= probability["probability_pct"] <= 100
    assert probability["decision"]["manual_confirmation_required"] is True
    assert probability["active"] is False
    assert probability["probability_pct"] == probability["baseline_probability_pct"]


def test_forecast_is_bounded():
    forecast = build_forecast(synthetic_prices(daily_change=0.05), horizon_days=1)

    assert -2.5 <= forecast["expected_change_pct"] <= 2.5
    assert forecast["horizon_days"] == 1
