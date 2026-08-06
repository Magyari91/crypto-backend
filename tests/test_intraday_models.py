from datetime import datetime, timedelta, timezone

from app.intraday_models import (
    build_intraday_estimate,
    prepare_intraday_data,
    walk_forward_intraday_backtest,
)
from app.intraday_risk import build_intraday_risk_estimate


def synthetic_candles(hours: int = 3000, hourly_change: float = 0.0002):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    candles = []
    for offset in range(hours):
        open_price = price
        price *= 1 + hourly_change
        candles.append(
            {
                "timestamp": int((start + timedelta(hours=offset)).timestamp() * 1000),
                "open": open_price,
                "high": max(open_price, price) * 1.001,
                "low": min(open_price, price) * 0.999,
                "close": price,
                "volume": 1_000_000 * (1 + (offset % 24) / 100),
            }
        )
    return candles


def test_intraday_features_only_create_completed_targets():
    prepared = prepare_intraday_data(synthetic_candles(900), horizon_days=1)

    assert prepared.features_by_origin
    assert max(prepared.targets_by_origin) + 24 < len(prepared.candles)


def test_one_day_intraday_specialist_activates_on_stable_pattern():
    estimate = build_intraday_estimate(
        synthetic_candles(),
        horizon_days=1,
        direction_threshold=0.2,
        cache_key="synthetic",
    )

    assert estimate["data_resolution"] == "1h"
    assert estimate["active"] is True
    assert estimate["prediction_pct"] > 0.2
    assert estimate["training_samples"] >= 700
    assert estimate["holdout_directional_accuracy"] == 100.0


def test_intraday_walk_forward_contract():
    result = walk_forward_intraday_backtest(
        synthetic_candles(),
        horizon_days=1,
        direction_threshold=0.2,
        max_samples=12,
    )

    assert result["model"]["resolution"] == "1h"
    assert result["summary"]["samples"] == 12
    assert result["recent_results"][0]["forecast_at"] < result["recent_results"][0]["evaluated_at"]


def test_intraday_risk_requires_stability_across_time_blocks():
    estimate = build_intraday_risk_estimate(
        synthetic_candles(),
        horizon_days=1,
        cache_key="synthetic-risk",
    )

    assert estimate["target_coverage_pct"] == 80
    assert estimate["range_pct"] > 0
    assert estimate["stability_folds"] == 3
    assert estimate["active"] is False
