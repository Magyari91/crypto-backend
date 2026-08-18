from datetime import datetime, timedelta, timezone

from app.forecast import build_forecast
from app.specialist_models import SPECIALIST_REGISTRY


def synthetic_history(days: int = 280):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    prices = []
    volumes = []
    for offset in range(days):
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        price = 100 * (1.002 ** offset)
        prices.append([timestamp, price])
        volumes.append([timestamp, 1_000_000 * (1 + (offset % 13) / 20)])
    return prices, volumes


def test_registry_assigns_a_different_model_to_each_horizon():
    assert SPECIALIST_REGISTRY[1].candidates[0] == "huber"
    assert SPECIALIST_REGISTRY[7].candidates[0] == "gradient_boosting"
    assert SPECIALIST_REGISTRY[30].candidates[0] == "ridge"
    assert all(len(spec.candidates) == 3 for spec in SPECIALIST_REGISTRY.values())


def test_seven_day_specialist_activates_after_strict_holdout_validation():
    prices, volumes = synthetic_history()

    forecast = build_forecast(prices, horizon_days=7, volumes=volumes)

    assert forecast["specialist"]["available"] is True
    assert forecast["specialist"]["active"] is True
    assert forecast["specialist"]["training_samples"] == 212
    assert forecast["specialist"]["holdout_samples"] > 0
    assert forecast["specialist"]["validation_skill_pct"] > 0
    assert forecast["specialist"]["holdout_skill_pct"] > 0
    assert forecast["specialist"]["selected_model_key"] in {
        "gradient_boosting",
        "extra_trees",
        "huber",
    }
    assert sum(
        candidate["selected"]
        for candidate in forecast["specialist"]["validation_candidates"]
    ) == 1
    assert 0 < forecast["specialist"]["blend_weight"] <= 0.7
