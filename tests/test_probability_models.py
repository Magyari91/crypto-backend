from math import pi, sin

from app.probability_models import (
    _purged_blocks,
    build_probability_feature_vector,
    prepare_probability_data,
    probability_from_state,
    train_probability_model,
)


def cyclical_market(days: int = 900):
    price = 100.0
    market_price = 120.0
    prices = []
    market = []
    volumes = []
    for index in range(days):
        cycle = sin(2 * pi * index / 42)
        slower_cycle = sin(2 * pi * index / 130)
        price *= 1 + 0.001 + cycle * 0.0045 + slower_cycle * 0.0015
        market_price *= 1 + 0.0008 + cycle * 0.0032
        prices.append(price)
        market.append(market_price)
        volumes.append(1_000_000 * (1.2 + abs(cycle) * 0.8))
    return prices, volumes, market


def test_probability_split_purges_the_full_horizon():
    training, calibration, validation, holdout = _purged_blocks(
        list(range(700)),
        horizon_days=30,
    )

    assert max(training) + 30 < min(calibration)
    assert max(calibration) + 30 < min(validation)
    assert max(validation) + 30 < min(holdout)


def test_probability_model_reports_calibrated_holdout_metrics():
    prices, volumes, market = cyclical_market()
    prepared = prepare_probability_data(prices, volumes, market, horizon_days=7)
    state = train_probability_model(
        prepared,
        known_through_origin=len(prices) - 8,
    )
    current_features = build_probability_feature_vector(prices, volumes, market)
    estimate = probability_from_state(state, current_features)

    assert state.available is True
    assert state.candidate_key in {"logistic", "hist_gradient_boosting"}
    assert set(state.validation_candidates) == {"logistic", "hist_gradient_boosting"}
    assert state.holdout_samples >= 24
    assert 0 <= estimate["probability"] <= 1
    assert 0 <= estimate["baseline_probability"] <= 1
    assert sum(item["samples"] for item in state.reliability_bins) == state.holdout_samples
    if not state.active:
        assert estimate["probability"] == estimate["baseline_probability"]


def test_single_class_history_keeps_probability_model_in_reserve():
    prices = [100 * (1.003**index) for index in range(700)]
    volumes = [1_000_000.0] * len(prices)
    prepared = prepare_probability_data(prices, volumes, prices, horizon_days=7)
    state = train_probability_model(
        prepared,
        known_through_origin=len(prices) - 8,
    )

    assert state.available is False
    assert state.active is False
    assert state.estimator is None
    assert "egy osztályt" in state.reason
