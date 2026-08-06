from app.forecast_store import ForecastStore


def sample_forecast(horizon_days: int = 7):
    return {
        "horizon_days": horizon_days,
        "model_version": "2.0.0",
        "base_price": 100.0,
        "target_price": 104.0,
        "expected_change_pct": 4.0,
        "direction_key": "bullish",
        "confidence": 68,
        "indicators": {"rsi": 55.2},
    }


def test_store_deduplicates_forecasts_inside_time_bucket(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    first = store.record(
        "bitcoin",
        "BTC",
        "2026-07-10T12:01:00+00:00",
        sample_forecast(),
    )
    duplicate = store.record(
        "bitcoin",
        "BTC",
        "2026-07-10T12:12:00+00:00",
        sample_forecast(),
    )

    assert first is True
    assert duplicate is False
    assert len(store.recent("bitcoin", 7)) == 1


def test_store_keeps_separate_horizons_and_buckets(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    store.record("bitcoin", "BTC", "2026-07-10T12:01:00+00:00", sample_forecast(7))
    store.record("bitcoin", "BTC", "2026-07-10T12:16:00+00:00", sample_forecast(7))
    store.record("bitcoin", "BTC", "2026-07-10T12:01:00+00:00", sample_forecast(30))

    seven_day_records = store.recent("bitcoin", 7)
    assert len(seven_day_records) == 2
    assert seven_day_records[0]["generated_at"] > seven_day_records[1]["generated_at"]
    assert len(store.recent("bitcoin", 30)) == 1


def test_store_persists_probability_audit_fields(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()
    forecast = sample_forecast()
    forecast["probability_forecast"] = {
        "probability_pct": 63.5,
        "baseline_probability_pct": 47.0,
        "active": True,
        "event": {"target_return_pct": 1.0},
        "decision": {"key": "buy_candidate"},
    }

    store.record("bitcoin", "BTC", "2026-07-10T12:01:00+00:00", forecast)
    record = store.recent("bitcoin", 7)[0]

    assert record["event_probability_pct"] == 63.5
    assert record["baseline_probability_pct"] == 47.0
    assert record["event_target_return_pct"] == 1.0
    assert record["probability_model_active"] is True
    assert record["probability_decision"] == "buy_candidate"
