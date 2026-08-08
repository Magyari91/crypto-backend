from app.forecast_store import ForecastStore


class RecordingConnection:
    def __init__(self):
        self.statement = ""
        self.parameters = ()

    def execute(self, statement, parameters):
        self.statement = statement
        self.parameters = parameters
        return self


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


def test_store_persists_point_in_time_feature_snapshots(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    created = store.record_feature_snapshot(
        coin_id="bitcoin",
        symbol="BTC",
        generated_at="2026-07-10T12:01:00+00:00",
        horizon_days=7,
        market={"current_price": 100.0},
        technical={"rsi": 52.0},
        derivatives={"available": True, "funding_rate_pct": 0.01},
        news_sentiment={"score": 0.25, "sample_size": 4},
        model={"model_version": "5.0.0", "probability": {"active": False}},
    )
    duplicate = store.record_feature_snapshot(
        coin_id="bitcoin",
        symbol="BTC",
        generated_at="2026-07-10T12:12:00+00:00",
        horizon_days=7,
        market={"current_price": 101.0},
        technical={"rsi": 53.0},
        derivatives={"available": True},
        news_sentiment={"score": 0.3},
        model={"model_version": "5.0.0"},
    )

    status = store.feature_status("bitcoin", 7)
    assert created is True
    assert duplicate is False
    assert status["sample_count"] == 1
    assert status["feature_version"] == "1.0.0"
    assert status["latest_derivatives"]["funding_rate_pct"] == 0.01
    assert status["latest_model"]["model_version"] == "5.0.0"


def test_store_reports_sqlite_as_local_fallback(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")

    assert store.storage_status() == {"backend": "sqlite", "persistent": False}


def test_store_accepts_explicit_postgresql_url_and_converts_placeholders():
    store = ForecastStore("unused.sqlite3", "postgresql://user:secret@db/forecast")
    connection = RecordingConnection()

    store._execute(connection, "SELECT * FROM sample WHERE coin = ?", ("bitcoin",))

    assert store.storage_status() == {
        "backend": "postgresql",
        "persistent": True,
    }
    assert connection.statement == "SELECT * FROM sample WHERE coin = %s"
    assert connection.parameters == ("bitcoin",)


def test_store_rejects_non_postgresql_database_url(tmp_path):
    try:
        ForecastStore(tmp_path / "forecast.sqlite3", "sqlite:///other.sqlite3")
    except ValueError as exc:
        assert "PostgreSQL URL" in str(exc)
    else:
        raise AssertionError("A non-PostgreSQL URL must be rejected")
