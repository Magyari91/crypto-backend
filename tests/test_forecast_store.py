from datetime import datetime, timezone

from app.forecast_store import ForecastStore
from app.training_readiness import build_training_readiness


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


def test_store_settles_matured_feature_snapshots_once(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()
    store.record_feature_snapshot(
        coin_id="bitcoin",
        symbol="BTC",
        generated_at="2026-07-10T12:01:00+00:00",
        horizon_days=1,
        market={"current_price": 100.0},
        technical={"rsi": 52.0},
        derivatives={"available": True},
        news_sentiment={"score": 0.1},
        model={
            "model_version": "5.0.0",
            "probability": {"event": {"target_return_pct": 1.0}},
        },
    )

    early = store.settle_due_feature_snapshots(
        "bitcoin",
        1,
        "2026-07-11T11:59:00+00:00",
        102.0,
    )
    overdue = store.feature_status(
        "bitcoin",
        1,
        now=datetime(2026, 7, 11, 12, 10, tzinfo=timezone.utc),
    )
    settled = store.settle_due_feature_snapshots(
        "bitcoin",
        1,
        "2026-07-11T12:16:00+00:00",
        102.0,
    )
    duplicate = store.settle_due_feature_snapshots(
        "bitcoin",
        1,
        "2026-07-11T12:30:00+00:00",
        103.0,
    )
    status = store.feature_status("bitcoin", 1)

    assert early == 0
    assert overdue["overdue_sample_count"] == 1
    assert overdue["next_due_at"] == "2026-07-11T12:01:00+00:00"
    assert settled == 1
    assert duplicate == 0
    assert status["sample_count"] == 1
    assert status["labeled_sample_count"] == 1
    assert status["pending_sample_count"] == 0
    assert status["independent_labeled_days"] == 1
    assert status["label_coverage_pct"] == 100.0
    assert status["latest_outcome"]["realized_return_pct"] == 2.0
    assert status["latest_outcome"]["label_lag_minutes"] == 15.0
    assert status["latest_outcome"]["event_happened"] is True


def test_training_readiness_requires_persistent_storage(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    readiness = build_training_readiness(
        store.feature_status("bitcoin", 7),
        store.storage_status(),
        7,
    )

    assert readiness["status"] == "storage_required"
    assert readiness["ready_for_training"] is False
    assert readiness["minimum_independent_labels"] == 360
    assert readiness["candidate_minimums"] == {
        "probability": 360,
        "specialist": 140,
    }


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
