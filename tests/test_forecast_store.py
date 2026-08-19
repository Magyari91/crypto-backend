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


def test_store_reports_data_pipeline_health_and_storage_usage(tmp_path):
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
        model={"model_version": "5.1.0"},
    )
    assert store.settle_due_feature_snapshots(
        "bitcoin",
        1,
        "2026-07-11T12:16:00+00:00",
        102.0,
    ) == 1
    store.record_feature_snapshot(
        coin_id="ethereum",
        symbol="ETH",
        generated_at="2026-07-11T12:10:00+00:00",
        horizon_days=7,
        market={"current_price": 200.0},
        technical={"rsi": 48.0},
        derivatives={"available": True},
        news_sentiment={"score": 0.0},
        model={"model_version": "5.1.0"},
    )

    health = store.data_health(
        now=datetime(2026, 7, 11, 12, 20, tzinfo=timezone.utc),
        storage_limit_mb=1,
        stale_after_minutes=45,
    )

    assert health["status"] == "storage_required"
    assert health["collector"]["status"] == "healthy"
    assert health["collector"]["latest_snapshot_age_minutes"] == 10.0
    assert health["storage"]["database_size_bytes"] > 0
    assert health["storage"]["limit_bytes"] == 1024 * 1024
    assert health["storage"]["utilization_pct"] > 0
    assert health["totals"] == {
        "forecast_count": 0,
        "snapshot_count": 2,
        "outcome_count": 1,
        "pending_count": 1,
        "overdue_count": 0,
        "active_dataset_count": 2,
    }
    bitcoin = next(
        row
        for row in health["datasets"]
        if row["coin_id"] == "bitcoin" and row["horizon_days"] == 1
    )
    assert bitcoin["labeled_sample_count"] == 1
    assert bitcoin["label_coverage_pct"] == 100.0


def test_store_reports_live_performance_windows(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()
    snapshots = [
        (
            "2026-07-10T12:01:00+00:00",
            2.0,
            "bullish",
            {"lower_change_pct": 0.0, "upper_change_pct": 4.0},
            70.0,
            103.0,
            "2026-07-11T12:16:00+00:00",
        ),
        (
            "2026-07-11T12:01:00+00:00",
            -1.0,
            "bearish",
            {"lower_change_pct": -3.0, "upper_change_pct": 1.0},
            30.0,
            98.0,
            "2026-07-12T12:16:00+00:00",
        ),
    ]
    for generated_at, expected, direction, interval, probability, price, observed_at in snapshots:
        store.record_feature_snapshot(
            coin_id="bitcoin",
            symbol="BTC",
            generated_at=generated_at,
            horizon_days=1,
            market={"current_price": 100.0},
            technical={"rsi": 52.0},
            derivatives={"available": True},
            news_sentiment={"score": 0.1},
            model={
                "model_version": "5.1.0",
                "expected_change_pct": expected,
                "direction_key": direction,
                "prediction_interval": interval,
                "probability": {
                    "probability_pct": probability,
                    "baseline_probability_pct": 50.0,
                    "event": {"target_return_pct": 1.0},
                },
            },
        )
        assert store.settle_due_feature_snapshots(
            "bitcoin",
            1,
            observed_at,
            price,
        ) == 1

    performance = store.performance_summary(
        "bitcoin",
        1,
        now=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )

    assert performance["all_time"]["samples"] == 2
    assert performance["all_time"]["mae_pct"] == 1.0
    assert performance["all_time"]["baseline_mae_pct"] == 2.5
    assert performance["all_time"]["skill_vs_baseline_pct"] == 60.0
    assert performance["all_time"]["active_directional_accuracy_pct"] == 100.0
    assert performance["all_time"]["interval_coverage_pct"] == 100.0
    assert performance["all_time"]["probability"]["brier_score"] == 0.09
    assert performance["windows"][0]["samples"] == 2
