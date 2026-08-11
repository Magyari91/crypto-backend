from dataclasses import replace
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import main
from app.forecast_store import ForecastStore
from main import app


def analytics_prices(days: int = 180):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    values = []
    for offset in range(days):
        price *= 1.003
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        values.append([timestamp, price])
    return values


def intraday_candles(hours: int = 900):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    values = []
    for offset in range(hours):
        open_price = price
        price *= 1.0002
        values.append(
            {
                "timestamp": int((start + timedelta(hours=offset)).timestamp() * 1000),
                "open": open_price,
                "high": price * 1.001,
                "low": open_price * 0.999,
                "close": price,
                "volume": 1_000_000.0,
            }
        )
    return values


class AnalyticsMarketData:
    async def market_chart(self, _coin, days=365):
        return {"prices": analytics_prices(min(days, 180))}

    async def forecast_intraday_history(self, _coin, hours=6480):
        return {
            "candles": intraday_candles(min(hours, 900)),
            "source": "Test hourly source",
            "interval": "1h",
        }


def test_health_endpoint():
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_dashboard_rejects_unknown_coin_before_upstream_call():
    with TestClient(app) as client:
        response = client.get("/api/v1/dashboard?coin=unknown&horizon=7")

    assert response.status_code == 422
    assert "Nem támogatott" in response.json()["detail"]


def test_forecast_analytics_endpoint(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    with TestClient(app) as client:
        app.state.market_data = AnalyticsMarketData()
        app.state.forecast_store = store
        response = client.get("/api/v1/forecast/analytics?coin=bitcoin&horizon=7")

    assert response.status_code == 200
    payload = response.json()
    assert payload["asset"]["symbol"] == "BTC"
    assert payload["training_readiness"]["status"] == "storage_required"
    assert payload["backtest"]["summary"]["samples"] > 50
    assert payload["backtest"]["model"]["version"] == "5.0.0"
    probability = payload["backtest"]["summary"]["probability"]
    assert probability["target_return_pct"] == 1.0
    assert 0 <= probability["brier_score"] <= 1
    assert payload["history"] == []


def test_forecast_model_lab_endpoint(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    with TestClient(app) as client:
        app.state.market_data = AnalyticsMarketData()
        app.state.forecast_store = store
        response = client.get("/api/v1/forecast/lab?coin=bitcoin&horizon=7")

    assert response.status_code == 200
    payload = response.json()
    assert payload["asset"]["symbol"] == "BTC"
    assert payload["source"] == "Test hourly source"
    assert payload["data_resolution"] == "1h"
    assert payload["direction_candidate"]["available"] is False
    assert payload["risk_candidate"]["available"] is False


def test_forecast_model_lab_rejects_daily_long_horizon():
    with TestClient(app) as client:
        response = client.get("/api/v1/forecast/lab?coin=bitcoin&horizon=30")

    assert response.status_code == 422
    assert "1 vagy 7 napos" in response.json()["detail"]


def test_forecast_registry_exposes_challengers_and_feature_status(tmp_path):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()

    with TestClient(app) as client:
        app.state.forecast_store = store
        response = client.get("/api/v1/forecast/registry?coin=bitcoin&horizon=7")

    assert response.status_code == 200
    payload = response.json()
    assert payload["model_version"] == "5.0.0"
    assert payload["feature_store"]["sample_count"] == 0
    assert payload["storage"] == {"backend": "sqlite", "persistent": False}
    assert payload["training_readiness"]["status"] == "storage_required"
    seven_day = next(
        item for item in payload["probability_models"] if item["horizon_days"] == 7
    )
    assert [item["key"] for item in seven_day["candidates"]] == [
        "logistic",
        "hist_gradient_boosting",
    ]


def snapshot_dashboard(coin: str = "bitcoin", horizon: int = 7):
    return {
        "generated_at": "2026-08-08T12:01:00+00:00",
        "market": {"total_market_cap": 1_000_000.0},
        "derivatives": {"available": False},
        "news_sentiment": {"score": 0.1, "sample_size": 2},
        "selected": {
            "id": coin,
            "symbol": "BTC",
            "current_price": 100.0,
            "change_24h": 1.0,
            "change_7d": 2.0,
            "forecast": {
                "horizon_days": horizon,
                "model": "test-model",
                "model_version": "5.0.0",
                "base_price": 100.0,
                "target_price": 102.0,
                "expected_change_pct": 2.0,
                "direction_key": "bullish",
                "confidence": 60,
                "indicators": {"rsi": 55.0},
                "specialist": {},
                "probability_forecast": {},
            },
        },
    }


def test_snapshot_collector_is_disabled_without_token(monkeypatch):
    monkeypatch.setattr(main, "settings", replace(main.settings, snapshot_token=""))

    with TestClient(app) as client:
        response = client.post("/api/v1/internal/snapshots/collect")

    assert response.status_code == 503


def test_snapshot_collector_rejects_invalid_token(monkeypatch):
    monkeypatch.setattr(
        main,
        "settings",
        replace(main.settings, snapshot_token="collector-secret"),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/internal/snapshots/collect",
            headers={"X-Snapshot-Token": "wrong-secret"},
        )

    assert response.status_code == 401


def test_snapshot_collector_persists_manual_target(tmp_path, monkeypatch):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()
    monkeypatch.setattr(
        main,
        "settings",
        replace(main.settings, snapshot_token="collector-secret"),
    )

    async def build_test_dashboard(_service, coin, horizon):
        return snapshot_dashboard(coin, horizon)

    monkeypatch.setattr(main, "build_dashboard", build_test_dashboard)

    with TestClient(app) as client:
        app.state.forecast_store = store
        response = client.post(
            "/api/v1/internal/snapshots/collect?coin=bitcoin&horizon=7",
            headers={"X-Snapshot-Token": "collector-secret"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "collected"
    assert payload["scheduled"] is False
    assert payload["created"] == {"forecast": True, "feature_snapshot": True}
    assert payload["outcomes_settled"] == 0
    assert payload["feature_store"]["sample_count"] == 1
    assert set(payload["feature_store"]) == {
        "feature_version",
        "sample_count",
        "first_generated_at",
        "last_generated_at",
    }


def test_snapshot_collector_uses_scheduled_target(tmp_path, monkeypatch):
    store = ForecastStore(tmp_path / "forecast.sqlite3")
    store.initialize()
    monkeypatch.setattr(
        main,
        "settings",
        replace(main.settings, snapshot_token="collector-secret"),
    )
    class Target:
        coin = "ethereum"
        horizon = 30

    monkeypatch.setattr(main, "scheduled_snapshot_target", lambda: Target())

    async def build_test_dashboard(_service, coin, horizon):
        payload = snapshot_dashboard(coin, horizon)
        payload["selected"]["symbol"] = "ETH"
        return payload

    monkeypatch.setattr(main, "build_dashboard", build_test_dashboard)

    with TestClient(app) as client:
        app.state.forecast_store = store
        response = client.post(
            "/api/v1/internal/snapshots/collect",
            headers={"X-Snapshot-Token": "collector-secret"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["scheduled"] is True
    assert payload["target"] == {"coin": "ethereum", "horizon_days": 30}
    assert store.feature_status("ethereum", 30)["sample_count"] == 1
