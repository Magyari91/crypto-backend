from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

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
    seven_day = next(
        item for item in payload["probability_models"] if item["horizon_days"] == 7
    )
    assert [item["key"] for item in seven_day["candidates"]] == [
        "logistic",
        "hist_gradient_boosting",
    ]
