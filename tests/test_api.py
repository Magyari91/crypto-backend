from dataclasses import replace
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import main
from app.forecast_store import ForecastStore
from app.market_data import UpstreamServiceError
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


class NewsMarketData:
    async def news(self):
        return [
            {
                "id": "bitcoin-story",
                "title": "Bitcoin ETF approval drives bullish inflows",
                "url": "https://example.test/bitcoin-story",
                "source_info": {"name": "Example News"},
                "published_on": 1_786_441_200,
                "body": "Institutional adoption supports the rally.",
            }
        ]


class MarketCatalogData:
    def __init__(self):
        self.market_request = None

    async def markets(self, per_page=50, sparkline=True):
        self.market_request = {"per_page": per_page, "sparkline": sparkline}
        return [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 64_000,
                "market_cap": 1_200_000_000_000,
                "market_cap_rank": 1,
                "price_change_percentage_24h": 1.5,
                "price_change_percentage_7d_in_currency": 3.0,
            },
            {
                "id": "tether",
                "symbol": "usdt",
                "name": "Tether",
                "current_price": 1,
                "market_cap": 180_000_000_000,
                "market_cap_rank": 3,
                "price_change_percentage_24h": 0.01,
                "price_change_percentage_7d_in_currency": -0.01,
            },
        ]


class FallbackMarketCatalogData(MarketCatalogData):
    async def markets(self, per_page=50, sparkline=True):
        raise UpstreamServiceError("CoinGecko", "rate limited")

    async def supported_markets(self):
        return [
            {
                "id": "cardano",
                "symbol": "ada",
                "name": "Cardano",
                "current_price": "0.18",
                "market_cap": None,
                "market_cap_rank": None,
                "price_change_percentage_24h": "1.2",
            }
        ]


class BinanceMarketCatalogData(FallbackMarketCatalogData):
    def __init__(self):
        super().__init__()
        self.catalog_limit = None

    async def market_catalog(self, limit=200):
        self.catalog_limit = limit
        return [
            {
                "id": "binance-test",
                "symbol": "test",
                "name": "TEST",
                "current_price": "0.5",
                "market_cap": None,
                "market_cap_rank": 1,
                "quote_volume_24h": 2_000_000,
                "price_source": "Binance Futures",
                "price_change_percentage_24h": "4.2",
            }
        ]


class HyperliquidMarketCatalogData(BinanceMarketCatalogData):
    async def market_catalog(self, limit=200):
        rows = await super().market_catalog(limit)
        rows[0]["price_source"] = "Hyperliquid Perpetuals"
        return rows


def test_health_endpoint():
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_news_sentiment_endpoint_exposes_asset_context_and_articles():
    with TestClient(app) as client:
        app.state.market_data = NewsMarketData()
        response = client.get("/api/v1/news/sentiment?coin=bitcoin&limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["asset"] == {
        "id": "bitcoin",
        "symbol": "BTC",
        "name": "Bitcoin",
        "analysis_rank": 1,
    }
    assert payload["sentiment"]["label"] == "positive"
    assert payload["sentiment"]["role"] == "context_only"
    assert payload["sentiment"]["forecast_weight_pct"] == 0.0
    assert payload["articles"][0]["sentiment"]["method"] == "vader_crypto_lexicon_v2"


def test_market_catalog_returns_200_ready_normalized_rows():
    service = MarketCatalogData()
    with TestClient(app) as client:
        app.state.market_data = service
        response = client.get("/api/v1/markets?limit=200")

    assert response.status_code == 200
    payload = response.json()
    assert service.market_request == {"per_page": 200, "sparkline": False}
    assert payload["source"] == "CoinGecko"
    assert payload["ranking_basis"] == "market_cap"
    assert payload["partial"] is False
    assert payload["requested_limit"] == 200
    assert payload["count"] == 2
    assert payload["analysis_limit"] == 10
    assert len(payload["analysis_assets"]) == 10
    assert payload["items"][0]["analysis_available"] is True
    assert payload["items"][0]["analysis_rank"] == 1
    assert payload["items"][1]["analysis_available"] is False
    assert payload["items"][1]["analysis_rank"] is None
    assert "max-age=120" in response.headers["cache-control"]


def test_market_catalog_falls_back_to_analysis_assets():
    with TestClient(app) as client:
        app.state.market_data = FallbackMarketCatalogData()
        response = client.get("/api/v1/markets?limit=200")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "Binance Spot"
    assert payload["ranking_basis"] == "analysis_assets"
    assert payload["partial"] is True
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "cardano"
    assert payload["items"][0]["analysis_available"] is True


def test_market_catalog_uses_broad_binance_fallback_when_available():
    service = BinanceMarketCatalogData()
    with TestClient(app) as client:
        app.state.market_data = service
        response = client.get("/api/v1/markets?limit=200")

    assert response.status_code == 200
    payload = response.json()
    assert service.catalog_limit == 200
    assert payload["source"] == "Binance Spot + Futures"
    assert payload["ranking_basis"] == "quote_volume_24h"
    assert payload["partial"] is True
    assert payload["items"][0]["quote_volume_24h"] == 2_000_000
    assert payload["items"][0]["price_source"] == "Binance Futures"
    assert payload["items"][0]["analysis_available"] is False


def test_market_catalog_reports_hyperliquid_supplement_source():
    with TestClient(app) as client:
        app.state.market_data = HyperliquidMarketCatalogData()
        response = client.get("/api/v1/markets?limit=200")

    assert response.status_code == 200
    assert response.json()["source"] == "Binance Spot + Hyperliquid"


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
    assert payload["backtest"]["model"]["version"] == "5.1.0"
    probability = payload["backtest"]["summary"]["probability"]
    assert probability["target_return_pct"] == 1.0
    assert 0 <= probability["brier_score"] <= 1
    assert payload["history"] == []
    assert payload["live_performance"]["all_time"]["samples"] == 0


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
    assert payload["model_version"] == "5.1.0"
    assert payload["feature_store"]["sample_count"] == 0
    assert payload["storage"] == {"backend": "sqlite", "persistent": False}
    assert payload["training_readiness"]["status"] == "storage_required"
    assert payload["live_performance"]["all_time"]["samples"] == 0
    seven_day_specialist = next(
        item for item in payload["specialist_models"] if item["horizon_days"] == 7
    )
    assert [item["key"] for item in seven_day_specialist["candidates"]] == [
        "gradient_boosting",
        "extra_trees",
        "huber",
    ]
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
