import asyncio
from datetime import datetime, timezone
from typing import Any

from app.assets import ANALYSIS_ASSETS, analysis_asset_list
from app.forecast import build_forecast, calculate_indicators
from app.market_data import MarketDataService, UpstreamServiceError
from app.news import aggregate_news_sentiment, normalize_articles


SUPPORTED_COINS = ANALYSIS_ASSETS
FORECAST_HISTORY_DAYS = 2000


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _market_row(coin: dict[str, Any]) -> dict[str, Any]:
    sparkline = coin.get("sparkline_in_7d", {}).get("price", [])
    row = {
        "id": coin.get("id"),
        "symbol": str(coin.get("symbol", "")).upper(),
        "name": coin.get("name"),
        "image": coin.get("image"),
        "current_price": _number(coin.get("current_price")),
        "market_cap": _optional_number(coin.get("market_cap")),
        "market_cap_rank": coin.get("market_cap_rank"),
        "quote_volume_24h": _optional_number(coin.get("quote_volume_24h")),
        "change_24h": _optional_number(coin.get("price_change_percentage_24h")),
        "change_7d": _optional_number(coin.get("price_change_percentage_7d_in_currency")),
        "high_24h": _optional_number(coin.get("high_24h")),
        "low_24h": _optional_number(coin.get("low_24h")),
        "sparkline": [round(_number(price), 8) for price in sparkline[-42:]],
    }
    analysis_asset = SUPPORTED_COINS.get(str(row["id"]))
    row["analysis_available"] = analysis_asset is not None
    row["analysis_rank"] = (
        analysis_asset.get("analysis_rank") if analysis_asset else None
    )
    return row


def normalize_market_rows(coins: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_market_row(coin) for coin in coins if coin.get("id")]


def _enrich_market_from_chart(
    coin: dict[str, Any],
    chart: dict[str, Any],
) -> dict[str, Any]:
    enriched = dict(coin)
    prices = chart.get("prices", [])
    closes = [_optional_number(point[1]) for point in prices if len(point) >= 2]
    closes = [price for price in closes if price is not None]
    if not closes:
        return enriched

    enriched["current_price"] = enriched.get("current_price") or closes[-1]
    if enriched.get("price_change_percentage_24h") is None and len(closes) >= 2:
        enriched["price_change_percentage_24h"] = (closes[-1] / closes[-2] - 1) * 100
    if enriched.get("price_change_percentage_7d_in_currency") is None and len(closes) >= 8:
        enriched["price_change_percentage_7d_in_currency"] = (
            closes[-1] / closes[-8] - 1
        ) * 100
    enriched["sparkline_in_7d"] = {"price": closes[-42:]}
    return enriched


def normalize_news(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return normalize_articles(articles)


def build_indicator_summary(prices: list[list[float]]) -> list[dict[str, Any]]:
    latest = calculate_indicators(prices)
    return [
        {"name": "RSI (14)", "value": round(_number(latest["rsi"]), 1)},
        {"name": "EMA (20)", "value": round(_number(latest["ema20"]), 6)},
        {"name": "EMA (50)", "value": round(_number(latest["ema50"]), 6)},
        {"name": "MACD", "value": round(_number(latest["macd_histogram"]), 6)},
        {"name": "Bollinger felso", "value": round(_number(latest["bollinger_upper"]), 6)},
        {"name": "Bollinger also", "value": round(_number(latest["bollinger_lower"]), 6)},
    ]


async def _optional(awaitable, fallback):
    try:
        return await awaitable
    except Exception:
        return fallback


async def load_forecast_history(
    service: MarketDataService,
    coin: str,
    days: int = FORECAST_HISTORY_DAYS,
) -> dict[str, Any]:
    history_loader = getattr(service, "forecast_history", None)
    if callable(history_loader):
        try:
            return await history_loader(coin, days=days)
        except UpstreamServiceError:
            pass
    chart = await service.market_chart(coin, days=min(days, 365))
    return {**chart, "source": chart.get("source", "CoinGecko")}


async def build_dashboard(
    service: MarketDataService,
    selected_coin: str,
    horizon_days: int,
    include_news: bool = True,
) -> dict[str, Any]:
    global_task = _optional(service.global_market(), {"data": {}})
    markets_task = _optional(service.markets(per_page=30, sparkline=True), [])
    chart_task = load_forecast_history(service, selected_coin)
    benchmark_task = (
        asyncio.sleep(0, result=None)
        if selected_coin == "bitcoin"
        else load_forecast_history(service, "bitcoin")
    )
    fear_task = _optional(service.fear_greed(), None)
    news_task = _optional(service.news(), []) if include_news else asyncio.sleep(0, result=[])

    global_response, markets, chart, benchmark_chart, fear_greed, articles = await asyncio.gather(
        global_task,
        markets_task,
        chart_task,
        benchmark_task,
        fear_task,
        news_task,
    )
    benchmark_chart = chart if benchmark_chart is None else benchmark_chart

    market_data_source = "CoinGecko"
    if not markets:
        fallback_loader = getattr(service, "supported_markets", None)
        if callable(fallback_loader):
            markets = await _optional(fallback_loader(), [])
        market_data_source = "Binance Spot"

    global_data = global_response.get("data", {})
    selected_raw = next((coin for coin in markets if coin.get("id") == selected_coin), None)
    if selected_raw is None:
        selected_raw = {"id": selected_coin, **SUPPORTED_COINS[selected_coin]}
        market_data_source = chart.get("source", market_data_source)
    if market_data_source != "CoinGecko":
        selected_raw = _enrich_market_from_chart(selected_raw, chart)
        markets = [
            selected_raw if coin.get("id") == selected_coin else coin
            for coin in markets
        ] or [selected_raw]

    selected = _market_row(selected_raw)
    selected["forecast"] = await asyncio.to_thread(
        build_forecast,
        chart.get("prices", []),
        horizon_days,
        current_price=selected["current_price"],
        volumes=chart.get("total_volumes", []),
        market_prices=benchmark_chart.get("prices", []),
        funding_rates=chart.get("funding_rates", []),
    )
    selected["forecast"]["data_source"] = chart.get("source", "CoinGecko")
    selected["forecast"]["history_days"] = len(chart.get("prices", []))

    market_rows = normalize_market_rows(markets)
    valid_movers = [row for row in market_rows if row["change_24h"] is not None]
    sorted_movers = sorted(valid_movers, key=lambda row: row["change_24h"], reverse=True)

    fear_value = int(fear_greed.get("value", 0)) if fear_greed else None
    fear_label = fear_greed.get("value_classification") if fear_greed else None
    news_rows = normalize_news(articles)
    news_sentiment = aggregate_news_sentiment(news_rows, selected_coin)
    selected["forecast"]["sentiment_context"] = dict(news_sentiment)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market": {
            "overview_available": bool(global_data),
            "total_market_cap": _optional_number(global_data.get("total_market_cap", {}).get("usd")),
            "total_volume_24h": _optional_number(global_data.get("total_volume", {}).get("usd")),
            "btc_dominance": _optional_number(global_data.get("market_cap_percentage", {}).get("btc")),
            "eth_dominance": _optional_number(global_data.get("market_cap_percentage", {}).get("eth")),
            "market_cap_change_24h": _optional_number(global_data.get("market_cap_change_percentage_24h_usd")),
            "active_cryptocurrencies": int(global_data.get("active_cryptocurrencies", 0)),
            "fear_greed": {"value": fear_value, "label": fear_label},
        },
        "market_data_source": market_data_source,
        "derivatives": chart.get(
            "derivatives",
            {
                "available": False,
                "source": "Binance USDⓈ-M Futures",
                "status": "unavailable",
            },
        ),
        "news_sentiment": news_sentiment,
        "selected": selected,
        "movers": {
            "gainers": sorted_movers[:5],
            "losers": list(reversed(sorted_movers[-5:])),
        },
        "watchlist": market_rows[:10],
        "news": news_rows[:6],
        "supported_coins": analysis_asset_list(),
        "disclaimer": "Kísérleti technikai piaci jelzés, nem pénzügyi tanács.",
    }
