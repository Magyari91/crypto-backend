import asyncio
from datetime import datetime, timedelta, timezone

from app.dashboard import build_dashboard


def synthetic_prices(days: int = 120):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    price = 100.0
    values = []
    for offset in range(days):
        price *= 1.002
        timestamp = int((start + timedelta(days=offset)).timestamp() * 1000)
        values.append([timestamp, price])
    return values


class FakeMarketDataService:
    async def global_market(self):
        return {
            "data": {
                "total_market_cap": {"usd": 2_500_000_000_000},
                "total_volume": {"usd": 90_000_000_000},
                "market_cap_percentage": {"btc": 54.2, "eth": 17.4},
                "market_cap_change_percentage_24h_usd": 1.3,
                "active_cryptocurrencies": 12_000,
            }
        }

    async def markets(self, per_page=30, sparkline=True):
        return [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "image": "https://example.test/btc.png",
                "current_price": 126.0,
                "market_cap": 2_000_000_000_000,
                "market_cap_rank": 1,
                "price_change_percentage_24h": 2.1,
                "price_change_percentage_7d_in_currency": 4.8,
                "high_24h": 128.0,
                "low_24h": 121.0,
                "sparkline_in_7d": {"price": [120.0, 123.0, 126.0]},
            },
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "image": "https://example.test/eth.png",
                "current_price": 3000.0,
                "market_cap": 400_000_000_000,
                "market_cap_rank": 2,
                "price_change_percentage_24h": -1.2,
                "price_change_percentage_7d_in_currency": 0.4,
                "high_24h": 3100.0,
                "low_24h": 2950.0,
                "sparkline_in_7d": {"price": [3050.0, 3020.0, 3000.0]},
            },
        ]

    async def market_chart(self, coin, days=120):
        return {"prices": synthetic_prices(days)}

    async def fear_greed(self):
        return {"value": "62", "value_classification": "Greed"}

    async def news(self):
        return [
            {
                "id": "story-1",
                "title": "Market update",
                "url": "https://example.test/story",
                "source_info": {"name": "Example"},
                "published_on": 1_735_689_600,
            }
        ]


def test_dashboard_contract():
    payload = asyncio.run(build_dashboard(FakeMarketDataService(), "bitcoin", 7))

    assert payload["selected"]["id"] == "bitcoin"
    assert payload["selected"]["forecast"]["horizon_days"] == 7
    assert payload["selected"]["forecast"]["history_days"] == 365
    assert payload["selected"]["forecast"]["data_source"] == "CoinGecko"
    assert payload["market"]["btc_dominance"] == 54.2
    assert payload["movers"]["gainers"][0]["symbol"] == "BTC"
    assert payload["movers"]["losers"][0]["symbol"] == "ETH"
    assert payload["news"][0]["source"] == "Example"
