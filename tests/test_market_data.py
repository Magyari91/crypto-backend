import asyncio

import pytest

from app.market_data import MarketDataService, UpstreamServiceError


class BinanceHistoryService(MarketDataService):
    def __init__(self):
        self.calls = []

    async def _get_json(self, service, url, params, cache_seconds, headers=None):
        self.calls.append(params)
        if "endTime" not in params:
            origins = range(95, 1095)
        else:
            origins = range(0, 95)
        return [
            [
                origin * 86_400_000,
                "100",
                "105",
                "95",
                str(100 + origin / 100),
                "10",
                origin * 86_400_000 + 86_399_999,
                "1000",
            ]
            for origin in origins
        ]


def test_binance_history_paginates_beyond_one_thousand_days():
    service = BinanceHistoryService()

    history = asyncio.run(service._binance_history("BTC", 1095))

    assert len(history["prices"]) == 1095
    assert len(history["total_volumes"]) == 1095
    assert history["source"] == "Binance (USDT)"
    assert len(service.calls) == 2
    assert service.calls[0]["limit"] == 1000
    assert service.calls[1]["limit"] == 95


class BinanceIntradayService(MarketDataService):
    def __init__(self):
        self.calls = []

    async def _get_json(self, service, url, params, cache_seconds, headers=None):
        self.calls.append(params)
        call_index = len(self.calls)
        if call_index == 1:
            origins = range(1200, 2200)
        elif call_index == 2:
            origins = range(200, 1200)
        else:
            origins = range(0, 200)
        return [
            [
                origin * 3_600_000,
                "100",
                "102",
                "99",
                "101",
                "10",
                origin * 3_600_000 + 3_599_999,
                "1000",
            ]
            for origin in origins
        ]


def test_binance_intraday_history_returns_hourly_ohlcv():
    service = BinanceIntradayService()

    history = asyncio.run(service._binance_intraday_history("BTC", 2200))

    assert len(history["candles"]) == 2200
    assert history["interval"] == "1h"
    assert history["candles"][0]["open"] == 100.0
    assert history["candles"][-1]["volume"] == 1000.0
    assert [call["limit"] for call in service.calls] == [1000, 1000, 200]


class BinanceDerivativesService(MarketDataService):
    def __init__(self):
        self.calls = []

    async def _get_json(self, service, url, params, cache_seconds, headers=None):
        self.calls.append((url, params))
        if url.endswith("/fapi/v1/fundingRate"):
            start = params["startTime"] - params["startTime"] % 86_400_000
            return [
                {"fundingTime": start + 3_600_000, "fundingRate": "0.0001"},
                {"fundingTime": start + 28_800_000, "fundingRate": "0.0002"},
            ]
        if url.endswith("/openInterestHist"):
            return [
                {"timestamp": 1_700_000_000_000, "sumOpenInterestValue": "1000000"}
            ]
        if url.endswith("/globalLongShortAccountRatio"):
            return [
                {
                    "timestamp": 1_700_000_000_000,
                    "longShortRatio": "1.2",
                    "longAccount": "0.5455",
                    "shortAccount": "0.4545",
                }
            ]
        return [
            {
                "timestamp": 1_700_000_000_000,
                "buySellRatio": "1.1",
                "buyVol": "110",
                "sellVol": "100",
            }
        ]


def test_binance_derivatives_history_combines_public_futures_series():
    service = BinanceDerivativesService()

    payload = asyncio.run(service._binance_derivatives_history("BTC", 365))

    assert payload["snapshot"]["available"] is True
    assert payload["snapshot"]["funding_rate_pct"] == 0.015
    assert payload["snapshot"]["open_interest_usd"] == 1_000_000.0
    assert payload["snapshot"]["long_short_ratio"] == 1.2
    assert len(service.calls) == 4


class BinanceMarketsService(MarketDataService):
    def __init__(self):
        self.params = None

    async def _get_json(self, service, url, params, cache_seconds, headers=None):
        self.params = params
        return [
            {
                "symbol": "BTCUSDT",
                "lastPrice": "64500.5",
                "priceChangePercent": "2.4",
                "highPrice": "65000",
                "lowPrice": "62000",
                "quoteVolume": "1000000",
            },
            {
                "symbol": "ETHUSDT",
                "lastPrice": "3500",
                "priceChangePercent": "-1.2",
                "highPrice": "3600",
                "lowPrice": "3400",
                "quoteVolume": "2000000",
            },
            {
                "symbol": "TESTUSDT",
                "lastPrice": "0.5",
                "priceChangePercent": "4.2",
                "highPrice": "0.55",
                "lowPrice": "0.45",
                "quoteVolume": "500000",
            },
        ]


def test_supported_markets_normalizes_binance_tickers():
    service = BinanceMarketsService()

    markets = asyncio.run(service.supported_markets())

    assert [market["id"] for market in markets] == ["bitcoin", "ethereum"]
    assert markets[0]["current_price"] == "64500.5"
    assert markets[1]["price_change_percentage_24h"] == "-1.2"
    assert service.params is None


def test_market_catalog_ranks_all_usdt_pairs_by_quote_volume():
    service = BinanceMarketsService()

    markets = asyncio.run(service.market_catalog(limit=200))

    assert [market["id"] for market in markets] == [
        "ethereum",
        "bitcoin",
        "binance-test",
    ]
    assert [market["market_cap_rank"] for market in markets] == [1, 2, 3]
    assert markets[0]["quote_volume_24h"] == 2_000_000
    assert service.params is None


class InvalidJsonResponse:
    def raise_for_status(self):
        return None

    def json(self):
        raise ValueError("invalid json")


class CountingFailingClient:
    def __init__(self):
        self.calls = 0

    async def get(self, *args, **kwargs):
        self.calls += 1
        return InvalidJsonResponse()


def test_upstream_circuit_breaker_skips_repeated_failed_requests():
    service = MarketDataService()
    client = CountingFailingClient()
    service._client = client

    async def call_twice():
        with pytest.raises(UpstreamServiceError):
            await service._get_json("CoinGecko", "https://example.test/one", None, 60)
        with pytest.raises(UpstreamServiceError):
            await service._get_json("CoinGecko", "https://example.test/two", None, 60)

    asyncio.run(call_twice())

    assert client.calls == 1
