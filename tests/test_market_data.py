import asyncio

from app.market_data import MarketDataService


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
