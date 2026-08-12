import asyncio
import json
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any
from urllib.parse import urlencode

import httpx2

from app.assets import ANALYSIS_ASSETS
from app.cache import AsyncTTLCache
from app.config import settings
from app.derivatives import normalize_derivatives
from app.news import MAX_FEED_BYTES, merge_articles, parse_rss_feed


class UpstreamServiceError(RuntimeError):
    def __init__(self, service: str, message: str):
        super().__init__(message)
        self.service = service


class MarketDataService:
    UPSTREAM_RETRY_SECONDS = 300
    COINGECKO_URL = "https://api.coingecko.com/api/v3"
    CRYPTOCOMPARE_URL = "https://min-api.cryptocompare.com/data/v2"
    BINANCE_MARKET_URL = "https://data-api.binance.vision/api/v3"
    BINANCE_FUTURES_URL = "https://fapi.binance.com"
    FEAR_GREED_URL = "https://api.alternative.me/fng/"
    RSS_NEWS_FEEDS = (
        ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ("Decrypt", "https://decrypt.co/feed"),
        ("Cointelegraph", "https://cointelegraph.com/rss/tag/bitcoin"),
    )
    RSS_HEADERS = {
        "Accept": "application/rss+xml, application/xml, text/xml;q=0.9",
        "User-Agent": "Mozilla/5.0 (compatible; CryptoVision/2.0; +https://github.com/Magyari91/crypto-backend)",
    }
    FORECAST_SYMBOLS = {
        coin_id: str(metadata["symbol"])
        for coin_id, metadata in ANALYSIS_ASSETS.items()
    }
    FORECAST_NAMES = {
        coin_id: str(metadata["name"])
        for coin_id, metadata in ANALYSIS_ASSETS.items()
    }

    def __init__(self):
        self._client: httpx2.AsyncClient | None = None
        self._cache = AsyncTTLCache(settings.stale_cache_seconds)
        self._upstream_retry_at: dict[str, float] = {}

    def _ensure_upstream_available(self, service: str) -> None:
        if self._upstream_retry_at.get(service, 0) > monotonic():
            raise UpstreamServiceError(service, "Upstream retry cooldown is active")

    def _mark_upstream_available(self, service: str) -> None:
        self._upstream_retry_at.pop(service, None)

    def _mark_upstream_unavailable(self, service: str) -> None:
        self._upstream_retry_at[service] = monotonic() + self.UPSTREAM_RETRY_SECONDS

    async def start(self) -> None:
        timeout = httpx2.Timeout(settings.request_timeout_seconds)
        limits = httpx2.Limits(max_connections=20, max_keepalive_connections=10)
        self._client = httpx2.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers={"User-Agent": "CryptoVision/2.0"},
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _get_json(
        self,
        service: str,
        url: str,
        params: dict[str, Any] | None,
        cache_seconds: int,
        headers: dict[str, str] | None = None,
    ) -> Any:
        query = urlencode(sorted((params or {}).items()))
        cache_key = f"{url}?{query}"

        async def loader() -> Any:
            if self._client is None:
                raise RuntimeError("MarketDataService has not been started")
            self._ensure_upstream_available(service)
            try:
                response = await self._client.get(url, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
            except (httpx2.HTTPError, ValueError) as exc:
                self._mark_upstream_unavailable(service)
                raise UpstreamServiceError(service, str(exc)) from exc
            self._mark_upstream_available(service)
            return payload

        return await self._cache.get_or_set(cache_key, cache_seconds, loader)

    async def _get_bytes(
        self,
        service: str,
        url: str,
        cache_seconds: int,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        cache_key = f"bytes:{url}"

        async def loader() -> bytes:
            if self._client is None:
                raise RuntimeError("MarketDataService has not been started")
            self._ensure_upstream_available(service)
            try:
                response = await self._client.get(url, headers=headers, follow_redirects=True)
                response.raise_for_status()
                if len(response.content) > MAX_FEED_BYTES:
                    raise ValueError("Feed response is too large")
                payload = response.content
            except (httpx2.HTTPError, ValueError) as exc:
                self._mark_upstream_unavailable(service)
                raise UpstreamServiceError(service, str(exc)) from exc
            self._mark_upstream_available(service)
            return payload

        return await self._cache.get_or_set(cache_key, cache_seconds, loader)

    async def _rss_news(self, source: str, url: str) -> list[dict[str, Any]]:
        payload = await self._get_bytes(
            source,
            url,
            settings.news_cache_seconds,
            self.RSS_HEADERS,
        )
        try:
            return parse_rss_feed(payload, source)
        except ValueError as exc:
            raise UpstreamServiceError(source, str(exc)) from exc

    async def global_market(self) -> dict[str, Any]:
        return await self._get_json(
            "CoinGecko",
            f"{self.COINGECKO_URL}/global",
            None,
            settings.market_cache_seconds,
        )

    async def markets(self, per_page: int = 50, sparkline: bool = True) -> list[dict[str, Any]]:
        data = await self._get_json(
            "CoinGecko",
            f"{self.COINGECKO_URL}/coins/markets",
            {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": 1,
                "sparkline": str(sparkline).lower(),
                "price_change_percentage": "24h,7d",
            },
            settings.market_cache_seconds,
        )
        if not isinstance(data, list):
            raise UpstreamServiceError("CoinGecko", "Unexpected markets response")
        return data

    async def supported_markets(self) -> list[dict[str, Any]]:
        pairs = [f"{symbol}USDT" for symbol in self.FORECAST_SYMBOLS.values()]
        data = await self._get_json(
            "Binance",
            f"{self.BINANCE_MARKET_URL}/ticker/24hr",
            {"symbols": json.dumps(pairs, separators=(",", ":"))},
            settings.market_cache_seconds,
        )
        if not isinstance(data, list):
            raise UpstreamServiceError("Binance", "Unexpected ticker response")

        tickers = {
            str(item.get("symbol", "")).upper(): item
            for item in data
            if isinstance(item, dict)
        }
        markets = []
        for coin_id, symbol in self.FORECAST_SYMBOLS.items():
            ticker = tickers.get(f"{symbol}USDT")
            if ticker is None:
                continue
            markets.append(
                {
                    "id": coin_id,
                    "symbol": symbol.lower(),
                    "name": self.FORECAST_NAMES[coin_id],
                    "image": None,
                    "current_price": ticker.get("lastPrice"),
                    "market_cap": None,
                    "market_cap_rank": None,
                    "price_change_percentage_24h": ticker.get("priceChangePercent"),
                    "price_change_percentage_7d_in_currency": None,
                    "high_24h": ticker.get("highPrice"),
                    "low_24h": ticker.get("lowPrice"),
                    "sparkline_in_7d": {"price": []},
                }
            )
        if not markets:
            raise UpstreamServiceError("Binance", "No supported ticker data")
        return markets

    async def market_chart(self, coin: str, days: int = 120) -> dict[str, Any]:
        return await self._get_json(
            "CoinGecko",
            f"{self.COINGECKO_URL}/coins/{coin}/market_chart",
            {"vs_currency": "usd", "days": days},
            settings.chart_cache_seconds,
        )

    async def _binance_history(self, symbol: str, days: int) -> dict[str, Any]:
        requested_days = min(max(days, 61), 2000)
        candles: dict[int, list[Any]] = {}
        end_time = None

        while len(candles) < requested_days:
            remaining = requested_days - len(candles)
            params: dict[str, Any] = {
                "symbol": f"{symbol}USDT",
                "interval": "1d",
                "limit": min(remaining, 1000),
            }
            if end_time is not None:
                params["endTime"] = end_time
            rows = await self._get_json(
                "Binance",
                f"{self.BINANCE_MARKET_URL}/klines",
                params,
                settings.chart_cache_seconds,
            )
            if not isinstance(rows, list) or not rows:
                break
            valid_rows = [row for row in rows if isinstance(row, list) and len(row) >= 8]
            if not valid_rows:
                break
            for row in valid_rows:
                candles[int(row[0])] = row
            end_time = min(int(row[0]) for row in valid_rows) - 1
            if len(valid_rows) < params["limit"]:
                break

        prices = []
        volumes = []
        for timestamp, row in sorted(candles.items())[-requested_days:]:
            try:
                close = float(row[4])
                quote_volume = float(row[7])
            except (TypeError, ValueError):
                continue
            if close <= 0:
                continue
            prices.append([timestamp, close])
            volumes.append([timestamp, max(quote_volume, 0.0)])

        if len(prices) < 61:
            raise UpstreamServiceError("Binance", "Not enough historical data")
        return {
            "prices": prices,
            "total_volumes": volumes,
            "source": "Binance (USDT)",
        }

    async def _binance_intraday_history(
        self,
        symbol: str,
        hours: int,
    ) -> dict[str, Any]:
        requested_hours = min(max(hours, 720), 10_000)
        rows_by_timestamp: dict[int, list[Any]] = {}
        end_time = None

        while len(rows_by_timestamp) < requested_hours:
            remaining = requested_hours - len(rows_by_timestamp)
            params: dict[str, Any] = {
                "symbol": f"{symbol}USDT",
                "interval": "1h",
                "limit": min(remaining, 1000),
            }
            if end_time is not None:
                params["endTime"] = end_time
            rows = await self._get_json(
                "Binance",
                f"{self.BINANCE_MARKET_URL}/klines",
                params,
                settings.chart_cache_seconds,
            )
            if not isinstance(rows, list) or not rows:
                break
            valid_rows = [row for row in rows if isinstance(row, list) and len(row) >= 8]
            if not valid_rows:
                break
            for row in valid_rows:
                rows_by_timestamp[int(row[0])] = row
            end_time = min(int(row[0]) for row in valid_rows) - 1
            if len(valid_rows) < params["limit"]:
                break

        candles = []
        for timestamp, row in sorted(rows_by_timestamp.items())[-requested_hours:]:
            try:
                open_price = float(row[1])
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                quote_volume = float(row[7])
            except (TypeError, ValueError):
                continue
            if min(open_price, high, low, close) <= 0:
                continue
            candles.append(
                {
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": max(quote_volume, 0.0),
                }
            )

        if len(candles) < 720:
            raise UpstreamServiceError("Binance", "Not enough hourly historical data")
        return {
            "candles": candles,
            "source": "Binance (USDT)",
            "interval": "1h",
        }

    async def _binance_funding_history(
        self,
        symbol: str,
        days: int,
    ) -> list[dict[str, Any]]:
        requested_days = min(max(days, 30), 2000)
        now = datetime.now(timezone.utc)
        cursor = int((now - timedelta(days=requested_days)).timestamp() * 1000)
        end_time = int(now.timestamp() * 1000)
        rows_by_timestamp: dict[int, dict[str, Any]] = {}

        while cursor <= end_time and len(rows_by_timestamp) < requested_days * 4:
            rows = await self._get_json(
                "Binance Futures",
                f"{self.BINANCE_FUTURES_URL}/fapi/v1/fundingRate",
                {
                    "symbol": f"{symbol}USDT",
                    "startTime": cursor,
                    "endTime": end_time,
                    "limit": 1000,
                },
                settings.chart_cache_seconds,
            )
            if not isinstance(rows, list) or not rows:
                break
            valid_rows = [
                row
                for row in rows
                if isinstance(row, dict) and row.get("fundingTime") is not None
            ]
            if not valid_rows:
                break
            for row in valid_rows:
                rows_by_timestamp[int(row["fundingTime"])] = row
            next_cursor = max(int(row["fundingTime"]) for row in valid_rows) + 1
            if next_cursor <= cursor or len(valid_rows) < 1000:
                break
            cursor = next_cursor

        return [rows_by_timestamp[key] for key in sorted(rows_by_timestamp)]

    async def _binance_derivatives_history(
        self,
        symbol: str,
        days: int,
    ) -> dict[str, Any]:
        pair = f"{symbol}USDT"
        funding_task = self._binance_funding_history(symbol, days)
        open_interest_task = self._get_json(
            "Binance Futures",
            f"{self.BINANCE_FUTURES_URL}/futures/data/openInterestHist",
            {"symbol": pair, "period": "1d", "limit": 30},
            settings.chart_cache_seconds,
        )
        long_short_task = self._get_json(
            "Binance Futures",
            f"{self.BINANCE_FUTURES_URL}/futures/data/globalLongShortAccountRatio",
            {"symbol": pair, "period": "1d", "limit": 30},
            settings.chart_cache_seconds,
        )
        taker_task = self._get_json(
            "Binance Futures",
            f"{self.BINANCE_FUTURES_URL}/futures/data/takerlongshortRatio",
            {"symbol": pair, "period": "1d", "limit": 30},
            settings.chart_cache_seconds,
        )
        funding, open_interest, long_short, taker = await asyncio.gather(
            funding_task,
            open_interest_task,
            long_short_task,
            taker_task,
            return_exceptions=True,
        )

        def rows(value: Any) -> list[dict[str, Any]]:
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
            return []

        normalized = normalize_derivatives(
            funding_rows=rows(funding),
            open_interest_rows=rows(open_interest),
            long_short_rows=rows(long_short),
            taker_rows=rows(taker),
        )
        if not normalized["snapshot"]["available"]:
            failures = [
                str(value)
                for value in (funding, open_interest, long_short, taker)
                if isinstance(value, Exception)
            ]
            if failures:
                normalized["snapshot"]["status"] = "unavailable"
                normalized["snapshot"]["reason"] = "A futures adatforrás átmenetileg nem érhető el."
        return normalized

    async def _cryptocompare_history(
        self,
        symbol: str,
        days: int,
    ) -> dict[str, Any]:
        headers = {"authorization": f"Apikey {settings.cryptocompare_api_key}"}
        payload = await self._get_json(
            "CryptoCompare",
            f"{self.CRYPTOCOMPARE_URL}/histoday",
            {
                "fsym": symbol,
                "tsym": "USD",
                "limit": min(max(days, 61), 2000),
                "aggregate": 1,
                "e": "CCCAGG",
                "extraParams": "CryptoVision",
            },
            settings.chart_cache_seconds,
            headers,
        )
        if not isinstance(payload, dict) or payload.get("Response") == "Error":
            message = (
                payload.get("Message", "Unexpected historical response")
                if isinstance(payload, dict)
                else "Unexpected historical response"
            )
            raise UpstreamServiceError("CryptoCompare", str(message))

        rows = payload.get("Data", {}).get("Data", [])
        prices = []
        volumes = []
        for row in rows if isinstance(rows, list) else []:
            try:
                timestamp = int(row["time"]) * 1000
                close = float(row["close"])
                quote_volume = float(row.get("volumeto", 0.0))
            except (KeyError, TypeError, ValueError):
                continue
            if close <= 0:
                continue
            prices.append([timestamp, close])
            volumes.append([timestamp, max(quote_volume, 0.0)])

        if len(prices) < 61:
            raise UpstreamServiceError("CryptoCompare", "Not enough historical data")
        return {"prices": prices, "total_volumes": volumes, "source": "CryptoCompare"}

    async def forecast_history(self, coin: str, days: int = 1095) -> dict[str, Any]:
        symbol = self.FORECAST_SYMBOLS.get(coin)
        if symbol is None:
            raise UpstreamServiceError("Forecast history", "Unsupported forecast symbol")

        try:
            history, derivatives = await asyncio.gather(
                self._binance_history(symbol, days),
                self._binance_derivatives_history(symbol, days),
                return_exceptions=True,
            )
            if isinstance(history, Exception):
                raise history
            if isinstance(derivatives, Exception):
                derivatives = normalize_derivatives()
                derivatives["snapshot"]["status"] = "unavailable"
                derivatives["snapshot"]["reason"] = (
                    "A futures adatforrás átmenetileg nem érhető el."
                )
            return {
                **history,
                "funding_rates": derivatives.get("funding_rates", []),
                "derivatives": derivatives.get("snapshot", {}),
            }
        except UpstreamServiceError:
            if settings.cryptocompare_api_key:
                return await self._cryptocompare_history(symbol, days)
            raise

    async def derivatives_snapshot(self, coin: str) -> dict[str, Any]:
        symbol = self.FORECAST_SYMBOLS.get(coin)
        if symbol is None:
            raise UpstreamServiceError("Binance Futures", "Unsupported forecast symbol")
        payload = await self._binance_derivatives_history(symbol, days=2000)
        return payload["snapshot"]

    async def forecast_intraday_history(
        self,
        coin: str,
        hours: int = 6480,
    ) -> dict[str, Any]:
        symbol = self.FORECAST_SYMBOLS.get(coin)
        if symbol is None:
            raise UpstreamServiceError("Forecast history", "Unsupported forecast symbol")
        return await self._binance_intraday_history(symbol, hours)

    async def news(self) -> list[dict[str, Any]]:
        feed_results = await asyncio.gather(
            *(self._rss_news(source, url) for source, url in self.RSS_NEWS_FEEDS),
            return_exceptions=True,
        )
        articles = []
        failures = []
        for result in feed_results:
            if isinstance(result, Exception):
                failures.append(str(result))
            else:
                articles.extend(result)

        if settings.cryptocompare_api_key:
            try:
                data = await self._get_json(
                    "CryptoCompare",
                    f"{self.CRYPTOCOMPARE_URL}/news/",
                    {"lang": "EN"},
                    settings.news_cache_seconds,
                    {"authorization": f"Apikey {settings.cryptocompare_api_key}"},
                )
                provider_articles = data.get("Data", []) if isinstance(data, dict) else []
                if isinstance(provider_articles, list):
                    articles.extend(provider_articles)
            except UpstreamServiceError as exc:
                failures.append(str(exc))

        merged = merge_articles(articles)
        if not merged:
            detail = "; ".join(failures) or "No articles returned"
            raise UpstreamServiceError("News feeds", detail)
        return merged

    async def fear_greed(self) -> dict[str, Any] | None:
        data = await self._get_json(
            "Alternative.me",
            self.FEAR_GREED_URL,
            {"limit": 1},
            settings.news_cache_seconds,
        )
        values = data.get("data", []) if isinstance(data, dict) else []
        return values[0] if values else None

    async def search(self, query: str) -> dict[str, Any]:
        return await self._get_json(
            "CoinGecko",
            f"{self.COINGECKO_URL}/search",
            {"query": query},
            settings.market_cache_seconds,
        )
