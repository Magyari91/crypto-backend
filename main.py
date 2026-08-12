import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import secrets

from fastapi import (
    FastAPI,
    Header,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.async_jobs import AsyncJobCache
from app.assets import ANALYSIS_LIMIT, analysis_asset_list
from app.backtest import evaluate_journal, walk_forward_backtest
from app.cache import AsyncTTLCache
from app.config import settings
from app.dashboard import (
    SUPPORTED_COINS,
    build_dashboard,
    build_indicator_summary,
    load_forecast_history,
    normalize_market_rows,
    normalize_news,
)
from app.forecast import DIRECTION_THRESHOLDS, MODEL_VERSION
from app.forecast_store import ForecastStore
from app.market_data import MarketDataService, UpstreamServiceError
from app.model_lab import build_model_lab
from app.news import aggregate_news_sentiment
from app.probability_models import probability_registry_payload
from app.snapshot_schedule import scheduled_snapshot_target
from app.specialist_models import specialist_registry_payload
from app.training_readiness import build_training_readiness


@asynccontextmanager
async def lifespan(app: FastAPI):
    service = MarketDataService()
    forecast_store = ForecastStore(
        settings.forecast_db_path,
        settings.forecast_database_url,
    )
    await asyncio.to_thread(forecast_store.initialize)
    await service.start()
    app.state.market_data = service
    app.state.forecast_store = forecast_store
    app.state.analytics_cache = AsyncTTLCache(settings.stale_cache_seconds)
    app.state.analytics_jobs = AsyncJobCache()
    app.state.snapshot_lock = asyncio.Lock()
    try:
        yield
    finally:
        await service.close()


app = FastAPI(
    title="CryptoVision API",
    description="Cached market data and transparent technical market signals.",
    version=MODEL_VERSION,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=800)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.exception_handler(UpstreamServiceError)
async def upstream_error_handler(_request: Request, exc: UpstreamServiceError):
    return JSONResponse(
        status_code=502,
        content={
            "detail": "A piaci adatszolgáltató átmenetileg nem érhető el.",
            "service": exc.service,
        },
    )


def market_service(request: Request) -> MarketDataService:
    return request.app.state.market_data


def journal_store(request: Request) -> ForecastStore:
    return request.app.state.forecast_store


async def record_dashboard_forecast(
    request: Request,
    payload: dict,
) -> dict[str, bool | int]:
    selected = payload["selected"]
    forecast = selected["forecast"]
    store = journal_store(request)

    def persist() -> dict[str, bool]:
        forecast_created = store.record(
            selected["id"],
            selected["symbol"],
            payload["generated_at"],
            forecast,
        )
        snapshot_created = store.record_feature_snapshot(
            coin_id=selected["id"],
            symbol=selected["symbol"],
            generated_at=payload["generated_at"],
            horizon_days=int(forecast["horizon_days"]),
            market={
                **payload.get("market", {}),
                "current_price": selected.get("current_price"),
                "change_24h": selected.get("change_24h"),
                "change_7d": selected.get("change_7d"),
            },
            technical=forecast.get("indicators", {}),
            derivatives=payload.get("derivatives", {}),
            news_sentiment=payload.get("news_sentiment", {}),
            model={
                "model": forecast.get("model"),
                "model_version": forecast.get("model_version"),
                "specialist": forecast.get("specialist", {}),
                "probability": forecast.get("probability_forecast", {}),
            },
        )
        outcomes_settled = store.settle_due_feature_snapshots(
            coin_id=selected["id"],
            horizon_days=int(forecast["horizon_days"]),
            observed_at=payload["generated_at"],
            observed_price=selected.get("current_price"),
        )
        return {
            "forecast": forecast_created,
            "feature_snapshot": snapshot_created,
            "outcomes_settled": outcomes_settled,
        }

    return await asyncio.to_thread(persist)


def validate_coin(coin: str) -> str:
    normalized = coin.strip().lower()
    if normalized not in SUPPORTED_COINS:
        supported = ", ".join(SUPPORTED_COINS)
        raise ValueError(f"Nem támogatott eszköz. Választható: {supported}")
    return normalized


@app.get("/")
async def root():
    return {
        "name": "CryptoVision API",
        "version": MODEL_VERSION,
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health(request: Request):
    return {
        "status": "ok",
        "version": MODEL_VERSION,
        "storage": journal_store(request).storage_status(),
    }


@app.post("/api/v1/internal/snapshots/collect")
async def collect_snapshot(
    request: Request,
    coin: str | None = Query(default=None),
    horizon: int | None = Query(default=None),
    snapshot_token: str | None = Header(default=None, alias="X-Snapshot-Token"),
):
    configured_token = settings.snapshot_token
    if not configured_token:
        return JSONResponse(
            status_code=503,
            content={"detail": "A snapshot gyujto nincs aktivalva."},
        )
    if snapshot_token is None or not secrets.compare_digest(
        snapshot_token,
        configured_token,
    ):
        return JSONResponse(
            status_code=401,
            content={"detail": "Ervenytelen snapshot token."},
        )
    if (coin is None) != (horizon is None):
        return JSONResponse(
            status_code=422,
            content={"detail": "A coin es horizon parametert egyutt kell megadni."},
        )

    scheduled = coin is None
    if scheduled:
        target = scheduled_snapshot_target()
        selected_coin = target.coin
        selected_horizon = target.horizon
    else:
        try:
            selected_coin = validate_coin(coin or "")
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"detail": str(exc)})
        selected_horizon = int(horizon or 0)
        if selected_horizon not in {1, 7, 30}:
            return JSONResponse(
                status_code=422,
                content={"detail": "Az idotav 1, 7 vagy 30 nap lehet."},
            )

    lock: asyncio.Lock = request.app.state.snapshot_lock
    if lock.locked():
        return JSONResponse(
            status_code=409,
            content={"detail": "Egy snapshot gyujtes mar folyamatban van."},
        )

    async with lock:
        payload = await build_dashboard(
            market_service(request),
            selected_coin,
            selected_horizon,
        )
        persistence = await record_dashboard_forecast(request, payload)
        created = {
            "forecast": bool(persistence["forecast"]),
            "feature_snapshot": bool(persistence["feature_snapshot"]),
        }
        feature_store = await asyncio.to_thread(
            journal_store(request).feature_status,
            selected_coin,
            selected_horizon,
        )
        feature_summary = {
            key: feature_store[key]
            for key in (
                "feature_version",
                "sample_count",
                "first_generated_at",
                "last_generated_at",
            )
        }

    return {
        "status": "collected",
        "scheduled": scheduled,
        "generated_at": payload["generated_at"],
        "target": {"coin": selected_coin, "horizon_days": selected_horizon},
        "created": created,
        "outcomes_settled": persistence["outcomes_settled"],
        "storage": journal_store(request).storage_status(),
        "feature_store": feature_summary,
    }


@app.get("/api/v1/dashboard")
async def dashboard(
    request: Request,
    response: Response,
    coin: str = Query(default="bitcoin"),
    horizon: int = Query(default=7, ge=1, le=30),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    if horizon not in {1, 7, 30}:
        return JSONResponse(
            status_code=422,
            content={"detail": "Az időtáv 1, 7 vagy 30 nap lehet."},
        )

    response.headers["Cache-Control"] = "public, max-age=30, stale-while-revalidate=120"
    payload = await build_dashboard(market_service(request), selected_coin, horizon)
    await record_dashboard_forecast(request, payload)
    return payload


@app.get("/api/v1/forecast")
async def forecast(
    request: Request,
    coin: str = Query(default="bitcoin"),
    horizon: int = Query(default=7),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    if horizon not in {1, 7, 30}:
        return JSONResponse(
            status_code=422,
            content={"detail": "Az időtáv 1, 7 vagy 30 nap lehet."},
        )

    payload = await build_dashboard(market_service(request), selected_coin, horizon)
    await record_dashboard_forecast(request, payload)
    return {
        "generated_at": payload["generated_at"],
        "asset": payload["selected"],
        "disclaimer": payload["disclaimer"],
    }


@app.get("/api/v1/forecast/analytics")
async def forecast_analytics(
    request: Request,
    response: Response,
    coin: str = Query(default="bitcoin"),
    horizon: int = Query(default=7),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    if horizon not in {1, 7, 30}:
        return JSONResponse(
            status_code=422,
            content={"detail": "Az időtáv 1, 7 vagy 30 nap lehet."},
        )

    chart_task = load_forecast_history(market_service(request), selected_coin)
    benchmark_task = (
        asyncio.sleep(0, result=None)
        if selected_coin == "bitcoin"
        else load_forecast_history(market_service(request), "bitcoin")
    )
    store = journal_store(request)
    history_task = asyncio.to_thread(
        store.recent,
        selected_coin,
        horizon,
        12,
    )
    feature_status_task = asyncio.to_thread(
        store.feature_status,
        selected_coin,
        horizon,
    )
    chart, benchmark_chart, history, feature_status = await asyncio.gather(
        chart_task,
        benchmark_task,
        history_task,
        feature_status_task,
    )
    benchmark_chart = chart if benchmark_chart is None else benchmark_chart
    prices = chart.get("prices", [])

    last_point = prices[-1] if prices else [0, 0]
    benchmark_prices = benchmark_chart.get("prices", [])
    benchmark_last_point = benchmark_prices[-1] if benchmark_prices else [0, 0]
    funding_rates = chart.get("funding_rates", [])
    funding_last_point = funding_rates[-1] if funding_rates else [0, 0]
    backtest_cache_key = (
        f"{selected_coin}:{horizon}:{len(prices)}:{last_point[0]}:"
        f"{last_point[1]}:{benchmark_last_point[0]}:{benchmark_last_point[1]}:"
        f"{funding_last_point[0]}:{funding_last_point[1]}:{MODEL_VERSION}"
    )

    async def calculate_backtest():
        return await asyncio.to_thread(
            walk_forward_backtest,
            prices,
            horizon,
            chart.get("total_volumes", []),
            max_samples=60,
            market_prices=benchmark_chart.get("prices", []),
            funding_rates=funding_rates,
            minimum_refit_days=60,
        )

    try:
        job_status, evaluated_history = await asyncio.gather(
            request.app.state.analytics_jobs.get_or_start(
                backtest_cache_key,
                settings.chart_cache_seconds,
                calculate_backtest,
            ),
            asyncio.to_thread(evaluate_journal, history, prices),
        )
        if job_status.state == "pending":
            job_status = await request.app.state.analytics_jobs.wait(
                backtest_cache_key,
                settings.chart_cache_seconds,
                timeout_seconds=2.0,
            )
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    generated_at = datetime.now(timezone.utc).isoformat()
    common_payload = {
        "generated_at": generated_at,
        "asset": {"id": selected_coin, **SUPPORTED_COINS[selected_coin]},
        "horizon_days": horizon,
        "history": evaluated_history,
        "history_bucket_minutes": 15,
        "training_readiness": build_training_readiness(
            feature_status,
            store.storage_status(),
            horizon,
        ),
        "disclaimer": "A múltbeli eredmény nem garantálja a jövőbeli teljesítményt.",
    }
    if job_status.state != "ready":
        return JSONResponse(
            status_code=202,
            headers={"Cache-Control": "no-store", "Retry-After": "5"},
            content={
                **common_payload,
                "status": "pending",
                "retry_after_seconds": 5,
                "analysis_profile": "60 lezárt időpont, legfeljebb 60 napos újratanítás",
            },
        )

    response.headers["Cache-Control"] = "no-store"
    return {
        **common_payload,
        "status": "ready",
        "analysis_profile": "60 lezárt időpont, legfeljebb 60 napos újratanítás",
        "backtest": job_status.value,
    }


@app.get("/api/v1/forecast/lab")
async def forecast_model_lab(
    request: Request,
    response: Response,
    coin: str = Query(default="bitcoin"),
    horizon: int = Query(default=7),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    if horizon not in {1, 7}:
        return JSONResponse(
            status_code=422,
            content={"detail": "Az órás modelllabor 1 vagy 7 napos időtávhoz érhető el."},
        )

    history = await market_service(request).forecast_intraday_history(
        selected_coin,
        hours=6480,
    )
    candles = history.get("candles", [])
    last_candle = candles[-1] if candles else {"timestamp": 0, "close": 0}
    cache_key = (
        f"lab:{selected_coin}:{horizon}:{len(candles)}:"
        f"{last_candle['timestamp']}:{last_candle['close']}"
    )

    async def calculate_lab():
        return await asyncio.to_thread(
            build_model_lab,
            candles,
            horizon,
            DIRECTION_THRESHOLDS[horizon],
        )

    try:
        lab = await request.app.state.analytics_cache.get_or_set(
            cache_key,
            settings.chart_cache_seconds,
            calculate_lab,
        )
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    response.headers["Cache-Control"] = "no-store"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "asset": {"id": selected_coin, **SUPPORTED_COINS[selected_coin]},
        "source": history.get("source", "Binance (USDT)"),
        **lab,
        "disclaimer": "Kísérleti modelljelölt, nem pénzügyi tanács.",
    }


@app.get("/api/v1/markets")
async def markets(
    request: Request,
    response: Response,
    limit: int = Query(default=200, ge=10, le=200),
):
    service = market_service(request)
    source = "CoinGecko"
    ranking_basis = "market_cap"
    partial = False
    try:
        data = await service.markets(per_page=limit, sparkline=False)
    except UpstreamServiceError:
        fallback_catalog = getattr(service, "market_catalog", None)
        if callable(fallback_catalog):
            data = await fallback_catalog(limit=limit)
            source = "Binance Spot"
            ranking_basis = "quote_volume_24h"
        else:
            data = await service.supported_markets()
            source = "Binance Spot"
            ranking_basis = "analysis_assets"
        partial = True

    rows = normalize_market_rows(data)
    response.headers["Cache-Control"] = "public, max-age=120, stale-while-revalidate=600"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "ranking_basis": ranking_basis,
        "partial": partial,
        "requested_limit": limit,
        "count": len(rows),
        "analysis_limit": ANALYSIS_LIMIT,
        "analysis_assets": analysis_asset_list(),
        "items": rows,
    }


@app.get("/api/v1/forecast/registry")
async def forecast_registry(
    request: Request,
    coin: str = Query(default="bitcoin"),
    horizon: int = Query(default=7),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
    if horizon not in {1, 7, 30}:
        return JSONResponse(
            status_code=422,
            content={"detail": "Az időtáv 1, 7 vagy 30 nap lehet."},
        )

    feature_store = await asyncio.to_thread(
        journal_store(request).feature_status,
        selected_coin,
        horizon,
    )
    storage = journal_store(request).storage_status()
    return {
        "model_version": MODEL_VERSION,
        "asset": {"id": selected_coin, **SUPPORTED_COINS[selected_coin]},
        "horizon_days": horizon,
        "probability_models": probability_registry_payload(),
        "specialist_models": specialist_registry_payload(),
        "feature_store": feature_store,
        "storage": storage,
        "training_readiness": build_training_readiness(
            feature_store,
            storage,
            horizon,
        ),
        "activation_policy": (
            "A challenger csak pozitív validációs és érintetlen holdout-előny, "
            "stabil idősávok, elfogadható kalibráció és alacsony eloszláseltolódás "
            "mellett válhat aktívvá."
        ),
    }


@app.get("/api/v1/derivatives")
async def derivatives(
    request: Request,
    response: Response,
    coin: str = Query(default="bitcoin"),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
    response.headers["Cache-Control"] = "public, max-age=60, stale-while-revalidate=300"
    return await market_service(request).derivatives_snapshot(selected_coin)


@app.get("/api/v1/news")
async def news(request: Request, limit: int = Query(default=6, ge=1, le=20)):
    data = await market_service(request).news()
    return normalize_news(data)[:limit]


@app.get("/api/v1/news/sentiment")
async def news_sentiment(
    request: Request,
    response: Response,
    coin: str = Query(default="bitcoin"),
    limit: int = Query(default=20, ge=1, le=20),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    rows = normalize_news(await market_service(request).news())[:limit]
    response.headers["Cache-Control"] = "public, max-age=120, stale-while-revalidate=600"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "asset": {"id": selected_coin, **SUPPORTED_COINS[selected_coin]},
        "sentiment": aggregate_news_sentiment(rows, selected_coin),
        "articles": rows,
    }


@app.get("/api/v1/search")
async def search(request: Request, query: str = Query(min_length=2, max_length=40)):
    data = await market_service(request).search(query)
    return data.get("coins", [])[:10]


# Backward-compatible routes for the currently deployed frontend.
@app.get("/market-overview")
async def legacy_market_overview(request: Request):
    payload = await build_dashboard(market_service(request), "bitcoin", 7)
    market = payload["market"]
    forecast_data = payload["selected"]["forecast"]
    return {
        "market_cap_total": market["total_market_cap"],
        "btc_dominance": market["btc_dominance"],
        "liquidation": 0,
        "avg_rsi": forecast_data["indicators"]["rsi"],
    }


@app.get("/crypto-data")
async def legacy_crypto_data(request: Request):
    return await market_service(request).markets(per_page=50, sparkline=False)


@app.get("/crypto-news")
async def legacy_crypto_news(request: Request):
    data = await market_service(request).news()
    return normalize_news(data)


@app.get("/crypto-indicators")
async def legacy_crypto_indicators(
    request: Request,
    coin: str = Query(default="bitcoin"),
    days: int = Query(default=90, ge=30, le=365),
):
    try:
        selected_coin = validate_coin(coin)
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    chart = await market_service(request).market_chart(selected_coin, max(days, 90))
    return build_indicator_summary(chart.get("prices", []))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    service: MarketDataService = websocket.app.state.market_data
    try:
        while True:
            payload = await build_dashboard(service, "bitcoin", 7, include_news=False)
            await websocket.send_json(
                {
                    "generated_at": payload["generated_at"],
                    "market": payload["market"],
                    "selected": payload["selected"],
                }
            )
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        return


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
