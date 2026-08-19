from dataclasses import dataclass
import os


def _csv_setting(name: str, default: str) -> tuple[str, ...]:
    value = os.getenv(name, default)
    origins = tuple(item.strip() for item in value.split(",") if item.strip())
    return origins or ("*",)


@dataclass(frozen=True)
class Settings:
    cors_origins: tuple[str, ...] = _csv_setting(
        "CORS_ORIGINS",
        "*",
    )
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
    market_cache_seconds: int = int(os.getenv("MARKET_CACHE_SECONDS", "60"))
    chart_cache_seconds: int = int(os.getenv("CHART_CACHE_SECONDS", "300"))
    news_cache_seconds: int = int(os.getenv("NEWS_CACHE_SECONDS", "300"))
    stale_cache_seconds: int = int(os.getenv("STALE_CACHE_SECONDS", "900"))
    cryptocompare_api_key: str = os.getenv("CRYPTOCOMPARE_API_KEY", "").strip()
    forecast_db_path: str = os.getenv(
        "FORECAST_DB_PATH",
        "data/forecasts.sqlite3",
    ).strip()
    forecast_database_url: str = os.getenv(
        "FORECAST_DATABASE_URL",
        "",
    ).strip()
    forecast_storage_limit_mb: int = int(
        os.getenv("FORECAST_STORAGE_LIMIT_MB", "512")
    )
    snapshot_token: str = os.getenv("SNAPSHOT_TOKEN", "").strip()
    snapshot_stale_after_minutes: int = int(
        os.getenv("SNAPSHOT_STALE_AFTER_MINUTES", "45")
    )


settings = Settings()
