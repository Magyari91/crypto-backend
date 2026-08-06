import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


JOURNAL_BUCKET_MINUTES = 15
FEATURE_SNAPSHOT_VERSION = "1.0.0"


def _utc_datetime(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


class ForecastStore:
    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    horizon_days INTEGER NOT NULL,
                    model_version TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    bucket_start TEXT NOT NULL,
                    due_at TEXT NOT NULL,
                    base_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    expected_change_pct REAL NOT NULL,
                    direction_key TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    indicators_json TEXT NOT NULL,
                    UNIQUE (coin_id, horizon_days, model_version, bucket_start)
                )
                """
            )
            existing_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(forecast_log)").fetchall()
            }
            probability_columns = {
                "event_probability": "REAL",
                "baseline_probability": "REAL",
                "event_target_return_pct": "REAL",
                "probability_model_active": "INTEGER",
                "probability_decision": "TEXT",
            }
            for column, column_type in probability_columns.items():
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE forecast_log ADD COLUMN {column} {column_type}"
                    )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_forecast_log_lookup
                ON forecast_log (coin_id, horizon_days, generated_at DESC)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_snapshot (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    horizon_days INTEGER NOT NULL,
                    feature_version TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    bucket_start TEXT NOT NULL,
                    market_json TEXT NOT NULL,
                    technical_json TEXT NOT NULL,
                    derivatives_json TEXT NOT NULL,
                    news_sentiment_json TEXT NOT NULL,
                    model_json TEXT NOT NULL,
                    UNIQUE (coin_id, horizon_days, feature_version, bucket_start)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_feature_snapshot_lookup
                ON feature_snapshot (coin_id, horizon_days, generated_at DESC)
                """
            )

    def record(
        self,
        coin_id: str,
        symbol: str,
        generated_at: str,
        forecast: dict[str, Any],
    ) -> bool:
        timestamp = _utc_datetime(generated_at)
        bucket = timestamp.replace(
            minute=(timestamp.minute // JOURNAL_BUCKET_MINUTES) * JOURNAL_BUCKET_MINUTES,
            second=0,
            microsecond=0,
        )
        horizon_days = int(forecast["horizon_days"])
        due_at = timestamp + timedelta(days=horizon_days)
        probability = forecast.get("probability_forecast", {})
        event = probability.get("event", {})
        decision = probability.get("decision", {})

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO forecast_log (
                    coin_id,
                    symbol,
                    horizon_days,
                    model_version,
                    generated_at,
                    bucket_start,
                    due_at,
                    base_price,
                    target_price,
                    expected_change_pct,
                    direction_key,
                    confidence,
                    indicators_json,
                    event_probability,
                    baseline_probability,
                    event_target_return_pct,
                    probability_model_active,
                    probability_decision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    coin_id,
                    symbol,
                    horizon_days,
                    str(forecast["model_version"]),
                    timestamp.isoformat(),
                    bucket.isoformat(),
                    due_at.isoformat(),
                    float(forecast["base_price"]),
                    float(forecast["target_price"]),
                    float(forecast["expected_change_pct"]),
                    str(forecast["direction_key"]),
                    float(forecast["confidence"]),
                    json.dumps(forecast.get("indicators", {}), ensure_ascii=False),
                    probability.get("probability_pct"),
                    probability.get("baseline_probability_pct"),
                    event.get("target_return_pct"),
                    int(bool(probability.get("active"))) if probability else None,
                    decision.get("key"),
                ),
            )
            return cursor.rowcount == 1

    def record_feature_snapshot(
        self,
        coin_id: str,
        symbol: str,
        generated_at: str,
        horizon_days: int,
        market: dict[str, Any],
        technical: dict[str, Any],
        derivatives: dict[str, Any],
        news_sentiment: dict[str, Any],
        model: dict[str, Any],
    ) -> bool:
        timestamp = _utc_datetime(generated_at)
        bucket = timestamp.replace(
            minute=(timestamp.minute // JOURNAL_BUCKET_MINUTES) * JOURNAL_BUCKET_MINUTES,
            second=0,
            microsecond=0,
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO feature_snapshot (
                    coin_id,
                    symbol,
                    horizon_days,
                    feature_version,
                    generated_at,
                    bucket_start,
                    market_json,
                    technical_json,
                    derivatives_json,
                    news_sentiment_json,
                    model_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    coin_id,
                    symbol,
                    int(horizon_days),
                    FEATURE_SNAPSHOT_VERSION,
                    timestamp.isoformat(),
                    bucket.isoformat(),
                    json.dumps(market, ensure_ascii=False),
                    json.dumps(technical, ensure_ascii=False),
                    json.dumps(derivatives, ensure_ascii=False),
                    json.dumps(news_sentiment, ensure_ascii=False),
                    json.dumps(model, ensure_ascii=False),
                ),
            )
            return cursor.rowcount == 1

    def feature_status(self, coin_id: str, horizon_days: int) -> dict[str, Any]:
        with self._connect() as connection:
            summary = connection.execute(
                """
                SELECT
                    COUNT(*) AS sample_count,
                    MIN(generated_at) AS first_generated_at,
                    MAX(generated_at) AS last_generated_at
                FROM feature_snapshot
                WHERE coin_id = ? AND horizon_days = ?
                """,
                (coin_id, int(horizon_days)),
            ).fetchone()
            latest = connection.execute(
                """
                SELECT feature_version, derivatives_json, news_sentiment_json, model_json
                FROM feature_snapshot
                WHERE coin_id = ? AND horizon_days = ?
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (coin_id, int(horizon_days)),
            ).fetchone()

        return {
            "feature_version": (
                latest["feature_version"] if latest else FEATURE_SNAPSHOT_VERSION
            ),
            "sample_count": int(summary["sample_count"] if summary else 0),
            "first_generated_at": summary["first_generated_at"] if summary else None,
            "last_generated_at": summary["last_generated_at"] if summary else None,
            "latest_derivatives": (
                json.loads(latest["derivatives_json"]) if latest else None
            ),
            "latest_news_sentiment": (
                json.loads(latest["news_sentiment_json"]) if latest else None
            ),
            "latest_model": json.loads(latest["model_json"]) if latest else None,
        }

    def recent(self, coin_id: str, horizon_days: int, limit: int = 12) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM forecast_log
                WHERE coin_id = ? AND horizon_days = ?
                ORDER BY generated_at DESC
                LIMIT ?
                """,
                (coin_id, horizon_days, limit),
            ).fetchall()

        return [
            {
                "id": row["id"],
                "coin_id": row["coin_id"],
                "symbol": row["symbol"],
                "horizon_days": row["horizon_days"],
                "model_version": row["model_version"],
                "generated_at": row["generated_at"],
                "due_at": row["due_at"],
                "base_price": row["base_price"],
                "target_price": row["target_price"],
                "expected_change_pct": row["expected_change_pct"],
                "direction_key": row["direction_key"],
                "confidence": row["confidence"],
                "indicators": json.loads(row["indicators_json"]),
                "event_probability_pct": row["event_probability"],
                "baseline_probability_pct": row["baseline_probability"],
                "event_target_return_pct": row["event_target_return_pct"],
                "probability_model_active": (
                    bool(row["probability_model_active"])
                    if row["probability_model_active"] is not None
                    else None
                ),
                "probability_decision": row["probability_decision"],
            }
            for row in rows
        ]
