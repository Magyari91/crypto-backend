import json
from math import sqrt
import sqlite3
from statistics import mean
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


JOURNAL_BUCKET_MINUTES = 15
FEATURE_SNAPSHOT_VERSION = "1.0.0"
LIVE_PERFORMANCE_WINDOWS = (7, 30, 90)
MINIMUM_LIVE_PERFORMANCE_SAMPLES = 20
PERFORMANCE_DIRECTION_THRESHOLDS = {1: 0.20, 7: 0.75, 30: 1.50}


def _utc_datetime(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _direction_key(value: float, horizon_days: int) -> str:
    threshold = PERFORMANCE_DIRECTION_THRESHOLDS[horizon_days]
    if value > threshold:
        return "bullish"
    if value < -threshold:
        return "bearish"
    return "neutral"


def _performance_metrics(
    records: list[dict[str, Any]],
    horizon_days: int,
) -> dict[str, Any]:
    samples = len(records)
    if not samples:
        return {
            "status": "collecting",
            "samples": 0,
            "minimum_samples": MINIMUM_LIVE_PERFORMANCE_SAMPLES,
            "from": None,
            "to": None,
            "mae_pct": None,
            "rmse_pct": None,
            "baseline_mae_pct": None,
            "skill_vs_baseline_pct": None,
            "directional_accuracy_pct": None,
            "active_directional_accuracy_pct": None,
            "signal_coverage_pct": None,
            "interval_coverage_pct": None,
            "interval_samples": 0,
            "probability": None,
        }

    absolute_errors = [
        abs(item["expected_change_pct"] - item["realized_return_pct"])
        for item in records
    ]
    baseline_errors = [abs(item["realized_return_pct"]) for item in records]
    mae = mean(absolute_errors)
    baseline_mae = mean(baseline_errors)
    skill = (
        (baseline_mae - mae) / baseline_mae * 100
        if baseline_mae > 0
        else 0.0
    )
    direction_hits = [
        item["predicted_direction"]
        == _direction_key(item["realized_return_pct"], horizon_days)
        for item in records
    ]
    active = [item for item in records if item["predicted_direction"] != "neutral"]
    active_hits = [
        item["predicted_direction"]
        == _direction_key(item["realized_return_pct"], horizon_days)
        for item in active
    ]
    interval_records = [
        item
        for item in records
        if item["lower_change_pct"] is not None
        and item["upper_change_pct"] is not None
    ]
    interval_coverage = (
        mean(
            item["lower_change_pct"]
            <= item["realized_return_pct"]
            <= item["upper_change_pct"]
            for item in interval_records
        )
        * 100
        if interval_records
        else None
    )
    probability_records = [
        item
        for item in records
        if item["event_happened"] is not None
        and item["event_probability"] is not None
    ]
    probability = None
    if probability_records:
        brier = mean(
            (item["event_probability"] - int(item["event_happened"])) ** 2
            for item in probability_records
        )
        baseline_rows = [
            item
            for item in probability_records
            if item["baseline_probability"] is not None
        ]
        baseline_brier = (
            mean(
                (item["baseline_probability"] - int(item["event_happened"])) ** 2
                for item in baseline_rows
            )
            if baseline_rows
            else None
        )
        brier_skill = (
            (baseline_brier - brier) / baseline_brier * 100
            if baseline_brier is not None and baseline_brier > 0
            else None
        )
        probability = {
            "samples": len(probability_records),
            "brier_score": round(brier, 4),
            "baseline_brier_score": (
                round(baseline_brier, 4) if baseline_brier is not None else None
            ),
            "brier_skill_pct": (
                round(brier_skill, 2) if brier_skill is not None else None
            ),
        }

    return {
        "status": (
            "ready" if samples >= MINIMUM_LIVE_PERFORMANCE_SAMPLES else "collecting"
        ),
        "samples": samples,
        "minimum_samples": MINIMUM_LIVE_PERFORMANCE_SAMPLES,
        "from": min(item["observed_at"] for item in records).isoformat(),
        "to": max(item["observed_at"] for item in records).isoformat(),
        "mae_pct": round(mae, 4),
        "rmse_pct": round(sqrt(mean(error**2 for error in absolute_errors)), 4),
        "baseline_mae_pct": round(baseline_mae, 4),
        "skill_vs_baseline_pct": round(skill, 2),
        "directional_accuracy_pct": round(mean(direction_hits) * 100, 2),
        "active_directional_accuracy_pct": (
            round(mean(active_hits) * 100, 2) if active_hits else None
        ),
        "signal_coverage_pct": round(len(active) / samples * 100, 2),
        "interval_coverage_pct": (
            round(interval_coverage, 2) if interval_coverage is not None else None
        ),
        "interval_samples": len(interval_records),
        "probability": probability,
    }


class ForecastStore:
    def __init__(
        self,
        database_path: str | Path,
        database_url: str | None = None,
    ):
        self.database_path = Path(database_path)
        self.database_url = (database_url or "").strip()
        if self.database_url and not self.database_url.startswith(
            ("postgresql://", "postgres://")
        ):
            raise ValueError("FORECAST_DATABASE_URL must be a PostgreSQL URL")
        self.backend = "postgresql" if self.database_url else "sqlite"

    def _connect(self) -> Any:
        if self.database_url:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:
                raise RuntimeError(
                    "PostgreSQL storage requires psycopg[binary]"
                ) from exc
            return psycopg.connect(
                self.database_url,
                connect_timeout=5,
                row_factory=dict_row,
            )

        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def _execute(
        self,
        connection: Any,
        statement: str,
        parameters: tuple[Any, ...] = (),
    ) -> Any:
        if self.backend == "postgresql":
            statement = statement.replace("?", "%s")
        return connection.execute(statement, parameters)

    def _column_names(self, connection: Any, table_name: str) -> set[str]:
        if self.backend == "postgresql":
            rows = self._execute(
                connection,
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = ?
                """,
                (table_name,),
            ).fetchall()
            return {str(row["column_name"]) for row in rows}
        rows = self._execute(
            connection,
            f"PRAGMA table_info({table_name})",
        ).fetchall()
        return {str(row["name"]) for row in rows}

    def storage_status(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "persistent": self.backend == "postgresql",
        }

    def initialize(self) -> None:
        if self.backend == "sqlite":
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
        id_column = (
            "BIGSERIAL PRIMARY KEY"
            if self.backend == "postgresql"
            else "INTEGER PRIMARY KEY AUTOINCREMENT"
        )
        with self._connect() as connection:
            if self.backend == "sqlite":
                self._execute(connection, "PRAGMA journal_mode=WAL")
            self._execute(
                connection,
                f"""
                CREATE TABLE IF NOT EXISTS forecast_log (
                    id {id_column},
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
                """,
            )
            existing_columns = self._column_names(connection, "forecast_log")
            probability_columns = {
                "event_probability": "REAL",
                "baseline_probability": "REAL",
                "event_target_return_pct": "REAL",
                "probability_model_active": "INTEGER",
                "probability_decision": "TEXT",
            }
            for column, column_type in probability_columns.items():
                if column not in existing_columns:
                    self._execute(
                        connection,
                        f"ALTER TABLE forecast_log ADD COLUMN {column} {column_type}"
                    )
            self._execute(
                connection,
                """
                CREATE INDEX IF NOT EXISTS idx_forecast_log_lookup
                ON forecast_log (coin_id, horizon_days, generated_at DESC)
                """,
            )
            self._execute(
                connection,
                f"""
                CREATE TABLE IF NOT EXISTS feature_snapshot (
                    id {id_column},
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
                """,
            )
            self._execute(
                connection,
                """
                CREATE INDEX IF NOT EXISTS idx_feature_snapshot_lookup
                ON feature_snapshot (coin_id, horizon_days, generated_at DESC)
                """,
            )
            self._execute(
                connection,
                """
                CREATE TABLE IF NOT EXISTS feature_outcome (
                    feature_snapshot_id BIGINT PRIMARY KEY,
                    due_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    outcome_price REAL NOT NULL,
                    realized_return_pct REAL NOT NULL,
                    label_lag_minutes REAL NOT NULL,
                    event_target_return_pct REAL,
                    event_happened INTEGER,
                    FOREIGN KEY (feature_snapshot_id)
                        REFERENCES feature_snapshot(id) ON DELETE CASCADE
                )
                """,
            )
            self._execute(
                connection,
                """
                CREATE INDEX IF NOT EXISTS idx_feature_outcome_observed
                ON feature_outcome (observed_at DESC)
                """,
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
            cursor = self._execute(
                connection,
                """
                INSERT INTO forecast_log (
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
                ON CONFLICT (coin_id, horizon_days, model_version, bucket_start)
                DO NOTHING
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
            cursor = self._execute(
                connection,
                """
                INSERT INTO feature_snapshot (
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
                ON CONFLICT (coin_id, horizon_days, feature_version, bucket_start)
                DO NOTHING
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

    def settle_due_feature_snapshots(
        self,
        coin_id: str,
        horizon_days: int,
        observed_at: str,
        observed_price: float | int | str | None,
    ) -> int:
        timestamp = _utc_datetime(observed_at)
        try:
            price = float(observed_price)
        except (TypeError, ValueError):
            return 0
        if price <= 0:
            return 0

        settled = 0
        with self._connect() as connection:
            rows = self._execute(
                connection,
                """
                SELECT
                    snapshot.id,
                    snapshot.generated_at,
                    snapshot.horizon_days,
                    snapshot.market_json,
                    snapshot.model_json
                FROM feature_snapshot AS snapshot
                LEFT JOIN feature_outcome AS outcome
                    ON outcome.feature_snapshot_id = snapshot.id
                WHERE
                    snapshot.coin_id = ?
                    AND snapshot.horizon_days = ?
                    AND outcome.feature_snapshot_id IS NULL
                ORDER BY snapshot.generated_at
                """,
                (coin_id, int(horizon_days)),
            ).fetchall()

            for row in rows:
                generated_at = _utc_datetime(row["generated_at"])
                due_at = generated_at + timedelta(days=int(row["horizon_days"]))
                if due_at > timestamp:
                    continue

                market = json.loads(row["market_json"])
                try:
                    base_price = float(market.get("current_price"))
                except (TypeError, ValueError):
                    continue
                if base_price <= 0:
                    continue

                realized_return = ((price / base_price) - 1) * 100
                model = json.loads(row["model_json"])
                event = (model.get("probability") or {}).get("event") or {}
                try:
                    event_threshold = float(event["target_return_pct"])
                except (KeyError, TypeError, ValueError):
                    event_threshold = None
                event_happened = (
                    int(realized_return >= event_threshold)
                    if event_threshold is not None
                    else None
                )
                cursor = self._execute(
                    connection,
                    """
                    INSERT INTO feature_outcome (
                        feature_snapshot_id,
                        due_at,
                        observed_at,
                        outcome_price,
                        realized_return_pct,
                        label_lag_minutes,
                        event_target_return_pct,
                        event_happened
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (feature_snapshot_id) DO NOTHING
                    """,
                    (
                        row["id"],
                        due_at.isoformat(),
                        timestamp.isoformat(),
                        price,
                        realized_return,
                        max(0.0, (timestamp - due_at).total_seconds() / 60),
                        event_threshold,
                        event_happened,
                    ),
                )
                settled += int(cursor.rowcount == 1)

        return settled

    def feature_status(
        self,
        coin_id: str,
        horizon_days: int,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current_time = now or datetime.now(timezone.utc)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        current_time = current_time.astimezone(timezone.utc)

        with self._connect() as connection:
            summary = self._execute(
                connection,
                """
                SELECT
                    COUNT(*) AS sample_count,
                    SUM(
                        CASE WHEN outcome.feature_snapshot_id IS NOT NULL THEN 1 ELSE 0 END
                    ) AS labeled_sample_count,
                    COUNT(DISTINCT CASE
                        WHEN outcome.feature_snapshot_id IS NOT NULL
                        THEN SUBSTR(snapshot.generated_at, 1, 10)
                    END) AS independent_labeled_days,
                    MIN(snapshot.generated_at) AS first_generated_at,
                    MAX(snapshot.generated_at) AS last_generated_at,
                    MIN(outcome.observed_at) AS first_labeled_at,
                    MAX(outcome.observed_at) AS last_labeled_at
                FROM feature_snapshot AS snapshot
                LEFT JOIN feature_outcome AS outcome
                    ON outcome.feature_snapshot_id = snapshot.id
                WHERE snapshot.coin_id = ? AND snapshot.horizon_days = ?
                """,
                (coin_id, int(horizon_days)),
            ).fetchone()
            latest = self._execute(
                connection,
                """
                SELECT feature_version, derivatives_json, news_sentiment_json, model_json
                FROM feature_snapshot
                WHERE coin_id = ? AND horizon_days = ?
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (coin_id, int(horizon_days)),
            ).fetchone()
            pending_rows = self._execute(
                connection,
                """
                SELECT snapshot.generated_at, snapshot.horizon_days
                FROM feature_snapshot AS snapshot
                LEFT JOIN feature_outcome AS outcome
                    ON outcome.feature_snapshot_id = snapshot.id
                WHERE
                    snapshot.coin_id = ?
                    AND snapshot.horizon_days = ?
                    AND outcome.feature_snapshot_id IS NULL
                """,
                (coin_id, int(horizon_days)),
            ).fetchall()
            latest_outcome = self._execute(
                connection,
                """
                SELECT
                    outcome.due_at,
                    outcome.observed_at,
                    outcome.outcome_price,
                    outcome.realized_return_pct,
                    outcome.label_lag_minutes,
                    outcome.event_target_return_pct,
                    outcome.event_happened
                FROM feature_outcome AS outcome
                JOIN feature_snapshot AS snapshot
                    ON snapshot.id = outcome.feature_snapshot_id
                WHERE snapshot.coin_id = ? AND snapshot.horizon_days = ?
                ORDER BY outcome.observed_at DESC
                LIMIT 1
                """,
                (coin_id, int(horizon_days)),
            ).fetchone()

        sample_count = int(summary["sample_count"] if summary else 0)
        labeled_count = int((summary["labeled_sample_count"] if summary else 0) or 0)
        pending_due_dates = [
            _utc_datetime(row["generated_at"])
            + timedelta(days=int(row["horizon_days"]))
            for row in pending_rows
        ]
        overdue_count = sum(due_at <= current_time for due_at in pending_due_dates)

        return {
            "feature_version": (
                latest["feature_version"] if latest else FEATURE_SNAPSHOT_VERSION
            ),
            "sample_count": sample_count,
            "labeled_sample_count": labeled_count,
            "pending_sample_count": sample_count - labeled_count,
            "overdue_sample_count": overdue_count,
            "independent_labeled_days": int(
                (summary["independent_labeled_days"] if summary else 0) or 0
            ),
            "label_coverage_pct": round(
                (labeled_count / sample_count) * 100,
                2,
            ) if sample_count else 0.0,
            "first_generated_at": summary["first_generated_at"] if summary else None,
            "last_generated_at": summary["last_generated_at"] if summary else None,
            "first_labeled_at": summary["first_labeled_at"] if summary else None,
            "last_labeled_at": summary["last_labeled_at"] if summary else None,
            "next_due_at": (
                min(pending_due_dates).isoformat() if pending_due_dates else None
            ),
            "latest_outcome": (
                {
                    "due_at": latest_outcome["due_at"],
                    "observed_at": latest_outcome["observed_at"],
                    "outcome_price": latest_outcome["outcome_price"],
                    "realized_return_pct": round(
                        float(latest_outcome["realized_return_pct"]),
                        4,
                    ),
                    "label_lag_minutes": round(
                        float(latest_outcome["label_lag_minutes"]),
                        2,
                    ),
                    "event_target_return_pct": latest_outcome[
                        "event_target_return_pct"
                    ],
                    "event_happened": (
                        bool(latest_outcome["event_happened"])
                        if latest_outcome["event_happened"] is not None
                        else None
                    ),
                }
                if latest_outcome
                else None
            ),
            "latest_derivatives": (
                json.loads(latest["derivatives_json"]) if latest else None
            ),
            "latest_news_sentiment": (
                json.loads(latest["news_sentiment_json"]) if latest else None
            ),
            "latest_model": json.loads(latest["model_json"]) if latest else None,
        }

    def performance_summary(
        self,
        coin_id: str,
        horizon_days: int,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current_time = now or datetime.now(timezone.utc)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        current_time = current_time.astimezone(timezone.utc)

        with self._connect() as connection:
            rows = self._execute(
                connection,
                """
                SELECT
                    snapshot.generated_at,
                    snapshot.model_json,
                    outcome.observed_at,
                    outcome.realized_return_pct,
                    outcome.event_happened
                FROM feature_outcome AS outcome
                JOIN feature_snapshot AS snapshot
                    ON snapshot.id = outcome.feature_snapshot_id
                WHERE snapshot.coin_id = ? AND snapshot.horizon_days = ?
                ORDER BY outcome.observed_at
                """,
                (coin_id, int(horizon_days)),
            ).fetchall()

        records = []
        for row in rows:
            model = json.loads(row["model_json"])
            expected_change = _optional_float(model.get("expected_change_pct"))
            realized_return = _optional_float(row["realized_return_pct"])
            if expected_change is None or realized_return is None:
                continue
            observed_at = _utc_datetime(row["observed_at"])
            if observed_at > current_time:
                continue
            interval = model.get("prediction_interval") or {}
            probability = model.get("probability") or {}
            probability_pct = _optional_float(probability.get("probability_pct"))
            baseline_probability_pct = _optional_float(
                probability.get("baseline_probability_pct")
            )
            records.append(
                {
                    "observed_at": observed_at,
                    "expected_change_pct": expected_change,
                    "realized_return_pct": realized_return,
                    "predicted_direction": (
                        model.get("direction_key")
                        or _direction_key(expected_change, int(horizon_days))
                    ),
                    "lower_change_pct": _optional_float(
                        interval.get("lower_change_pct")
                    ),
                    "upper_change_pct": _optional_float(
                        interval.get("upper_change_pct")
                    ),
                    "event_happened": row["event_happened"],
                    "event_probability": (
                        probability_pct / 100 if probability_pct is not None else None
                    ),
                    "baseline_probability": (
                        baseline_probability_pct / 100
                        if baseline_probability_pct is not None
                        else None
                    ),
                }
            )

        windows = []
        for days in LIVE_PERFORMANCE_WINDOWS:
            cutoff = current_time - timedelta(days=days)
            window_records = [item for item in records if item["observed_at"] >= cutoff]
            windows.append({"days": days, **_performance_metrics(window_records, horizon_days)})

        return {
            "horizon_days": int(horizon_days),
            "generated_at": current_time.isoformat(),
            "all_time": _performance_metrics(records, horizon_days),
            "windows": windows,
            "methodology": (
                "Csak a valóban publikált, lejárt előrejelzések kerülnek be. "
                "A MAE-t a változatlan árat feltételező alapmodellel, a "
                "valószínűséget Brier-score alapján hasonlítjuk össze."
            ),
        }

    def recent(self, coin_id: str, horizon_days: int, limit: int = 12) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = self._execute(
                connection,
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
