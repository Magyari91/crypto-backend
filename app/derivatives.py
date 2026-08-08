from datetime import datetime, timezone
from math import isfinite
from statistics import mean
from typing import Any


DAY_MS = 86_400_000


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


def _timestamp(value: Any) -> int | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return timestamp if timestamp > 0 else None


def _daily_average(
    rows: list[dict[str, Any]],
    timestamp_key: str,
    value_key: str,
    multiplier: float = 1.0,
) -> list[list[float]]:
    daily: dict[int, list[float]] = {}
    for row in rows:
        timestamp = _timestamp(row.get(timestamp_key))
        value = _number(row.get(value_key))
        if timestamp is None or value is None:
            continue
        day = timestamp - timestamp % DAY_MS
        daily.setdefault(day, []).append(value * multiplier)
    return [
        [timestamp, mean(values)]
        for timestamp, values in sorted(daily.items())
        if values
    ]


def _latest_number(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in reversed(rows):
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _change_pct(values: list[float], lookback: int) -> float | None:
    if len(values) < 2:
        return None
    reference_index = max(0, len(values) - lookback - 1)
    reference = values[reference_index]
    if reference <= 0:
        return None
    return ((values[-1] / reference) - 1) * 100


def _iso_timestamp(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat()


def normalize_derivatives(
    funding_rows: list[dict[str, Any]] | None = None,
    open_interest_rows: list[dict[str, Any]] | None = None,
    long_short_rows: list[dict[str, Any]] | None = None,
    taker_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    funding_rows = funding_rows or []
    open_interest_rows = open_interest_rows or []
    long_short_rows = long_short_rows or []
    taker_rows = taker_rows or []

    funding_rates = _daily_average(
        funding_rows,
        "fundingTime",
        "fundingRate",
        multiplier=100.0,
    )
    funding_values = [float(row[1]) for row in funding_rates]
    open_interest_values = [
        value
        for row in open_interest_rows
        if (value := _number(row.get("sumOpenInterestValue"))) is not None
    ]
    latest_long_ratio = _latest_number(long_short_rows, "longShortRatio")
    latest_long_account = _latest_number(long_short_rows, "longAccount")
    latest_short_account = _latest_number(long_short_rows, "shortAccount")
    latest_buy_volume = _latest_number(taker_rows, "buyVol")
    latest_sell_volume = _latest_number(taker_rows, "sellVol")
    latest_taker_ratio = _latest_number(taker_rows, "buySellRatio")
    taker_total = (latest_buy_volume or 0.0) + (latest_sell_volume or 0.0)

    timestamps = [
        timestamp
        for timestamp in (
            _timestamp(funding_rows[-1].get("fundingTime")) if funding_rows else None,
            _timestamp(open_interest_rows[-1].get("timestamp")) if open_interest_rows else None,
            _timestamp(long_short_rows[-1].get("timestamp")) if long_short_rows else None,
            _timestamp(taker_rows[-1].get("timestamp")) if taker_rows else None,
        )
        if timestamp is not None
    ]
    available = bool(funding_values or open_interest_values or long_short_rows or taker_rows)

    return {
        "funding_rates": funding_rates,
        "snapshot": {
            "available": available,
            "source": "Binance USDⓈ-M Futures",
            "updated_at": _iso_timestamp(max(timestamps) if timestamps else None),
            "funding_rate_pct": round(funding_values[-1], 6) if funding_values else None,
            "funding_7d_avg_pct": (
                round(mean(funding_values[-7:]), 6) if funding_values else None
            ),
            "funding_history_days": len(funding_values),
            "open_interest_usd": (
                round(open_interest_values[-1], 2) if open_interest_values else None
            ),
            "open_interest_change_7d_pct": (
                round(_change_pct(open_interest_values, 7), 2)
                if _change_pct(open_interest_values, 7) is not None
                else None
            ),
            "long_short_ratio": (
                round(latest_long_ratio, 4) if latest_long_ratio is not None else None
            ),
            "long_account_pct": (
                round(latest_long_account * 100, 2)
                if latest_long_account is not None
                else None
            ),
            "short_account_pct": (
                round(latest_short_account * 100, 2)
                if latest_short_account is not None
                else None
            ),
            "taker_buy_sell_ratio": (
                round(latest_taker_ratio, 4) if latest_taker_ratio is not None else None
            ),
            "taker_buy_share_pct": (
                round((latest_buy_volume or 0.0) / taker_total * 100, 2)
                if taker_total > 0
                else None
            ),
            "history_window_days": min(
                30,
                max(len(open_interest_rows), len(long_short_rows), len(taker_rows)),
            ),
        },
    }
