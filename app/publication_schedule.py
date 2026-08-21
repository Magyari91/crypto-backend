from datetime import datetime, time, timedelta, timezone
from typing import NamedTuple


PUBLICATION_HOUR_UTC = 0
PUBLICATION_MINUTE_UTC = 12
PUBLICATION_CRON = "12 0 * * *"


class PublicationRule(NamedTuple):
    horizon_days: int
    cadence: str
    schedule: str


PUBLICATION_RULES = (
    PublicationRule(1, "daily", "Every day"),
    PublicationRule(7, "weekly", "Every Monday"),
    PublicationRule(30, "monthly", "The first day of every month"),
)


def _utc_moment(moment: datetime | None = None) -> datetime:
    current = moment or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError(
            "Forecast publication scheduling requires a timezone-aware datetime"
        )
    return current.astimezone(timezone.utc)


def publication_horizons(moment: datetime | None = None) -> tuple[int, ...]:
    current = _utc_moment(moment)
    horizons = [1]
    if current.weekday() == 0:
        horizons.append(7)
    if current.day == 1:
        horizons.append(30)
    return tuple(horizons)


def next_publication_at(
    horizon_days: int,
    moment: datetime | None = None,
) -> datetime:
    current = _utc_moment(moment)
    if horizon_days not in {rule.horizon_days for rule in PUBLICATION_RULES}:
        raise ValueError("The publication horizon must be 1, 7, or 30 days")

    publication_time = time(
        PUBLICATION_HOUR_UTC,
        PUBLICATION_MINUTE_UTC,
        tzinfo=timezone.utc,
    )
    candidate = datetime.combine(current.date(), publication_time)

    if horizon_days == 1:
        if candidate <= current:
            candidate += timedelta(days=1)
        return candidate

    if horizon_days == 7:
        candidate += timedelta(days=(-current.weekday()) % 7)
        if candidate <= current:
            candidate += timedelta(days=7)
        return candidate

    candidate = candidate.replace(day=1)
    if candidate <= current:
        year = candidate.year + int(candidate.month == 12)
        month = 1 if candidate.month == 12 else candidate.month + 1
        candidate = candidate.replace(year=year, month=month, day=1)
    return candidate


def publication_rule_payload(
    horizon_days: int,
    moment: datetime | None = None,
) -> dict[str, str | int]:
    current = _utc_moment(moment)
    rule = next(
        (item for item in PUBLICATION_RULES if item.horizon_days == horizon_days),
        None,
    )
    if rule is None:
        raise ValueError("The publication horizon must be 1, 7, or 30 days")
    return {
        "horizon_days": rule.horizon_days,
        "cadence": rule.cadence,
        "schedule": rule.schedule,
        "publication_time_utc": f"{PUBLICATION_HOUR_UTC:02d}:{PUBLICATION_MINUTE_UTC:02d}",
        "next_publication_at": next_publication_at(horizon_days, current).isoformat(),
    }


def publication_schedule_payload(
    moment: datetime | None = None,
) -> dict[str, object]:
    current = _utc_moment(moment)
    return {
        "generated_at": current.isoformat(),
        "timezone": "UTC",
        "trigger_cron": PUBLICATION_CRON,
        "market_close_reference": "00:00 UTC daily candle close",
        "rules": [
            publication_rule_payload(rule.horizon_days, current)
            for rule in PUBLICATION_RULES
        ],
    }


if __name__ == "__main__":
    print(" ".join(str(horizon) for horizon in publication_horizons()))
