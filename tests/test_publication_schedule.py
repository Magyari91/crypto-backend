from datetime import datetime, timezone

import pytest

from app.publication_schedule import (
    next_publication_at,
    publication_horizons,
    publication_schedule_payload,
)


def test_daily_publication_is_always_due_and_weekly_monthly_follow_calendar():
    regular_day = datetime(2026, 8, 19, 0, 12, tzinfo=timezone.utc)
    first_monday = datetime(2026, 6, 1, 0, 12, tzinfo=timezone.utc)

    assert publication_horizons(regular_day) == (1,)
    assert publication_horizons(first_monday) == (1, 7, 30)


def test_next_publications_are_after_the_current_instant():
    current = datetime(2026, 8, 21, 0, 13, tzinfo=timezone.utc)

    assert next_publication_at(1, current) == datetime(
        2026, 8, 22, 0, 12, tzinfo=timezone.utc
    )
    assert next_publication_at(7, current) == datetime(
        2026, 8, 24, 0, 12, tzinfo=timezone.utc
    )
    assert next_publication_at(30, current) == datetime(
        2026, 9, 1, 0, 12, tzinfo=timezone.utc
    )


def test_publication_schedule_payload_exposes_all_horizons():
    payload = publication_schedule_payload(
        datetime(2026, 8, 21, 0, 13, tzinfo=timezone.utc)
    )

    assert payload["timezone"] == "UTC"
    assert payload["trigger_cron"] == "12 0 * * *"
    assert [rule["horizon_days"] for rule in payload["rules"]] == [1, 7, 30]


def test_publication_schedule_rejects_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        publication_horizons(datetime(2026, 8, 21, 0, 12))
