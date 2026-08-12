from datetime import datetime, timedelta, timezone

import pytest

from app.assets import ANALYSIS_ASSETS
from app.snapshot_schedule import SNAPSHOT_TARGETS, scheduled_snapshot_target


def test_snapshot_rotation_is_deterministic_and_advances_every_fifteen_minutes():
    start = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    first = scheduled_snapshot_target(start)
    same_slot = scheduled_snapshot_target(start + timedelta(minutes=14, seconds=59))
    next_slot = scheduled_snapshot_target(start + timedelta(minutes=15))

    assert first == same_slot
    assert next_slot != first


def test_snapshot_rotation_covers_every_supported_coin_and_horizon():
    pairs = set(SNAPSHOT_TARGETS)

    assert {target.coin for target in pairs} == set(ANALYSIS_ASSETS)
    for coin in {target.coin for target in pairs}:
        assert {target.horizon for target in pairs if target.coin == coin} == {1, 7, 30}

    assert len(SNAPSHOT_TARGETS) == 40


def test_snapshot_rotation_rejects_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        scheduled_snapshot_target(datetime(2026, 8, 8, 12, 0))
