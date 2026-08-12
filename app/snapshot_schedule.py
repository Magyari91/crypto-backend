from datetime import datetime, timezone
from typing import NamedTuple

from app.assets import ANALYSIS_ASSETS


class SnapshotTarget(NamedTuple):
    coin: str
    horizon: int


SLOT_MINUTES = 15

# The 30 core slots cover every analysis asset and horizon in 7.5 hours. Ten
# additional liquid-asset slots keep the highest-traffic forecasts denser.
_CORE_TARGETS = tuple(
    SnapshotTarget(coin, horizon)
    for horizon in (1, 7, 30)
    for coin in ANALYSIS_ASSETS
)
_EXTRA_TARGETS = (
    SnapshotTarget("bitcoin", 1),
    SnapshotTarget("ethereum", 1),
    SnapshotTarget("bitcoin", 7),
    SnapshotTarget("ethereum", 7),
    SnapshotTarget("bitcoin", 30),
    SnapshotTarget("ethereum", 30),
    SnapshotTarget("solana", 1),
    SnapshotTarget("ripple", 1),
    SnapshotTarget("binancecoin", 1),
    SnapshotTarget("dogecoin", 1),
)
SNAPSHOT_TARGETS = _CORE_TARGETS + _EXTRA_TARGETS


def scheduled_snapshot_target(moment: datetime | None = None) -> SnapshotTarget:
    current = moment or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("Snapshot scheduling requires a timezone-aware datetime")
    slot_seconds = SLOT_MINUTES * 60
    slot = int(current.timestamp()) // slot_seconds
    return SNAPSHOT_TARGETS[slot % len(SNAPSHOT_TARGETS)]
