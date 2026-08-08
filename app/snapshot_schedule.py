from datetime import datetime, timezone
from typing import NamedTuple


class SnapshotTarget(NamedTuple):
    coin: str
    horizon: int


SLOT_MINUTES = 15

# Every supported pair is sampled at least once per five-hour rotation. The
# shorter Bitcoin and Ethereum horizons receive extra observations.
SNAPSHOT_TARGETS = (
    SnapshotTarget("bitcoin", 1),
    SnapshotTarget("ethereum", 1),
    SnapshotTarget("bitcoin", 7),
    SnapshotTarget("solana", 1),
    SnapshotTarget("bitcoin", 1),
    SnapshotTarget("ripple", 1),
    SnapshotTarget("ethereum", 7),
    SnapshotTarget("dogecoin", 1),
    SnapshotTarget("bitcoin", 30),
    SnapshotTarget("solana", 7),
    SnapshotTarget("ethereum", 1),
    SnapshotTarget("ripple", 7),
    SnapshotTarget("bitcoin", 7),
    SnapshotTarget("dogecoin", 7),
    SnapshotTarget("solana", 30),
    SnapshotTarget("bitcoin", 1),
    SnapshotTarget("ethereum", 30),
    SnapshotTarget("ripple", 30),
    SnapshotTarget("bitcoin", 7),
    SnapshotTarget("dogecoin", 30),
)


def scheduled_snapshot_target(moment: datetime | None = None) -> SnapshotTarget:
    current = moment or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("Snapshot scheduling requires a timezone-aware datetime")
    slot_seconds = SLOT_MINUTES * 60
    slot = int(current.timestamp()) // slot_seconds
    return SNAPSHOT_TARGETS[slot % len(SNAPSHOT_TARGETS)]
