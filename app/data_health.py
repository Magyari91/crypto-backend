from typing import Any

from app.assets import ANALYSIS_ASSETS
from app.snapshot_schedule import SLOT_MINUTES, SNAPSHOT_TARGETS
from app.training_readiness import build_training_readiness


HORIZONS = (1, 7, 30)


def _empty_feature_status() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "labeled_sample_count": 0,
        "pending_sample_count": 0,
        "overdue_sample_count": 0,
        "independent_labeled_days": 0,
        "label_coverage_pct": 0.0,
        "first_generated_at": None,
        "last_generated_at": None,
        "first_labeled_at": None,
        "last_labeled_at": None,
        "next_due_at": None,
    }


def build_data_health_payload(health: dict[str, Any]) -> dict[str, Any]:
    rows_by_key = {
        (row["coin_id"], int(row["horizon_days"])): row
        for row in health["datasets"]
    }
    datasets = []
    horizons = []
    storage = health["storage"]
    for horizon_days in HORIZONS:
        horizon_rows = []
        for coin_id, metadata in ANALYSIS_ASSETS.items():
            feature_status = rows_by_key.get(
                (coin_id, horizon_days),
                _empty_feature_status(),
            )
            readiness = build_training_readiness(
                feature_status,
                storage,
                horizon_days,
            )
            dataset = {
                "coin_id": coin_id,
                **metadata,
                "horizon_days": horizon_days,
                **feature_status,
                "training": {
                    key: readiness[key]
                    for key in (
                        "status",
                        "ready_for_training",
                        "minimum_independent_labels",
                        "remaining_independent_labels",
                        "progress_pct",
                    )
                },
            }
            datasets.append(dataset)
            horizon_rows.append(dataset)

        horizons.append(
            {
                "horizon_days": horizon_days,
                "expected_dataset_count": len(ANALYSIS_ASSETS),
                "active_dataset_count": sum(
                    row["sample_count"] > 0 for row in horizon_rows
                ),
                "snapshot_count": sum(row["sample_count"] for row in horizon_rows),
                "outcome_count": sum(
                    row["labeled_sample_count"] for row in horizon_rows
                ),
                "overdue_count": sum(
                    row["overdue_sample_count"] for row in horizon_rows
                ),
                "ready_dataset_count": sum(
                    row["training"]["ready_for_training"] for row in horizon_rows
                ),
            }
        )

    expected_dataset_count = len(ANALYSIS_ASSETS) * len(HORIZONS)
    active_dataset_count = int(health["totals"]["active_dataset_count"])
    return {
        **health,
        "collector": {
            **health["collector"],
            "interval_minutes": SLOT_MINUTES,
            "rotation_slots": len(SNAPSHOT_TARGETS),
        },
        "totals": {
            **health["totals"],
            "expected_dataset_count": expected_dataset_count,
            "dataset_coverage_pct": round(
                active_dataset_count / expected_dataset_count * 100,
                2,
            ),
        },
        "horizons": horizons,
        "datasets": datasets,
    }
