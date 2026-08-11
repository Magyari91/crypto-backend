from typing import Any

from app.probability_models import PROBABILITY_REGISTRY
from app.specialist_models import SPECIALIST_REGISTRY


def build_training_readiness(
    feature_status: dict[str, Any],
    storage: dict[str, Any],
    horizon_days: int,
) -> dict[str, Any]:
    probability_minimum = PROBABILITY_REGISTRY[horizon_days].min_samples
    specialist_minimum = SPECIALIST_REGISTRY[horizon_days].min_samples
    minimum_independent_labels = max(probability_minimum, specialist_minimum)
    sample_count = int(feature_status.get("sample_count") or 0)
    labeled_count = int(feature_status.get("labeled_sample_count") or 0)
    independent_days = int(feature_status.get("independent_labeled_days") or 0)
    overdue_count = int(feature_status.get("overdue_sample_count") or 0)
    persistent = bool(storage.get("persistent"))
    remaining = max(0, minimum_independent_labels - independent_days)
    progress = min(
        100.0,
        (independent_days / minimum_independent_labels) * 100,
    )

    if not persistent:
        status = "storage_required"
        reason = (
            "A minták az újraindításkor elvesznek. Tartós PostgreSQL szükséges "
            "az élő modell tanításához."
        )
    elif sample_count == 0:
        status = "collecting"
        reason = "Az első pont-időbeli feature minták gyűjtése folyamatban van."
    elif overdue_count:
        status = "labeling_delayed"
        reason = (
            f"{overdue_count} lejárt minta vár kimeneti árra; a következő collector "
            "futás címkézi őket."
        )
    elif labeled_count == 0:
        status = "maturing"
        reason = (
            f"A minták gyűlnek, az első {horizon_days} napos kimenetelek még nem "
            "jártak le."
        )
    elif independent_days < minimum_independent_labels:
        status = "collecting_labels"
        reason = (
            f"Még {remaining} független, lezárt nap szükséges a tanítási kapuhoz."
        )
    else:
        status = "ready"
        reason = (
            "A pont-időbeli adatkészlet elérte a tanítási minimumot; a jelölt modell "
            "külön holdout ellenőrzésre bocsátható."
        )

    return {
        "status": status,
        "ready_for_training": status == "ready",
        "reason": reason,
        "horizon_days": horizon_days,
        "sample_count": sample_count,
        "labeled_sample_count": labeled_count,
        "independent_labeled_days": independent_days,
        "overdue_sample_count": overdue_count,
        "label_coverage_pct": float(feature_status.get("label_coverage_pct") or 0),
        "minimum_independent_labels": minimum_independent_labels,
        "remaining_independent_labels": remaining,
        "progress_pct": round(progress, 2),
        "next_due_at": feature_status.get("next_due_at"),
        "last_labeled_at": feature_status.get("last_labeled_at"),
        "storage": storage,
        "candidate_minimums": {
            "probability": probability_minimum,
            "specialist": specialist_minimum,
        },
    }
