from collections import Counter
from datetime import datetime, timezone
from math import sqrt
from statistics import mean
from typing import Any

from app.forecast import (
    DIRECTION_THRESHOLDS,
    MAX_CALIBRATION_SAMPLES,
    MINIMUM_FEATURE_DAYS,
    MODEL_NAME,
    MODEL_VERSION,
    apply_specialist_estimate,
    build_model_estimate,
    classify_direction,
    daily_points,
    daily_value_map,
    technical_snapshot,
)
from app.probability_models import (
    PROBABILITY_REGISTRY,
    binary_log_loss,
    brier_score,
    calibration_error,
    prepare_probability_data,
    probability_from_state,
    reliability_bins,
    safe_roc_auc,
    train_probability_model,
)
from app.specialist_models import (
    SPECIALIST_REGISTRY,
    prepare_specialist_data,
    specialist_estimate_from_state,
    train_specialist,
)


MINIMUM_TRAINING_DAYS = 61
MAX_BACKTEST_SAMPLES = 180


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _round_metric(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _agreement_bands(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bands = (
        ("38-49%", 38, 49),
        ("50-64%", 50, 64),
        ("65-78%", 65, 78),
    )
    output = []
    for label, lower, upper in bands:
        band_results = [item for item in results if lower <= item["confidence"] <= upper]
        matches = [item for item in band_results if item["predicted_direction"] != "neutral"]
        output.append(
            {
                "label": label,
                "samples": len(matches),
                "total_samples": len(band_results),
                "directional_accuracy": _round_metric(
                    mean(item["hit"] for item in matches) * 100 if matches else None
                ),
                "mae_pct": _round_metric(
                    mean(item["absolute_error_pct"] for item in matches) if matches else None
                ),
            }
        )
    return output


def walk_forward_backtest(
    prices: list[list[float]],
    horizon_days: int,
    volumes: list[list[float]] | None = None,
    max_samples: int = MAX_BACKTEST_SAMPLES,
    market_prices: list[list[float]] | None = None,
    funding_rates: list[list[float]] | None = None,
    minimum_refit_days: int | None = None,
) -> dict[str, Any]:
    if horizon_days not in {1, 7, 30}:
        raise ValueError("Az időtáv 1, 7 vagy 30 nap lehet.")

    points = daily_points(prices)
    last_anchor = len(points) - horizon_days - 1
    first_anchor = MINIMUM_TRAINING_DAYS - 1
    if last_anchor < first_anchor:
        raise ValueError("Nincs elegendő lezárt időszak a visszaméréshez.")

    first_anchor = max(first_anchor, last_anchor - max_samples + 1)
    values = [float(point["price"]) for point in points]
    volume_by_day = daily_value_map(volumes or [])
    volume_values = [
        volume_by_day.get(_parse_timestamp(point["timestamp"]).date().isoformat())
        for point in points
    ]
    funding_by_day = daily_value_map(funding_rates or [])
    funding_values = [
        funding_by_day.get(_parse_timestamp(point["timestamp"]).date().isoformat())
        for point in points
    ]
    market_by_day = daily_value_map(market_prices or prices)
    market_values = []
    previous_market_value = None
    for point, fallback_value in zip(points, values):
        day = _parse_timestamp(point["timestamp"]).date().isoformat()
        market_value = market_by_day.get(day)
        if market_value is not None and market_value > 0:
            previous_market_value = market_value
        market_values.append(float(previous_market_value or fallback_value))
    feature_first = MINIMUM_FEATURE_DAYS - 1
    snapshots = {
        anchor: technical_snapshot(
            values[: anchor + 1],
            horizon_days,
            volume_values[: anchor + 1],
        )
        for anchor in range(feature_first, last_anchor + 1)
    }
    calibration_samples = {
        anchor: {
            "regime": snapshots[anchor]["regime"],
            "candidates": snapshots[anchor]["candidates"],
            "actual_change_pct": (
                (values[anchor + horizon_days] / values[anchor]) - 1
            ) * 100,
        }
        for anchor in range(feature_first, last_anchor + 1)
        if anchor + horizon_days < len(values)
    }
    specialist_data = prepare_specialist_data(values, volume_values, horizon_days)
    probability_data = prepare_probability_data(
        values,
        volume_values,
        market_values,
        horizon_days,
        funding_values,
    )
    specialist_state = None
    specialist_last_refit = None
    specialist_refit_days = max(
        SPECIALIST_REGISTRY[horizon_days].refit_days,
        minimum_refit_days or 0,
    )
    probability_state = None
    probability_last_refit = None
    probability_refit_days = max(
        PROBABILITY_REGISTRY[horizon_days].refit_days,
        minimum_refit_days or 0,
    )
    results = []

    for anchor in range(first_anchor, last_anchor + 1):
        known_last_anchor = anchor - horizon_days
        known_samples = [
            calibration_samples[origin]
            for origin in range(feature_first, known_last_anchor + 1)
            if origin in calibration_samples
        ][-MAX_CALIBRATION_SAMPLES:]
        estimate = build_model_estimate(
            snapshots[anchor],
            known_samples,
            horizon_days,
        )
        technical_change = float(estimate["expected_change"])
        if (
            specialist_state is None
            or specialist_last_refit is None
            or anchor - specialist_last_refit >= specialist_refit_days
        ):
            specialist_state = train_specialist(
                specialist_data,
                known_through_origin=known_last_anchor,
                direction_threshold=DIRECTION_THRESHOLDS[horizon_days],
            )
            specialist_last_refit = anchor
        specialist = specialist_estimate_from_state(
            specialist_state,
            specialist_data.features_by_origin[anchor],
        )
        if (
            probability_state is None
            or probability_last_refit is None
            or anchor - probability_last_refit >= probability_refit_days
        ):
            probability_state = train_probability_model(
                probability_data,
                known_through_origin=known_last_anchor,
            )
            probability_last_refit = anchor
        probability = probability_from_state(
            probability_state,
            probability_data.features_by_origin.get(anchor),
        )
        estimate = apply_specialist_estimate(estimate, specialist, horizon_days)
        base_price = values[anchor]
        actual_price = values[anchor + horizon_days]
        actual_change = ((actual_price / base_price) - 1) * 100
        _, actual_direction = classify_direction(actual_change, horizon_days)
        expected_change = float(estimate["expected_change"])
        absolute_error = abs(expected_change - actual_change)
        technical_error = abs(technical_change - actual_change)
        event_happened = int(
            actual_change >= PROBABILITY_REGISTRY[horizon_days].target_return_pct
        )
        predicted_event = int(probability["probability"] >= 0.5)

        results.append(
            {
                "forecast_at": points[anchor]["timestamp"],
                "evaluated_at": points[anchor + horizon_days]["timestamp"],
                "base_price": round(base_price, 8),
                "target_price": round(base_price * (1 + expected_change / 100), 8),
                "actual_price": round(actual_price, 8),
                "predicted_change_pct": round(expected_change, 2),
                "actual_change_pct": round(actual_change, 2),
                "predicted_direction": estimate["direction_key"],
                "actual_direction": actual_direction,
                "confidence": round(estimate["confidence"]),
                "hit": estimate["direction_key"] == actual_direction,
                "absolute_error_pct": round(absolute_error, 2),
                "baseline_error_pct": round(abs(actual_change), 2),
                "technical_error_pct": round(technical_error, 2),
                "specialist_active": specialist["active"],
                "event_probability_pct": round(probability["probability"] * 100, 2),
                "candidate_probability_pct": (
                    round(probability["candidate_probability"] * 100, 2)
                    if probability["candidate_probability"] is not None
                    else None
                ),
                "baseline_probability_pct": round(
                    probability["baseline_probability"] * 100,
                    2,
                ),
                "probability_model_active": probability_state.active,
                "probability_candidate_key": probability_state.candidate_key,
                "buy_probability_threshold_pct": round(
                    probability_state.buy_threshold * 100,
                    2,
                ),
                "event_happened": bool(event_happened),
                "probability_hit": predicted_event == event_happened,
            }
        )

    model_mae = mean(item["absolute_error_pct"] for item in results)
    baseline_mae = mean(item["baseline_error_pct"] for item in results)
    technical_mae = mean(item["technical_error_pct"] for item in results)
    rmse = sqrt(mean(item["absolute_error_pct"] ** 2 for item in results))
    skill = ((baseline_mae - model_mae) / baseline_mae * 100) if baseline_mae else 0.0
    skill_vs_technical = (
        (technical_mae - model_mae) / technical_mae * 100
        if technical_mae
        else 0.0
    )
    active_results = [item for item in results if item["predicted_direction"] != "neutral"]
    active_accuracy = (
        mean(item["hit"] for item in active_results) * 100 if active_results else None
    )
    specialist_results = [item for item in results if item["specialist_active"]]
    event_actual = [int(item["event_happened"]) for item in results]
    event_probabilities = [item["event_probability_pct"] / 100 for item in results]
    baseline_probabilities = [
        item["baseline_probability_pct"] / 100 for item in results
    ]
    probability_brier = brier_score(event_actual, event_probabilities)
    baseline_probability_brier = mean(
        (probability - target) ** 2
        for target, probability in zip(event_actual, baseline_probabilities)
    )
    probability_brier_skill = (
        (baseline_probability_brier - probability_brier)
        / baseline_probability_brier
        * 100
        if baseline_probability_brier > 0
        else 0.0
    )
    probability_log_loss = binary_log_loss(event_actual, event_probabilities)
    probability_auc = safe_roc_auc(event_actual, event_probabilities)
    probability_calibration_error = calibration_error(
        event_actual,
        event_probabilities,
    )
    buy_results = [
        item
        for item in results
        if item["probability_model_active"]
        and item["event_probability_pct"] >= item["buy_probability_threshold_pct"]
    ]
    buy_precision = (
        mean(item["event_happened"] for item in buy_results) * 100
        if buy_results
        else None
    )
    positive_events = sum(event_actual)
    buy_recall = (
        sum(item["event_happened"] for item in buy_results) / positive_events * 100
        if positive_events
        else None
    )
    active_probability_results = [
        item for item in results if item["probability_model_active"]
    ]
    candidate_results = [
        item for item in results if item["candidate_probability_pct"] is not None
    ]
    challenger = None
    if candidate_results:
        candidate_actual = [int(item["event_happened"]) for item in candidate_results]
        candidate_probabilities = [
            item["candidate_probability_pct"] / 100 for item in candidate_results
        ]
        candidate_baselines = [
            item["baseline_probability_pct"] / 100 for item in candidate_results
        ]
        candidate_score = brier_score(candidate_actual, candidate_probabilities)
        candidate_baseline_score = mean(
            (probability - target) ** 2
            for target, probability in zip(candidate_actual, candidate_baselines)
        )
        candidate_skill = (
            (candidate_baseline_score - candidate_score)
            / candidate_baseline_score
            * 100
            if candidate_baseline_score > 0
            else 0.0
        )
        candidate_auc = safe_roc_auc(candidate_actual, candidate_probabilities)
        model_counts = Counter(
            item["probability_candidate_key"]
            for item in candidate_results
            if item["probability_candidate_key"]
        )
        challenger = {
            "samples": len(candidate_results),
            "brier_score": round(candidate_score, 4),
            "baseline_brier_score": round(candidate_baseline_score, 4),
            "brier_skill_pct": round(candidate_skill, 2),
            "log_loss": round(
                binary_log_loss(candidate_actual, candidate_probabilities),
                4,
            ),
            "roc_auc": round(candidate_auc, 4) if candidate_auc is not None else None,
            "calibration_error_pct": round(
                calibration_error(candidate_actual, candidate_probabilities) * 100,
                2,
            ),
            "model_usage": dict(model_counts),
        }

    return {
        "model": {
            "name": MODEL_NAME,
            "version": MODEL_VERSION,
            "method": "walk_forward",
            "minimum_training_days": MINIMUM_TRAINING_DAYS,
            "specialist": {
                "key": SPECIALIST_REGISTRY[horizon_days].key,
                "label": SPECIALIST_REGISTRY[horizon_days].label,
                "family": SPECIALIST_REGISTRY[horizon_days].family,
                "refit_days": specialist_refit_days,
            },
            "probability": {
                "key": PROBABILITY_REGISTRY[horizon_days].key,
                "label": PROBABILITY_REGISTRY[horizon_days].label,
                "target_return_pct": PROBABILITY_REGISTRY[horizon_days].target_return_pct,
                "refit_days": probability_refit_days,
                "calibration": "Platt",
            },
        },
        "horizon_days": horizon_days,
        "period": {
            "from": results[0]["forecast_at"],
            "to": results[-1]["evaluated_at"],
        },
        "summary": {
            "samples": len(results),
            "directional_accuracy": round(mean(item["hit"] for item in results) * 100, 2),
            "active_directional_accuracy": _round_metric(active_accuracy),
            "signal_coverage_pct": round(len(active_results) / len(results) * 100, 2),
            "mae_pct": round(model_mae, 2),
            "rmse_pct": round(rmse, 2),
            "baseline_mae_pct": round(baseline_mae, 2),
            "technical_mae_pct": round(technical_mae, 2),
            "skill_vs_baseline_pct": round(skill, 2),
            "skill_vs_technical_pct": round(skill_vs_technical, 2),
            "beats_baseline": model_mae < baseline_mae,
            "beats_technical": model_mae < technical_mae,
            "specialist_usage_pct": round(
                len(specialist_results) / len(results) * 100,
                2,
            ),
            "probability": {
                "target_return_pct": PROBABILITY_REGISTRY[horizon_days].target_return_pct,
                "brier_score": round(probability_brier, 4),
                "baseline_brier_score": round(baseline_probability_brier, 4),
                "brier_skill_pct": round(probability_brier_skill, 2),
                "log_loss": round(probability_log_loss, 4),
                "roc_auc": round(probability_auc, 4) if probability_auc is not None else None,
                "calibration_error_pct": round(probability_calibration_error * 100, 2),
                "buy_precision_pct": _round_metric(buy_precision),
                "buy_recall_pct": _round_metric(buy_recall),
                "buy_signal_coverage_pct": round(len(buy_results) / len(results) * 100, 2),
                "active_model_usage_pct": round(
                    len(active_probability_results) / len(results) * 100,
                    2,
                ),
                "challenger": challenger,
                "reliability_bins": reliability_bins(
                    event_actual,
                    event_probabilities,
                ),
            },
        },
        "agreement_bands": _agreement_bands(results),
        "recent_results": list(reversed(results[-8:])),
        "methodology": (
            "Minden tesztpont csak az addig elérhető adatokat használja. "
            "A horizont-specialista és a kalibrált valószínűségi modell időszakosan "
            "újratanul; egyik sem kapcsol be elkülönített holdouton és időbeli "
            "stabilitási kapun mért előny nélkül. A valószínűségi összehasonlítás "
            "alapja az adott időpontban ismert historikus eseményarány."
        ),
    }


def evaluate_journal(
    records: list[dict[str, Any]],
    prices: list[list[float]],
) -> list[dict[str, Any]]:
    points = daily_points(prices)
    dated_points = [
        (_parse_timestamp(point["timestamp"]).date(), float(point["price"]))
        for point in points
    ]
    evaluated = []

    for record in records:
        due_date = _parse_timestamp(record["due_at"]).date()
        actual_price = next(
            (price for point_date, price in dated_points if point_date >= due_date),
            None,
        )
        item = dict(record)
        if actual_price is None:
            item.update(
                {
                    "status": "pending",
                    "actual_price": None,
                    "actual_change_pct": None,
                    "actual_direction": None,
                    "hit": None,
                    "absolute_error_pct": None,
                    "event_happened": None,
                    "probability_hit": None,
                    "probability_brier": None,
                    "baseline_probability_brier": None,
                }
            )
        else:
            actual_change = ((actual_price / float(record["base_price"])) - 1) * 100
            _, actual_direction = classify_direction(actual_change, int(record["horizon_days"]))
            probability_evaluation = {
                "event_happened": None,
                "probability_hit": None,
                "probability_brier": None,
                "baseline_probability_brier": None,
            }
            event_probability = record.get("event_probability_pct")
            event_threshold = record.get("event_target_return_pct")
            if event_probability is not None and event_threshold is not None:
                event_happened = int(actual_change >= float(event_threshold))
                probability_value = float(event_probability) / 100
                baseline_value = float(
                    record.get("baseline_probability_pct") or event_probability
                ) / 100
                probability_evaluation = {
                    "event_happened": bool(event_happened),
                    "probability_hit": int(probability_value >= 0.5) == event_happened,
                    "probability_brier": round(
                        (probability_value - event_happened) ** 2,
                        5,
                    ),
                    "baseline_probability_brier": round(
                        (baseline_value - event_happened) ** 2,
                        5,
                    ),
                }
            item.update(
                {
                    "status": "evaluated",
                    "actual_price": round(actual_price, 8),
                    "actual_change_pct": round(actual_change, 2),
                    "actual_direction": actual_direction,
                    "hit": record["direction_key"] == actual_direction,
                    "absolute_error_pct": round(
                        abs(float(record["expected_change_pct"]) - actual_change),
                        2,
                    ),
                    **probability_evaluation,
                }
            )
        evaluated.append(item)

    return evaluated
