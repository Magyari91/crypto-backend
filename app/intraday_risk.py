from collections import OrderedDict
from dataclasses import dataclass
from statistics import mean
from typing import Any

from sklearn.ensemble import GradientBoostingRegressor

from app.intraday_models import (
    FEATURE_LABELS,
    FEATURE_NAMES,
    INTRADAY_HORIZON_HOURS,
    INTRADAY_LOOKBACK_HOURS,
    PreparedIntradayData,
    build_intraday_feature_vector,
    prepare_intraday_data,
)


RISK_QUANTILE = 0.80
RISK_MAX_TRAINING_SAMPLES = {1: 1800, 7: 1100}
RISK_MINIMUM_SAMPLES = {1: 700, 7: 550}
RISK_REFIT_HOURS = {1: 168, 7: 336}
RISK_RANGE_LIMITS = {1: 15.0, 7: 40.0}
MINIMUM_PINBALL_SKILL_PCT = 2.0
MINIMUM_COVERAGE_PCT = 68.0
MAXIMUM_COVERAGE_PCT = 92.0
MAX_LIVE_CACHE_ENTRIES = 20


@dataclass
class IntradayRiskState:
    horizon_days: int
    estimator: Any | None
    active: bool
    available: bool
    training_samples: int
    holdout_samples: int
    pinball_skill_pct: float
    holdout_coverage_pct: float
    stability_skill_pct: float
    positive_stability_folds: int
    stability_folds: int
    baseline_range_pct: float
    residuals: list[float]
    top_features: list[dict[str, Any]]
    reason: str


_LIVE_RISK_CACHE: OrderedDict[tuple[Any, ...], IntradayRiskState] = OrderedDict()


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _pinball_loss(
    actual: list[float],
    predicted: list[float],
    probability: float = RISK_QUANTILE,
) -> float:
    losses = []
    for observed, estimate in zip(actual, predicted):
        error = observed - estimate
        losses.append(max(probability * error, (probability - 1) * error))
    return mean(losses)


def _make_model() -> GradientBoostingRegressor:
    return GradientBoostingRegressor(
        loss="quantile",
        alpha=RISK_QUANTILE,
        n_estimators=120,
        learning_rate=0.035,
        max_depth=2,
        min_samples_leaf=18,
        subsample=0.85,
        random_state=42,
    )


def _fit_model(
    features: list[list[float]],
    targets: list[float],
) -> GradientBoostingRegressor:
    weights = [0.55 + 0.45 * ((index + 1) / len(features)) for index in range(len(features))]
    model = _make_model()
    model.fit(features, targets, sample_weight=weights)
    return model


def _predict_ranges(
    model: GradientBoostingRegressor,
    features: list[list[float]],
    horizon_days: int,
) -> list[float]:
    limit = RISK_RANGE_LIMITS[horizon_days]
    return [max(0.05, min(float(value), limit)) for value in model.predict(features)]


def _feature_importance(model: GradientBoostingRegressor) -> list[dict[str, Any]]:
    values = [abs(float(value)) for value in model.feature_importances_]
    total = sum(values)
    if total <= 0:
        return []
    ranked = sorted(zip(FEATURE_NAMES, values), key=lambda item: item[1], reverse=True)
    return [
        {
            "key": name,
            "label": FEATURE_LABELS[name],
            "importance_pct": round(value / total * 100, 1),
        }
        for name, value in ranked[:4]
    ]


def _empty_state(
    horizon_days: int,
    samples: int,
    reason: str,
) -> IntradayRiskState:
    return IntradayRiskState(
        horizon_days=horizon_days,
        estimator=None,
        active=False,
        available=False,
        training_samples=samples,
        holdout_samples=0,
        pinball_skill_pct=0.0,
        holdout_coverage_pct=0.0,
        stability_skill_pct=0.0,
        positive_stability_folds=0,
        stability_folds=0,
        baseline_range_pct=0.0,
        residuals=[],
        top_features=[],
        reason=reason,
    )


def _stability_metrics(
    prepared: PreparedIntradayData,
    origins: list[int],
) -> tuple[float, int, int]:
    horizon_days = prepared.horizon_days
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    features = prepared.features_by_origin
    targets = prepared.targets_by_origin
    block_size = max(60, round(len(origins) * 0.08))
    skills = []

    for fraction in (0.55, 0.70, 0.85):
        validation_index = round(len(origins) * fraction)
        validation_start = origins[validation_index]
        training = [
            origin
            for origin in origins[:validation_index]
            if origin + horizon_hours < validation_start
        ]
        validation = origins[
            validation_index : min(validation_index + block_size, len(origins))
        ]
        if min(len(training), len(validation)) < 60:
            continue
        training_targets = [abs(targets[origin]) for origin in training]
        validation_targets = [abs(targets[origin]) for origin in validation]
        baseline_range = _quantile(training_targets, RISK_QUANTILE)
        model = _fit_model(
            [features[origin] for origin in training],
            training_targets,
        )
        predictions = _predict_ranges(
            model,
            [features[origin] for origin in validation],
            horizon_days,
        )
        model_loss = _pinball_loss(validation_targets, predictions)
        baseline_loss = _pinball_loss(
            validation_targets,
            [baseline_range] * len(validation_targets),
        )
        skills.append(
            (baseline_loss - model_loss) / baseline_loss * 100
            if baseline_loss > 0
            else 0.0
        )

    return (
        mean(skills) if skills else 0.0,
        sum(skill > 0 for skill in skills),
        len(skills),
    )


def train_intraday_risk(
    prepared: PreparedIntradayData,
    known_through_origin: int,
) -> IntradayRiskState:
    horizon_days = prepared.horizon_days
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    origins = sorted(
        origin
        for origin in prepared.targets_by_origin
        if origin <= known_through_origin
    )[-RISK_MAX_TRAINING_SAMPLES[horizon_days] :]
    if len(origins) < RISK_MINIMUM_SAMPLES[horizon_days]:
        return _empty_state(
            horizon_days,
            len(origins),
            "Nincs elég lezárt órás minta a kockázati modellhez.",
        )

    holdout_start = origins[round(len(origins) * 0.80)]
    training = [origin for origin in origins if origin + horizon_hours < holdout_start]
    holdout = [origin for origin in origins if origin >= holdout_start]
    if min(len(training), len(holdout)) < 60:
        return _empty_state(
            horizon_days,
            len(origins),
            "Nincs elég minta a tisztított kockázati holdouthoz.",
        )

    features = prepared.features_by_origin
    targets = prepared.targets_by_origin
    training_targets = [abs(targets[origin]) for origin in training]
    holdout_targets = [abs(targets[origin]) for origin in holdout]
    baseline_range = _quantile(training_targets, RISK_QUANTILE)
    holdout_model = _fit_model(
        [features[origin] for origin in training],
        training_targets,
    )
    predictions = _predict_ranges(
        holdout_model,
        [features[origin] for origin in holdout],
        horizon_days,
    )
    model_loss = _pinball_loss(holdout_targets, predictions)
    baseline_loss = _pinball_loss(
        holdout_targets,
        [baseline_range] * len(holdout_targets),
    )
    skill_pct = (
        (baseline_loss - model_loss) / baseline_loss * 100
        if baseline_loss > 0
        else 0.0
    )
    coverage = mean(
        observed <= estimate
        for observed, estimate in zip(holdout_targets, predictions)
    ) * 100
    stability_skill, positive_folds, stability_folds = _stability_metrics(
        prepared,
        training,
    )
    stable_across_folds = (
        stability_folds >= 3
        and positive_folds >= 2
        and stability_skill > 0
    )
    active = (
        skill_pct >= MINIMUM_PINBALL_SKILL_PCT
        and MINIMUM_COVERAGE_PCT <= coverage <= MAXIMUM_COVERAGE_PCT
        and stable_across_folds
    )

    if active:
        estimator = _fit_model(
            [features[origin] for origin in origins],
            [abs(targets[origin]) for origin in origins],
        )
        reason = "Az órás kvantilismodell a tisztított holdouton javította a mozgási sávot."
        top_features = _feature_importance(estimator)
    else:
        estimator = None
        top_features = _feature_importance(holdout_model)
        if not stable_across_folds:
            reason = "Az órás kockázati modell nem volt stabil több időrendi blokkon."
        elif skill_pct < MINIMUM_PINBALL_SKILL_PCT:
            reason = "Az órás kockázati modell még nem verte meg a historikus sávot."
        else:
            reason = "Az órás kockázati modell lefedettsége még nincs a célzónában."

    return IntradayRiskState(
        horizon_days=horizon_days,
        estimator=estimator,
        active=active,
        available=True,
        training_samples=len(origins),
        holdout_samples=len(holdout),
        pinball_skill_pct=round(skill_pct, 2),
        holdout_coverage_pct=round(coverage, 2),
        stability_skill_pct=round(stability_skill, 2),
        positive_stability_folds=positive_folds,
        stability_folds=stability_folds,
        baseline_range_pct=round(baseline_range, 4),
        residuals=[observed - estimate for observed, estimate in zip(holdout_targets, predictions)],
        top_features=top_features,
        reason=reason,
    )


def risk_estimate_from_state(
    state: IntradayRiskState,
    current_features: list[float],
) -> dict[str, Any]:
    predicted_range = state.baseline_range_pct
    if state.active and state.estimator is not None:
        predicted_range = _predict_ranges(
            state.estimator,
            [current_features],
            state.horizon_days,
        )[0]
    return {
        "key": f"intraday_quantile_{state.horizon_days}d",
        "label": f"{state.horizon_days} napos órás mozgási sáv",
        "family": "80%-os Gradient Boosting kvantilismodell",
        "available": state.available,
        "active": state.active,
        "range_pct": round(predicted_range, 2),
        "baseline_range_pct": round(state.baseline_range_pct, 2),
        "training_samples": state.training_samples,
        "holdout_samples": state.holdout_samples,
        "pinball_skill_pct": state.pinball_skill_pct,
        "holdout_coverage_pct": state.holdout_coverage_pct,
        "stability_skill_pct": state.stability_skill_pct,
        "positive_stability_folds": state.positive_stability_folds,
        "stability_folds": state.stability_folds,
        "target_coverage_pct": round(RISK_QUANTILE * 100),
        "top_features": state.top_features,
        "reason": state.reason,
        "data_resolution": "1h",
        "purge_hours": INTRADAY_HORIZON_HOURS[state.horizon_days],
    }


def build_intraday_risk_estimate(
    candles: list[dict[str, Any]],
    horizon_days: int,
    cache_key: str = "",
) -> dict[str, Any]:
    prepared = prepare_intraday_data(candles[:-1], horizon_days)
    if len(prepared.candles) < INTRADAY_LOOKBACK_HOURS:
        raise ValueError("Nincs elegendő órás adat a kockázati modellhez.")
    cache_signature = (
        cache_key,
        horizon_days,
        len(prepared.candles),
        int(prepared.candles[-1]["timestamp"]),
        round(prepared.candles[-1]["close"], 8),
    )
    state = _LIVE_RISK_CACHE.get(cache_signature)
    if state is None:
        state = train_intraday_risk(
            prepared,
            known_through_origin=(
                len(prepared.candles)
                - 1
                - INTRADAY_HORIZON_HOURS[horizon_days]
            ),
        )
        _LIVE_RISK_CACHE[cache_signature] = state
        _LIVE_RISK_CACHE.move_to_end(cache_signature)
        while len(_LIVE_RISK_CACHE) > MAX_LIVE_CACHE_ENTRIES:
            _LIVE_RISK_CACHE.popitem(last=False)
    current_prepared = prepare_intraday_data(candles, horizon_days)
    return risk_estimate_from_state(
        state,
        build_intraday_feature_vector(current_prepared.candles),
    )


def walk_forward_intraday_risk(
    candles: list[dict[str, Any]],
    horizon_days: int,
    max_samples: int = 60,
) -> dict[str, Any]:
    prepared = prepare_intraday_data(candles, horizon_days)
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    last_anchor = len(prepared.candles) - horizon_hours - 1
    anchors = list(
        range(last_anchor, INTRADAY_LOOKBACK_HOURS - 2, -24)
    )[:max_samples]
    anchors.reverse()
    state = None
    last_refit = None
    results = []

    for anchor in anchors:
        if (
            state is None
            or last_refit is None
            or anchor - last_refit >= RISK_REFIT_HOURS[horizon_days]
        ):
            state = train_intraday_risk(
                prepared,
                known_through_origin=anchor - horizon_hours,
            )
            last_refit = anchor
        estimate = risk_estimate_from_state(
            state,
            build_intraday_feature_vector(prepared.candles[: anchor + 1]),
        )
        actual_range = abs(
            (
                prepared.candles[anchor + horizon_hours]["close"]
                / prepared.candles[anchor]["close"]
            )
            - 1
        ) * 100
        results.append(
            {
                "predicted_range_pct": estimate["range_pct"],
                "baseline_range_pct": estimate["baseline_range_pct"],
                "actual_range_pct": actual_range,
                "covered": actual_range <= estimate["range_pct"],
                "active": estimate["active"],
            }
        )

    actual = [item["actual_range_pct"] for item in results]
    predicted = [item["predicted_range_pct"] for item in results]
    baseline = [item["baseline_range_pct"] for item in results]
    model_loss = _pinball_loss(actual, predicted)
    baseline_loss = _pinball_loss(actual, baseline)
    active_results = [item for item in results if item["active"]]
    if active_results:
        active_actual = [item["actual_range_pct"] for item in active_results]
        active_predicted = [item["predicted_range_pct"] for item in active_results]
        active_baseline = [item["baseline_range_pct"] for item in active_results]
        active_model_loss = _pinball_loss(active_actual, active_predicted)
        active_baseline_loss = _pinball_loss(active_actual, active_baseline)
        active_skill = (
            (active_baseline_loss - active_model_loss) / active_baseline_loss * 100
            if active_baseline_loss > 0
            else 0.0
        )
        active_coverage = mean(item["covered"] for item in active_results) * 100
    else:
        active_skill = None
        active_coverage = None

    return {
        "horizon_days": horizon_days,
        "summary": {
            "samples": len(results),
            "pinball_skill_vs_baseline_pct": round(
                (baseline_loss - model_loss) / baseline_loss * 100
                if baseline_loss > 0
                else 0.0,
                2,
            ),
            "coverage_pct": round(mean(item["covered"] for item in results) * 100, 2),
            "specialist_usage_pct": round(
                len(active_results) / len(results) * 100,
                2,
            ),
            "active_pinball_skill_pct": (
                round(active_skill, 2) if active_skill is not None else None
            ),
            "active_coverage_pct": (
                round(active_coverage, 2) if active_coverage is not None else None
            ),
        },
    }
