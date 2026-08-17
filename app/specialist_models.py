from dataclasses import dataclass
from functools import lru_cache
from math import isfinite
from statistics import mean, stdev
from typing import Any

from sklearn.ensemble import ExtraTreesRegressor, GradientBoostingRegressor
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


FEATURE_LOOKBACK_DAYS = 61
SHRINKAGE_CANDIDATES = (0.0, 0.25, 0.5, 0.75, 1.0)


@dataclass(frozen=True)
class SpecialistSpec:
    key: str
    label: str
    family: str
    candidates: tuple[str, ...]
    min_samples: int
    max_training_samples: int
    refit_days: int
    point_limit: float
    target_clip: float
    minimum_skill_pct: float
    maximum_blend_weight: float


SPECIALIST_REGISTRY = {
    1: SpecialistSpec(
        key="huber_1d",
        label="1 napos modellverseny",
        family="Huber / Gradient Boosting / Extra Trees",
        candidates=("huber", "gradient_boosting", "extra_trees"),
        min_samples=100,
        max_training_samples=900,
        refit_days=7,
        point_limit=2.5,
        target_clip=10.0,
        minimum_skill_pct=2.0,
        maximum_blend_weight=0.75,
    ),
    7: SpecialistSpec(
        key="gradient_boosting_7d",
        label="7 napos modellverseny",
        family="Gradient Boosting / Extra Trees / Huber",
        candidates=("gradient_boosting", "extra_trees", "huber"),
        min_samples=140,
        max_training_samples=900,
        refit_days=14,
        point_limit=6.0,
        target_clip=25.0,
        minimum_skill_pct=1.5,
        maximum_blend_weight=0.70,
    ),
    30: SpecialistSpec(
        key="ridge_30d",
        label="30 napos modellverseny",
        family="Ridge / Gradient Boosting / Extra Trees",
        candidates=("ridge", "gradient_boosting", "extra_trees"),
        min_samples=180,
        max_training_samples=900,
        refit_days=30,
        point_limit=12.0,
        target_clip=55.0,
        minimum_skill_pct=1.0,
        maximum_blend_weight=0.65,
    ),
}


CANDIDATE_FAMILIES = {
    "huber": "Huber regresszió",
    "ridge": "Ridge regresszió",
    "gradient_boosting": "Huber Gradient Boosting",
    "extra_trees": "Regularizált Extra Trees",
}


FEATURE_NAMES = (
    "return_1d",
    "return_3d",
    "return_7d",
    "return_14d",
    "return_30d",
    "return_60d",
    "volatility_7d",
    "volatility_30d",
    "slope_20d",
    "slope_60d",
    "ema_5_20",
    "ema_20_50",
    "rsi_14",
    "price_zscore_20",
    "drawdown_30d",
    "volume_ratio_7_30",
    "volume_change_7d",
    "volume_available",
)


FEATURE_LABELS = {
    "return_1d": "1 napos hozam",
    "return_3d": "3 napos momentum",
    "return_7d": "7 napos momentum",
    "return_14d": "14 napos momentum",
    "return_30d": "30 napos momentum",
    "return_60d": "60 napos momentum",
    "volatility_7d": "7 napos volatilitás",
    "volatility_30d": "30 napos volatilitás",
    "slope_20d": "20 napos trendmeredekség",
    "slope_60d": "60 napos trendmeredekség",
    "ema_5_20": "EMA 5/20 eltérés",
    "ema_20_50": "EMA 20/50 eltérés",
    "rsi_14": "RSI (14)",
    "price_zscore_20": "20 napos ár-eltérés",
    "drawdown_30d": "30 napos visszaesés",
    "volume_ratio_7_30": "rövid/hosszú forgalom",
    "volume_change_7d": "7 napos forgalomváltozás",
    "volume_available": "forgalmi adatok teljessége",
}


@dataclass
class PreparedSpecialistData:
    horizon_days: int
    features_by_origin: dict[int, list[float]]
    targets_by_origin: dict[int, float]


@dataclass
class SpecialistState:
    spec: SpecialistSpec
    estimator: Any | None
    active: bool
    available: bool
    selected_model_key: str | None
    selected_model_family: str | None
    shrinkage: float
    blend_weight: float
    training_samples: int
    holdout_samples: int
    validation_skill_pct: float
    holdout_skill_pct: float
    holdout_directional_accuracy: float | None
    holdout_signal_coverage_pct: float
    residuals: list[float]
    top_features: list[dict[str, Any]]
    validation_candidates: list[dict[str, Any]]
    reason: str


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _return_pct(values: list[float], days: int) -> float:
    return ((values[-1] / values[-days - 1]) - 1) * 100


def _returns(values: list[float]) -> list[float]:
    return [((current / previous) - 1) * 100 for previous, current in zip(values, values[1:])]


def _ema_last(values: list[float], span: int) -> float:
    alpha = 2 / (span + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result
    return result


def _rsi(values: list[float], window: int = 14) -> float:
    recent = _returns(values[-(window + 1) :])
    gains = [max(value, 0.0) for value in recent]
    losses = [max(-value, 0.0) for value in recent]
    average_gain = mean(gains) if gains else 0.0
    average_loss = mean(losses) if losses else 0.0
    if average_gain == 0 and average_loss == 0:
        return 50.0
    if average_loss == 0:
        return 100.0
    return 100 - (100 / (1 + average_gain / average_loss))


def _normalized_slope(values: list[float], window: int) -> float:
    recent = values[-window:]
    x_middle = (len(recent) - 1) / 2
    y_middle = mean(recent)
    denominator = sum((index - x_middle) ** 2 for index in range(len(recent)))
    if denominator == 0 or y_middle == 0:
        return 0.0
    numerator = sum(
        (index - x_middle) * (value - y_middle)
        for index, value in enumerate(recent)
    )
    return numerator / denominator / y_middle * 100


def _volume_features(volumes: list[float | None]) -> tuple[float, float, float]:
    recent_30 = [
        float(value)
        for value in volumes[-30:]
        if value is not None and isfinite(float(value)) and float(value) > 0
    ]
    availability = len(recent_30) / min(30, len(volumes)) if volumes else 0.0
    if len(recent_30) < 14:
        return 1.0, 0.0, availability

    recent_7 = recent_30[-7:]
    long_average = mean(recent_30)
    ratio = mean(recent_7) / long_average if long_average > 0 else 1.0
    reference = recent_30[-8] if len(recent_30) >= 8 else recent_30[0]
    change = ((recent_30[-1] / reference) - 1) * 100 if reference > 0 else 0.0
    return _clamp(ratio, 0.2, 5.0), _clamp(change, -95.0, 400.0), availability


def build_feature_vector(
    values: list[float],
    volumes: list[float | None],
) -> list[float]:
    if len(values) < FEATURE_LOOKBACK_DAYS:
        raise ValueError("Legalább 61 napi adat szükséges a specialista modellhez.")

    daily_returns = _returns(values)
    recent_7 = daily_returns[-7:]
    recent_30 = daily_returns[-30:]
    ema5 = _ema_last(values, 5)
    ema20 = _ema_last(values, 20)
    ema50 = _ema_last(values, 50)
    recent_prices = values[-20:]
    price_average = mean(recent_prices)
    price_deviation = stdev(recent_prices) if len(recent_prices) > 1 else 0.0
    rolling_high = max(values[-30:])
    volume_ratio, volume_change, volume_available = _volume_features(volumes)

    return [
        _return_pct(values, 1),
        _return_pct(values, 3),
        _return_pct(values, 7),
        _return_pct(values, 14),
        _return_pct(values, 30),
        _return_pct(values, 60),
        stdev(recent_7) if len(recent_7) > 1 else 0.0,
        stdev(recent_30) if len(recent_30) > 1 else 0.0,
        _normalized_slope(values, 20),
        _normalized_slope(values, 60),
        ((ema5 / ema20) - 1) * 100 if ema20 else 0.0,
        ((ema20 / ema50) - 1) * 100 if ema50 else 0.0,
        _rsi(values),
        (values[-1] - price_average) / price_deviation if price_deviation else 0.0,
        ((values[-1] / rolling_high) - 1) * 100 if rolling_high else 0.0,
        volume_ratio,
        volume_change,
        volume_available,
    ]


def prepare_specialist_data(
    values: list[float],
    volumes: list[float | None],
    horizon_days: int,
) -> PreparedSpecialistData:
    if horizon_days not in SPECIALIST_REGISTRY:
        raise ValueError("Az időtáv 1, 7 vagy 30 nap lehet.")

    aligned_volumes = list(volumes[: len(values)])
    if len(aligned_volumes) < len(values):
        aligned_volumes.extend([None] * (len(values) - len(aligned_volumes)))

    features_by_origin = {}
    targets_by_origin = {}
    for origin in range(FEATURE_LOOKBACK_DAYS - 1, len(values)):
        features_by_origin[origin] = build_feature_vector(
            values[: origin + 1],
            aligned_volumes[: origin + 1],
        )
        if origin + horizon_days < len(values):
            targets_by_origin[origin] = (
                (values[origin + horizon_days] / values[origin]) - 1
            ) * 100

    return PreparedSpecialistData(
        horizon_days=horizon_days,
        features_by_origin=features_by_origin,
        targets_by_origin=targets_by_origin,
    )


def _make_estimator(model_key: str, horizon_days: int):
    if model_key == "huber":
        return Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "regressor",
                    HuberRegressor(
                        epsilon=1.35,
                        alpha=0.02,
                        max_iter=400,
                        tol=1e-5,
                    ),
                ),
            ]
        )
    if model_key == "gradient_boosting":
        return GradientBoostingRegressor(
            loss="huber",
            n_estimators=100 if horizon_days == 7 else 80,
            learning_rate=0.035 if horizon_days == 7 else 0.03,
            max_depth=2,
            min_samples_leaf=12,
            subsample=0.85,
            random_state=42,
        )
    if model_key == "extra_trees":
        return ExtraTreesRegressor(
            n_estimators=96,
            max_depth=5,
            min_samples_leaf=8,
            max_features=0.75,
            random_state=42,
            n_jobs=1,
        )
    if model_key == "ridge":
        return Pipeline(
            [
                ("scale", StandardScaler()),
                ("regressor", Ridge(alpha=24.0)),
            ]
        )
    raise ValueError(f"Ismeretlen specialista modelljelölt: {model_key}")


def _fit_estimator(estimator, features: list[list[float]], targets: list[float]):
    weights = [0.55 + 0.45 * ((index + 1) / len(features)) for index in range(len(features))]
    if isinstance(estimator, Pipeline):
        estimator.fit(features, targets, regressor__sample_weight=weights)
    else:
        estimator.fit(features, targets, sample_weight=weights)
    return estimator


def _predict(estimator, features: list[list[float]], spec: SpecialistSpec) -> list[float]:
    return [
        _clamp(float(value), -spec.point_limit, spec.point_limit)
        for value in estimator.predict(features)
    ]


def _mae(actual: list[float], predicted: list[float]) -> float:
    return mean(abs(expected - observed) for expected, observed in zip(predicted, actual))


def _direction(value: float, threshold: float) -> int:
    if value > threshold:
        return 1
    if value < -threshold:
        return -1
    return 0


def _feature_importance(estimator) -> list[dict[str, Any]]:
    fitted = estimator.named_steps["regressor"] if isinstance(estimator, Pipeline) else estimator
    raw = getattr(fitted, "feature_importances_", None)
    if raw is None:
        raw = getattr(fitted, "coef_", None)
    if raw is None:
        return []

    values = [abs(float(value)) for value in raw]
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
    spec: SpecialistSpec,
    training_samples: int,
    reason: str,
) -> SpecialistState:
    return SpecialistState(
        spec=spec,
        estimator=None,
        active=False,
        available=False,
        selected_model_key=None,
        selected_model_family=None,
        shrinkage=0.0,
        blend_weight=0.0,
        training_samples=training_samples,
        holdout_samples=0,
        validation_skill_pct=0.0,
        holdout_skill_pct=0.0,
        holdout_directional_accuracy=None,
        holdout_signal_coverage_pct=0.0,
        residuals=[],
        top_features=[],
        validation_candidates=[],
        reason=reason,
    )


def train_specialist(
    prepared: PreparedSpecialistData,
    known_through_origin: int,
    direction_threshold: float,
) -> SpecialistState:
    spec = SPECIALIST_REGISTRY[prepared.horizon_days]
    origins = sorted(
        origin
        for origin in prepared.targets_by_origin
        if origin <= known_through_origin
    )[-spec.max_training_samples :]
    if len(origins) < spec.min_samples:
        return _empty_state(
            spec,
            len(origins),
            f"Még legalább {spec.min_samples} lezárt tanítóminta szükséges.",
        )

    features = [prepared.features_by_origin[origin] for origin in origins]
    actual_targets = [prepared.targets_by_origin[origin] for origin in origins]
    fit_targets = [
        _clamp(value, -spec.target_clip, spec.target_clip)
        for value in actual_targets
    ]

    holdout_count = max(24, round(len(origins) * 0.20))
    training_end = len(origins) - holdout_count
    validation_count = max(18, round(training_end * 0.15))
    core_end = training_end - validation_count
    if core_end < 50:
        return _empty_state(spec, len(origins), "Nincs elég régi minta a háromrészes validációhoz.")

    calibration_actual = actual_targets[core_end:training_end]
    calibration_baseline_mae = _mae(
        calibration_actual,
        [0.0] * len(calibration_actual),
    )
    candidate_results = []
    for candidate_key in spec.candidates:
        calibration_model = _fit_estimator(
            _make_estimator(candidate_key, prepared.horizon_days),
            features[:core_end],
            fit_targets[:core_end],
        )
        calibration_predictions = _predict(
            calibration_model,
            features[core_end:training_end],
            spec,
        )
        candidate_shrinkage = min(
            SHRINKAGE_CANDIDATES,
            key=lambda candidate: _mae(
                calibration_actual,
                [prediction * candidate for prediction in calibration_predictions],
            ),
        )
        candidate_mae = _mae(
            calibration_actual,
            [prediction * candidate_shrinkage for prediction in calibration_predictions],
        )
        candidate_skill = (
            (calibration_baseline_mae - candidate_mae)
            / calibration_baseline_mae
            * 100
            if calibration_baseline_mae > 0
            else 0.0
        )
        candidate_results.append(
            {
                "key": candidate_key,
                "family": CANDIDATE_FAMILIES[candidate_key],
                "mae_pct": candidate_mae,
                "baseline_mae_pct": calibration_baseline_mae,
                "skill_vs_baseline_pct": candidate_skill,
                "shrinkage": candidate_shrinkage,
            }
        )

    selected_candidate = min(candidate_results, key=lambda item: item["mae_pct"])
    selected_model_key = str(selected_candidate["key"])
    selected_model_family = str(selected_candidate["family"])
    shrinkage = float(selected_candidate["shrinkage"])
    validation_skill_pct = float(selected_candidate["skill_vs_baseline_pct"])
    validation_candidates = [
        {
            **item,
            "mae_pct": round(float(item["mae_pct"]), 4),
            "baseline_mae_pct": round(float(item["baseline_mae_pct"]), 4),
            "skill_vs_baseline_pct": round(
                float(item["skill_vs_baseline_pct"]),
                2,
            ),
            "shrinkage": round(float(item["shrinkage"]), 2),
            "selected": item["key"] == selected_model_key,
        }
        for item in candidate_results
    ]

    holdout_model = _fit_estimator(
        _make_estimator(selected_model_key, prepared.horizon_days),
        features[:training_end],
        fit_targets[:training_end],
    )
    diagnostic_features = _feature_importance(holdout_model)
    holdout_predictions = [
        prediction * shrinkage
        for prediction in _predict(holdout_model, features[training_end:], spec)
    ]
    holdout_actual = actual_targets[training_end:]
    baseline_mae = _mae(holdout_actual, [0.0] * len(holdout_actual))
    model_mae = _mae(holdout_actual, holdout_predictions)
    holdout_skill_pct = (
        (baseline_mae - model_mae) / baseline_mae * 100
        if baseline_mae > 0
        else 0.0
    )

    active_pairs = [
        (_direction(prediction, direction_threshold), _direction(actual, direction_threshold))
        for prediction, actual in zip(holdout_predictions, holdout_actual)
        if _direction(prediction, direction_threshold) != 0
    ]
    active_accuracy = (
        mean(predicted == actual for predicted, actual in active_pairs) * 100
        if active_pairs
        else None
    )
    coverage = len(active_pairs) / len(holdout_actual) * 100
    minimum_active_samples = max(4, round(len(holdout_actual) * 0.05))
    active = (
        shrinkage > 0
        and validation_skill_pct > 0
        and holdout_skill_pct >= spec.minimum_skill_pct
        and len(active_pairs) >= minimum_active_samples
        and active_accuracy is not None
        and active_accuracy >= 52.0
    )

    if active:
        reason = (
            f"A {selected_model_family} nyerte a validációs versenyt, majd a "
            "külön holdouton is felülteljesítette a semleges alapmodellt."
        )
        estimator = _fit_estimator(
            _make_estimator(selected_model_key, prepared.horizon_days),
            features,
            fit_targets,
        )
        blend_weight = min(
            spec.maximum_blend_weight,
            0.35
            + max(min(validation_skill_pct, holdout_skill_pct), 0.0) / 25,
        )
        top_features = _feature_importance(estimator)
    else:
        estimator = None
        blend_weight = 0.0
        top_features = diagnostic_features
        if shrinkage == 0:
            reason = "A validáció szerint a semleges becslés volt pontosabb."
        elif validation_skill_pct <= 0:
            reason = "Egyik modelljelölt sem javított a validációs alapmodellen."
        elif holdout_skill_pct < spec.minimum_skill_pct:
            reason = "A validációs győztes holdout-előnye még nem érte el a bekapcsolási küszöböt."
        elif len(active_pairs) < minimum_active_samples:
            reason = "A specialista még túl kevés aktív jelzést adott."
        else:
            reason = "Az aktív irányjelzések találati aránya még nem megfelelő."

    residuals = [
        actual - prediction
        for prediction, actual in zip(holdout_predictions, holdout_actual)
    ]
    return SpecialistState(
        spec=spec,
        estimator=estimator,
        active=active,
        available=True,
        selected_model_key=selected_model_key,
        selected_model_family=selected_model_family,
        shrinkage=shrinkage,
        blend_weight=blend_weight,
        training_samples=len(origins),
        holdout_samples=len(holdout_actual),
        validation_skill_pct=round(validation_skill_pct, 2),
        holdout_skill_pct=round(holdout_skill_pct, 2),
        holdout_directional_accuracy=(
            round(active_accuracy, 2) if active_accuracy is not None else None
        ),
        holdout_signal_coverage_pct=round(coverage, 2),
        residuals=residuals,
        top_features=top_features,
        validation_candidates=validation_candidates,
        reason=reason,
    )


def specialist_estimate_from_state(
    state: SpecialistState,
    current_features: list[float],
) -> dict[str, Any]:
    raw_prediction = 0.0
    if state.active and state.estimator is not None:
        raw_prediction = _predict(
            state.estimator,
            [current_features],
            state.spec,
        )[0] * state.shrinkage

    prediction = _clamp(
        raw_prediction,
        -state.spec.point_limit,
        state.spec.point_limit,
    )
    return {
        "key": (
            f"{state.selected_model_key}_{state.spec.key.rsplit('_', 1)[-1]}"
            if state.selected_model_key
            else state.spec.key
        ),
        "label": state.spec.label,
        "family": state.selected_model_family or state.spec.family,
        "selected_model_key": state.selected_model_key,
        "available": state.available,
        "active": state.active,
        "prediction_pct": round(prediction, 4),
        "blend_weight": round(state.blend_weight, 4),
        "shrinkage": round(state.shrinkage, 2),
        "training_samples": state.training_samples,
        "holdout_samples": state.holdout_samples,
        "validation_skill_pct": state.validation_skill_pct,
        "holdout_skill_pct": state.holdout_skill_pct,
        "holdout_directional_accuracy": state.holdout_directional_accuracy,
        "holdout_signal_coverage_pct": state.holdout_signal_coverage_pct,
        "top_features": state.top_features,
        "validation_candidates": state.validation_candidates,
        "residuals": state.residuals,
        "reason": state.reason,
        "refit_days": state.spec.refit_days,
    }


def build_specialist_estimate(
    values: list[float],
    volumes: list[float | None],
    horizon_days: int,
    direction_threshold: float,
) -> dict[str, Any]:
    if len(values) < FEATURE_LOOKBACK_DAYS:
        raise ValueError("Legalább 61 napi adat szükséges a specialista modellhez.")

    closed_values = tuple(float(value) for value in values[:-1])
    closed_volumes = tuple(
        float(value) if value is not None else None
        for value in volumes[:-1]
    )
    state = _cached_live_state(
        closed_values,
        closed_volumes,
        horizon_days,
        direction_threshold,
    )
    return specialist_estimate_from_state(
        state,
        build_feature_vector(values, volumes),
    )


@lru_cache(maxsize=48)
def _cached_live_state(
    closed_values: tuple[float, ...],
    closed_volumes: tuple[float | None, ...],
    horizon_days: int,
    direction_threshold: float,
) -> SpecialistState:
    values = list(closed_values)
    volumes = list(closed_volumes)
    prepared = prepare_specialist_data(values, volumes, horizon_days)
    closed_origin = len(values) - 1
    return train_specialist(
        prepared,
        known_through_origin=closed_origin - horizon_days,
        direction_threshold=direction_threshold,
    )


def specialist_registry_payload() -> list[dict[str, Any]]:
    return [
        {
            "horizon_days": horizon,
            "key": spec.key,
            "label": spec.label,
            "family": spec.family,
            "candidates": [
                {"key": key, "family": CANDIDATE_FAMILIES[key]}
                for key in spec.candidates
            ],
            "minimum_samples": spec.min_samples,
            "refit_days": spec.refit_days,
        }
        for horizon, spec in SPECIALIST_REGISTRY.items()
    ]
