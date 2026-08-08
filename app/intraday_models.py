from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from math import cos, isfinite, pi, sin, sqrt
from statistics import mean, stdev
from typing import Any

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


INTRADAY_LOOKBACK_HOURS = 721
INTRADAY_HORIZON_HOURS = {1: 24, 7: 168}
INTRADAY_SAMPLE_STRIDE = {1: 3, 7: 6}
SHRINKAGE_CANDIDATES = (0.25, 0.5, 0.75, 1.0)
MODEL_CANDIDATES = ("huber", "ridge", "gradient_boosting")
MAX_LIVE_CACHE_ENTRIES = 20


@dataclass(frozen=True)
class IntradaySpec:
    min_samples: int
    max_training_samples: int
    point_limit: float
    target_clip: float
    minimum_skill_pct: float
    maximum_blend_weight: float
    refit_hours: int


INTRADAY_SPECS = {
    1: IntradaySpec(
        min_samples=700,
        max_training_samples=1800,
        point_limit=2.5,
        target_clip=10.0,
        minimum_skill_pct=2.0,
        maximum_blend_weight=0.65,
        refit_hours=168,
    ),
    7: IntradaySpec(
        min_samples=550,
        max_training_samples=1100,
        point_limit=6.0,
        target_clip=25.0,
        minimum_skill_pct=1.5,
        maximum_blend_weight=0.60,
        refit_hours=336,
    ),
}


MODEL_FAMILIES = {
    "huber": "Órás Huber regresszió",
    "ridge": "Órás Ridge regresszió",
    "gradient_boosting": "Órás Huber Gradient Boosting",
}


FEATURE_NAMES = (
    "return_1h",
    "return_3h",
    "return_6h",
    "return_12h",
    "return_24h",
    "return_72h",
    "return_168h",
    "return_336h",
    "return_720h",
    "volatility_24h",
    "volatility_168h",
    "slope_24h",
    "slope_168h",
    "slope_720h",
    "ema_12_48",
    "ema_48_168",
    "rsi_14h",
    "rsi_24h",
    "zscore_24h",
    "zscore_168h",
    "drawdown_168h",
    "range_24h",
    "range_168h",
    "body_24h",
    "close_location_24h",
    "volume_ratio_24_168",
    "volume_change_24h",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
)


FEATURE_LABELS = {
    "return_1h": "1 órás hozam",
    "return_3h": "3 órás momentum",
    "return_6h": "6 órás momentum",
    "return_12h": "12 órás momentum",
    "return_24h": "24 órás momentum",
    "return_72h": "72 órás momentum",
    "return_168h": "7 napos momentum",
    "return_336h": "14 napos momentum",
    "return_720h": "30 napos momentum",
    "volatility_24h": "24 órás volatilitás",
    "volatility_168h": "7 napos volatilitás",
    "slope_24h": "24 órás trendmeredekség",
    "slope_168h": "7 napos trendmeredekség",
    "slope_720h": "30 napos trendmeredekség",
    "ema_12_48": "EMA 12/48 eltérés",
    "ema_48_168": "EMA 48/168 eltérés",
    "rsi_14h": "órás RSI (14)",
    "rsi_24h": "órás RSI (24)",
    "zscore_24h": "24 órás ár-eltérés",
    "zscore_168h": "7 napos ár-eltérés",
    "drawdown_168h": "7 napos visszaesés",
    "range_24h": "24 órás gyertyatartomány",
    "range_168h": "7 napos gyertyatartomány",
    "body_24h": "24 órás gyertyatest",
    "close_location_24h": "záróár gyertyán belüli helye",
    "volume_ratio_24_168": "24 órás/7 napos forgalom",
    "volume_change_24h": "24 órás forgalomváltozás",
    "hour_sin": "UTC napszak",
    "hour_cos": "UTC napszak",
    "weekday_sin": "hét napja",
    "weekday_cos": "hét napja",
}


@dataclass
class PreparedIntradayData:
    horizon_days: int
    candles: list[dict[str, float]]
    features_by_origin: dict[int, list[float]]
    targets_by_origin: dict[int, float]


@dataclass
class IntradayState:
    spec: IntradaySpec
    selected_model: str
    estimator: Any | None
    active: bool
    available: bool
    shrinkage: float
    blend_weight: float
    training_samples: int
    holdout_samples: int
    validation_skill_pct: float
    holdout_directional_accuracy: float | None
    holdout_signal_coverage_pct: float
    residuals: list[float]
    top_features: list[dict[str, Any]]
    reason: str


_LIVE_STATE_CACHE: OrderedDict[tuple[Any, ...], IntradayState] = OrderedDict()


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def clean_intraday_candles(candles: list[dict[str, Any]]) -> list[dict[str, float]]:
    output: dict[int, dict[str, float]] = {}
    for candle in candles:
        try:
            timestamp = int(candle["timestamp"])
            item = {
                "timestamp": float(timestamp),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle.get("volume", 0.0)),
            }
        except (KeyError, TypeError, ValueError):
            continue
        if not all(isfinite(value) for value in item.values()):
            continue
        if min(item["open"], item["high"], item["low"], item["close"]) <= 0:
            continue
        output[timestamp] = item
    return [output[timestamp] for timestamp in sorted(output)]


def _return_pct(values: list[float], hours: int) -> float:
    return ((values[-1] / values[-hours - 1]) - 1) * 100


def _returns(values: list[float]) -> list[float]:
    return [((current / previous) - 1) * 100 for previous, current in zip(values, values[1:])]


def _ema_last(values: list[float], span: int) -> float:
    alpha = 2 / (span + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result
    return result


def _rsi(values: list[float], window: int) -> float:
    recent = _returns(values[-(window + 1) :])
    gains = [max(value, 0.0) for value in recent]
    losses = [max(-value, 0.0) for value in recent]
    average_gain = mean(gains) if gains else 0.0
    average_loss = mean(losses) if losses else 0.0
    if average_gain == 0 and average_loss == 0:
        return 50.0
    if average_loss == 0:
        return 100.0
    return 100 - 100 / (1 + average_gain / average_loss)


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


def _zscore(values: list[float], window: int) -> float:
    recent = values[-window:]
    deviation = stdev(recent) if len(recent) > 1 else 0.0
    return (values[-1] - mean(recent)) / deviation if deviation else 0.0


def build_intraday_feature_vector(
    candles: list[dict[str, float]],
) -> list[float]:
    if len(candles) < INTRADAY_LOOKBACK_HOURS:
        raise ValueError("Legalább 721 órás gyertya szükséges az intraday modellhez.")

    recent = candles[-INTRADAY_LOOKBACK_HOURS:]
    closes = [item["close"] for item in recent]
    hourly_returns = _returns(closes)
    ranges = [
        (item["high"] - item["low"]) / item["open"] * 100
        for item in recent
    ]
    bodies = [
        (item["close"] - item["open"]) / item["open"] * 100
        for item in recent
    ]
    close_locations = [
        (item["close"] - item["low"]) / (item["high"] - item["low"])
        if item["high"] > item["low"]
        else 0.5
        for item in recent
    ]
    volumes = [item["volume"] for item in recent]
    volume_168 = mean(volumes[-168:])
    previous_volume = mean(volumes[-48:-24])
    timestamp = datetime.fromtimestamp(recent[-1]["timestamp"] / 1000, timezone.utc)
    ema12 = _ema_last(closes, 12)
    ema48 = _ema_last(closes, 48)
    ema168 = _ema_last(closes, 168)

    return [
        _return_pct(closes, 1),
        _return_pct(closes, 3),
        _return_pct(closes, 6),
        _return_pct(closes, 12),
        _return_pct(closes, 24),
        _return_pct(closes, 72),
        _return_pct(closes, 168),
        _return_pct(closes, 336),
        _return_pct(closes, 720),
        stdev(hourly_returns[-24:]) * sqrt(24),
        stdev(hourly_returns[-168:]) * sqrt(168),
        _normalized_slope(closes, 24),
        _normalized_slope(closes, 168),
        _normalized_slope(closes, 720),
        ((ema12 / ema48) - 1) * 100,
        ((ema48 / ema168) - 1) * 100,
        _rsi(closes, 14),
        _rsi(closes, 24),
        _zscore(closes, 24),
        _zscore(closes, 168),
        ((closes[-1] / max(closes[-168:])) - 1) * 100,
        mean(ranges[-24:]),
        mean(ranges[-168:]),
        mean(bodies[-24:]),
        mean(close_locations[-24:]),
        mean(volumes[-24:]) / volume_168 if volume_168 > 0 else 1.0,
        ((mean(volumes[-24:]) / previous_volume) - 1) * 100
        if previous_volume > 0
        else 0.0,
        sin(2 * pi * timestamp.hour / 24),
        cos(2 * pi * timestamp.hour / 24),
        sin(2 * pi * timestamp.weekday() / 7),
        cos(2 * pi * timestamp.weekday() / 7),
    ]


def prepare_intraday_data(
    candles: list[dict[str, Any]],
    horizon_days: int,
) -> PreparedIntradayData:
    if horizon_days not in INTRADAY_HORIZON_HOURS:
        raise ValueError("Az órás modell csak 1 vagy 7 napos időtávhoz használható.")
    cleaned = clean_intraday_candles(candles)
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    stride = INTRADAY_SAMPLE_STRIDE[horizon_days]
    features_by_origin = {}
    targets_by_origin = {}
    for origin in range(INTRADAY_LOOKBACK_HOURS - 1, len(cleaned), stride):
        features_by_origin[origin] = build_intraday_feature_vector(
            cleaned[: origin + 1]
        )
        if origin + horizon_hours < len(cleaned):
            targets_by_origin[origin] = (
                (cleaned[origin + horizon_hours]["close"] / cleaned[origin]["close"])
                - 1
            ) * 100
    return PreparedIntradayData(
        horizon_days=horizon_days,
        candles=cleaned,
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
                        alpha=0.04 if horizon_days == 1 else 0.08,
                        max_iter=500,
                        tol=1e-5,
                    ),
                ),
            ]
        )
    if model_key == "ridge":
        return Pipeline(
            [
                ("scale", StandardScaler()),
                ("regressor", Ridge(alpha=20.0 if horizon_days == 1 else 34.0)),
            ]
        )
    return GradientBoostingRegressor(
        loss="huber",
        n_estimators=90 if horizon_days == 1 else 110,
        learning_rate=0.03,
        max_depth=2,
        min_samples_leaf=18,
        subsample=0.85,
        random_state=42,
    )


def _fit_estimator(estimator, features: list[list[float]], targets: list[float]):
    weights = [0.55 + 0.45 * ((index + 1) / len(features)) for index in range(len(features))]
    if isinstance(estimator, Pipeline):
        estimator.fit(features, targets, regressor__sample_weight=weights)
    else:
        estimator.fit(features, targets, sample_weight=weights)
    return estimator


def _predict(estimator, features: list[list[float]], limit: float) -> list[float]:
    return [_clamp(float(value), -limit, limit) for value in estimator.predict(features)]


def _mae(actual: list[float], predicted: list[float]) -> float:
    return mean(abs(predicted_value - actual_value) for predicted_value, actual_value in zip(predicted, actual))


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
    horizon_days: int,
    samples: int,
    reason: str,
) -> IntradayState:
    return IntradayState(
        spec=INTRADAY_SPECS[horizon_days],
        selected_model="ridge",
        estimator=None,
        active=False,
        available=False,
        shrinkage=0.0,
        blend_weight=0.0,
        training_samples=samples,
        holdout_samples=0,
        validation_skill_pct=0.0,
        holdout_directional_accuracy=None,
        holdout_signal_coverage_pct=0.0,
        residuals=[],
        top_features=[],
        reason=reason,
    )


def _purged_splits(
    origins: list[int],
    horizon_hours: int,
) -> tuple[list[int], list[int], list[int], list[int]]:
    holdout_start = origins[round(len(origins) * 0.80)]
    pre_holdout = [origin for origin in origins if origin + horizon_hours < holdout_start]
    validation_start = pre_holdout[round(len(pre_holdout) * 0.80)]
    core = [origin for origin in pre_holdout if origin + horizon_hours < validation_start]
    validation = [origin for origin in pre_holdout if origin >= validation_start]
    holdout = [origin for origin in origins if origin >= holdout_start]
    return core, validation, pre_holdout, holdout


def train_intraday_specialist(
    prepared: PreparedIntradayData,
    known_through_origin: int,
    direction_threshold: float,
) -> IntradayState:
    horizon_days = prepared.horizon_days
    spec = INTRADAY_SPECS[horizon_days]
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    origins = sorted(
        origin
        for origin in prepared.targets_by_origin
        if origin <= known_through_origin
    )[-spec.max_training_samples :]
    if len(origins) < spec.min_samples:
        return _empty_state(
            horizon_days,
            len(origins),
            f"Még legalább {spec.min_samples} lezárt órás tanítóminta szükséges.",
        )

    core, validation, pre_holdout, holdout = _purged_splits(
        origins,
        horizon_hours,
    )
    if min(len(core), len(validation), len(holdout)) < 40:
        return _empty_state(
            horizon_days,
            len(origins),
            "Nincs elég minta a purged validációs felosztáshoz.",
        )

    features = prepared.features_by_origin
    actual = prepared.targets_by_origin
    clipped = {
        origin: _clamp(actual[origin], -spec.target_clip, spec.target_clip)
        for origin in origins
    }
    validation_actual = [actual[origin] for origin in validation]
    baseline_validation_mae = _mae(
        validation_actual,
        [0.0] * len(validation_actual),
    )
    selected_model = "ridge"
    selected_shrinkage = 0.0
    selected_mae = baseline_validation_mae
    diagnostic_model = "ridge"
    diagnostic_mae = float("inf")

    for model_key in MODEL_CANDIDATES:
        estimator = _fit_estimator(
            _make_estimator(model_key, horizon_days),
            [features[origin] for origin in core],
            [clipped[origin] for origin in core],
        )
        raw_predictions = _predict(
            estimator,
            [features[origin] for origin in validation],
            spec.point_limit,
        )
        raw_mae = _mae(validation_actual, raw_predictions)
        if raw_mae < diagnostic_mae:
            diagnostic_mae = raw_mae
            diagnostic_model = model_key
        for shrinkage in SHRINKAGE_CANDIDATES:
            candidate_mae = _mae(
                validation_actual,
                [prediction * shrinkage for prediction in raw_predictions],
            )
            if candidate_mae < selected_mae:
                selected_mae = candidate_mae
                selected_model = model_key
                selected_shrinkage = shrinkage

    if selected_shrinkage == 0:
        selected_model = diagnostic_model

    holdout_model = _fit_estimator(
        _make_estimator(selected_model, horizon_days),
        [features[origin] for origin in pre_holdout],
        [clipped[origin] for origin in pre_holdout],
    )
    holdout_actual = [actual[origin] for origin in holdout]
    holdout_predictions = [
        prediction * selected_shrinkage
        for prediction in _predict(
            holdout_model,
            [features[origin] for origin in holdout],
            spec.point_limit,
        )
    ]
    baseline_mae = _mae(holdout_actual, [0.0] * len(holdout_actual))
    model_mae = _mae(holdout_actual, holdout_predictions)
    skill_pct = (
        (baseline_mae - model_mae) / baseline_mae * 100
        if baseline_mae > 0
        else 0.0
    )
    active_pairs = [
        (_direction(prediction, direction_threshold), _direction(observed, direction_threshold))
        for prediction, observed in zip(holdout_predictions, holdout_actual)
        if _direction(prediction, direction_threshold) != 0
    ]
    active_accuracy = (
        mean(predicted == observed for predicted, observed in active_pairs) * 100
        if active_pairs
        else None
    )
    coverage = len(active_pairs) / len(holdout_actual) * 100
    minimum_active_samples = max(6, round(len(holdout_actual) * 0.05))
    active = (
        selected_shrinkage > 0
        and skill_pct >= spec.minimum_skill_pct
        and len(active_pairs) >= minimum_active_samples
        and active_accuracy is not None
        and active_accuracy >= 52.0
    )

    if active:
        estimator = _fit_estimator(
            _make_estimator(selected_model, horizon_days),
            [features[origin] for origin in origins],
            [clipped[origin] for origin in origins],
        )
        blend_weight = min(
            spec.maximum_blend_weight,
            0.35 + max(skill_pct, 0.0) / 25,
        )
        reason = "Az órás specialista a purged holdouton igazolt előnyt mutatott."
        top_features = _feature_importance(estimator)
    else:
        estimator = None
        blend_weight = 0.0
        top_features = _feature_importance(holdout_model)
        if selected_shrinkage == 0:
            reason = "Az órás validáció szerint a semleges becslés volt pontosabb."
        elif skill_pct < spec.minimum_skill_pct:
            reason = "Az órás modell holdout-előnye még nem érte el a küszöböt."
        elif len(active_pairs) < minimum_active_samples:
            reason = "Az órás modell még túl kevés aktív jelzést adott."
        else:
            reason = "Az órás modell iránytalálati aránya még nem megfelelő."

    return IntradayState(
        spec=spec,
        selected_model=selected_model,
        estimator=estimator,
        active=active,
        available=True,
        shrinkage=selected_shrinkage,
        blend_weight=blend_weight,
        training_samples=len(origins),
        holdout_samples=len(holdout),
        validation_skill_pct=round(skill_pct, 2),
        holdout_directional_accuracy=(
            round(active_accuracy, 2) if active_accuracy is not None else None
        ),
        holdout_signal_coverage_pct=round(coverage, 2),
        residuals=[
            observed - prediction
            for prediction, observed in zip(holdout_predictions, holdout_actual)
        ],
        top_features=top_features,
        reason=reason,
    )


def intraday_estimate_from_state(
    state: IntradayState,
    current_features: list[float],
    horizon_days: int,
) -> dict[str, Any]:
    prediction = 0.0
    if state.active and state.estimator is not None:
        prediction = _predict(
            state.estimator,
            [current_features],
            state.spec.point_limit,
        )[0] * state.shrinkage
    return {
        "key": f"intraday_{state.selected_model}_{horizon_days}d",
        "label": f"Órás {horizon_days} napos specialista",
        "family": MODEL_FAMILIES[state.selected_model],
        "available": state.available,
        "active": state.active,
        "prediction_pct": round(prediction, 4),
        "blend_weight": round(state.blend_weight, 4),
        "shrinkage": round(state.shrinkage, 2),
        "training_samples": state.training_samples,
        "holdout_samples": state.holdout_samples,
        "validation_skill_pct": state.validation_skill_pct,
        "holdout_directional_accuracy": state.holdout_directional_accuracy,
        "holdout_signal_coverage_pct": state.holdout_signal_coverage_pct,
        "top_features": state.top_features,
        "residuals": state.residuals,
        "reason": state.reason,
        "data_resolution": "1h",
        "purge_hours": INTRADAY_HORIZON_HOURS[horizon_days],
        "refit_hours": state.spec.refit_hours,
    }


def build_intraday_estimate(
    candles: list[dict[str, Any]],
    horizon_days: int,
    direction_threshold: float,
    cache_key: str = "",
) -> dict[str, Any]:
    cleaned = clean_intraday_candles(candles)
    if len(cleaned) < INTRADAY_LOOKBACK_HOURS + 1:
        raise ValueError("Nincs elegendő órás adat az intraday modellhez.")
    closed = cleaned[:-1]
    cache_signature = (
        cache_key,
        horizon_days,
        len(closed),
        int(closed[-1]["timestamp"]),
        round(closed[-1]["close"], 8),
    )
    state = _LIVE_STATE_CACHE.get(cache_signature)
    if state is None:
        prepared = prepare_intraday_data(closed, horizon_days)
        state = train_intraday_specialist(
            prepared,
            known_through_origin=len(closed) - 1 - INTRADAY_HORIZON_HOURS[horizon_days],
            direction_threshold=direction_threshold,
        )
        _LIVE_STATE_CACHE[cache_signature] = state
        _LIVE_STATE_CACHE.move_to_end(cache_signature)
        while len(_LIVE_STATE_CACHE) > MAX_LIVE_CACHE_ENTRIES:
            _LIVE_STATE_CACHE.popitem(last=False)
    return intraday_estimate_from_state(
        state,
        build_intraday_feature_vector(cleaned),
        horizon_days,
    )


def walk_forward_intraday_backtest(
    candles: list[dict[str, Any]],
    horizon_days: int,
    direction_threshold: float,
    max_samples: int = 90,
) -> dict[str, Any]:
    prepared = prepare_intraday_data(candles, horizon_days)
    cleaned = prepared.candles
    horizon_hours = INTRADAY_HORIZON_HOURS[horizon_days]
    last_anchor = len(cleaned) - horizon_hours - 1
    first_possible = INTRADAY_LOOKBACK_HOURS - 1
    anchors = list(range(last_anchor, first_possible - 1, -24))[:max_samples]
    anchors.reverse()
    state = None
    last_refit = None
    results = []

    for anchor in anchors:
        if (
            state is None
            or last_refit is None
            or anchor - last_refit >= INTRADAY_SPECS[horizon_days].refit_hours
        ):
            state = train_intraday_specialist(
                prepared,
                known_through_origin=anchor - horizon_hours,
                direction_threshold=direction_threshold,
            )
            last_refit = anchor
        estimate = intraday_estimate_from_state(
            state,
            build_intraday_feature_vector(cleaned[: anchor + 1]),
            horizon_days,
        )
        expected_change = float(estimate["prediction_pct"])
        actual_change = (
            (cleaned[anchor + horizon_hours]["close"] / cleaned[anchor]["close"])
            - 1
        ) * 100
        predicted_direction = _direction(expected_change, direction_threshold)
        actual_direction = _direction(actual_change, direction_threshold)
        results.append(
            {
                "forecast_at": datetime.fromtimestamp(
                    cleaned[anchor]["timestamp"] / 1000,
                    timezone.utc,
                ).isoformat(),
                "evaluated_at": datetime.fromtimestamp(
                    cleaned[anchor + horizon_hours]["timestamp"] / 1000,
                    timezone.utc,
                ).isoformat(),
                "predicted_change_pct": round(expected_change, 2),
                "actual_change_pct": round(actual_change, 2),
                "predicted_direction": predicted_direction,
                "actual_direction": actual_direction,
                "hit": predicted_direction == actual_direction,
                "absolute_error_pct": abs(expected_change - actual_change),
                "baseline_error_pct": abs(actual_change),
                "specialist_active": estimate["active"],
            }
        )

    model_mae = mean(item["absolute_error_pct"] for item in results)
    baseline_mae = mean(item["baseline_error_pct"] for item in results)
    active_results = [item for item in results if item["predicted_direction"] != 0]
    active_accuracy = (
        mean(item["hit"] for item in active_results) * 100
        if active_results
        else None
    )
    return {
        "model": {
            "name": "Órás horizont-specialista",
            "resolution": "1h",
            "purge_hours": horizon_hours,
        },
        "horizon_days": horizon_days,
        "period": {
            "from": results[0]["forecast_at"],
            "to": results[-1]["evaluated_at"],
        },
        "summary": {
            "samples": len(results),
            "active_directional_accuracy": (
                round(active_accuracy, 2) if active_accuracy is not None else None
            ),
            "signal_coverage_pct": round(
                len(active_results) / len(results) * 100,
                2,
            ),
            "mae_pct": round(model_mae, 2),
            "baseline_mae_pct": round(baseline_mae, 2),
            "skill_vs_baseline_pct": round(
                (baseline_mae - model_mae) / baseline_mae * 100
                if baseline_mae
                else 0.0,
                2,
            ),
            "specialist_usage_pct": round(
                mean(item["specialist_active"] for item in results) * 100,
                2,
            ),
        },
        "recent_results": list(reversed(results[-8:])),
        "methodology": (
            "Az órás visszamérés csak a forecast időpontjáig lezárt gyertyákat "
            "használja; a címkék átfedését a validációs határok körül a teljes "
            "előrejelzési horizonttal tisztítja."
        ),
    }
