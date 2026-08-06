from dataclasses import dataclass
from functools import lru_cache
from math import isfinite, log, sqrt
from random import Random
from statistics import mean, stdev
from typing import Any

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


FEATURE_LOOKBACK_DAYS = 201
CALIBRATION_METHOD = "Platt-kalibráció"
BUY_THRESHOLDS = (0.55, 0.60, 0.65, 0.70, 0.75)
CALIBRATION_SHRINKAGE = (0.0, 0.25, 0.50, 0.75, 1.0)


@dataclass(frozen=True)
class ProbabilitySpec:
    key: str
    label: str
    target_return_pct: float
    min_samples: int
    max_training_samples: int
    refit_days: int
    minimum_brier_skill_pct: float
    maximum_calibration_error: float


PROBABILITY_REGISTRY = {
    1: ProbabilitySpec(
        key="calibrated_probability_1d",
        label="1 napos küszöbhozam-valószínűség",
        target_return_pct=0.20,
        min_samples=320,
        max_training_samples=1600,
        refit_days=7,
        minimum_brier_skill_pct=2.0,
        maximum_calibration_error=0.12,
    ),
    7: ProbabilitySpec(
        key="calibrated_probability_7d",
        label="7 napos küszöbhozam-valószínűség",
        target_return_pct=1.00,
        min_samples=360,
        max_training_samples=1600,
        refit_days=14,
        minimum_brier_skill_pct=2.0,
        maximum_calibration_error=0.12,
    ),
    30: ProbabilitySpec(
        key="calibrated_probability_30d",
        label="30 napos küszöbhozam-valószínűség",
        target_return_pct=3.00,
        min_samples=420,
        max_training_samples=1600,
        refit_days=30,
        minimum_brier_skill_pct=1.5,
        maximum_calibration_error=0.13,
    ),
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
    "downside_volatility_30d",
    "ema_20_50",
    "price_sma200",
    "rsi_14",
    "price_zscore_20",
    "drawdown_30d",
    "volume_ratio_7_30",
    "volume_zscore_20",
    "volume_change_7d",
    "volume_available",
    "market_return_7d",
    "market_return_30d",
    "market_ema_20_50",
    "market_price_sma200",
    "relative_strength_7d",
    "relative_strength_30d",
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
    "downside_volatility_30d": "negatív hozamok szórása",
    "ema_20_50": "EMA 20/50 trend",
    "price_sma200": "ár/SMA200 trend",
    "rsi_14": "RSI (14)",
    "price_zscore_20": "20 napos ár-eltérés",
    "drawdown_30d": "30 napos visszaesés",
    "volume_ratio_7_30": "rövid/hosszú forgalom",
    "volume_zscore_20": "forgalom z-score",
    "volume_change_7d": "7 napos forgalomváltozás",
    "volume_available": "forgalmi adatok teljessége",
    "market_return_7d": "BTC 7 napos trend",
    "market_return_30d": "BTC 30 napos trend",
    "market_ema_20_50": "BTC EMA 20/50 trend",
    "market_price_sma200": "BTC ár/SMA200 trend",
    "relative_strength_7d": "7 napos relatív erő",
    "relative_strength_30d": "30 napos relatív erő",
}


@dataclass
class PreparedProbabilityData:
    horizon_days: int
    features_by_origin: dict[int, list[float]]
    targets_by_origin: dict[int, int]
    returns_by_origin: dict[int, float]


@dataclass
class ProbabilityState:
    spec: ProbabilitySpec
    estimator: Any | None
    calibrator: Any | None
    candidate_key: str | None
    family: str
    available: bool
    active: bool
    baseline_probability: float
    shrinkage: float
    buy_threshold: float
    sell_threshold: float
    training_samples: int
    holdout_samples: int
    validation_brier_skill_pct: float
    holdout_brier_score: float | None
    baseline_brier_score: float | None
    holdout_brier_skill_pct: float
    holdout_log_loss: float | None
    baseline_log_loss: float | None
    roc_auc: float | None
    calibration_error: float | None
    buy_precision_pct: float | None
    buy_recall_pct: float | None
    buy_signal_coverage_pct: float
    stability_mean_skill_pct: float
    stability_positive_blocks: int
    stability_total_blocks: int
    historical_mean_skill_pct: float
    historical_positive_checks: int
    historical_total_checks: int
    reliability_bins: list[dict[str, Any]]
    top_features: list[dict[str, Any]]
    validation_candidates: dict[str, dict[str, float]]
    reason: str


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _finite(value: float, default: float = 0.0) -> float:
    return float(value) if isfinite(float(value)) else default


def _return_pct(values: list[float], days: int) -> float:
    lookback = min(days, len(values) - 1)
    if lookback <= 0 or values[-lookback - 1] <= 0:
        return 0.0
    return ((values[-1] / values[-lookback - 1]) - 1) * 100


def _returns(values: list[float]) -> list[float]:
    return [
        ((current / previous) - 1) * 100
        for previous, current in zip(values, values[1:])
        if previous > 0
    ]


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


def _zscore(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    deviation = stdev(values)
    return (values[-1] - mean(values)) / deviation if deviation > 0 else 0.0


def _volume_features(volumes: list[float | None]) -> tuple[float, float, float, float]:
    recent_values = volumes[-30:]
    valid = [
        float(value)
        for value in recent_values
        if value is not None and isfinite(float(value)) and float(value) > 0
    ]
    availability = len(valid) / len(recent_values) if recent_values else 0.0
    if len(valid) < 14:
        return 1.0, 0.0, 0.0, availability

    recent_7 = valid[-7:]
    long_average = mean(valid)
    ratio = mean(recent_7) / long_average if long_average > 0 else 1.0
    zscore = _zscore(valid[-20:])
    reference = valid[-8] if len(valid) >= 8 else valid[0]
    change = ((valid[-1] / reference) - 1) * 100 if reference > 0 else 0.0
    return (
        _clamp(ratio, 0.2, 5.0),
        _clamp(zscore, -6.0, 6.0),
        _clamp(change, -95.0, 400.0),
        availability,
    )


def _aligned_series(values: list[float], fallback: list[float]) -> list[float]:
    aligned = [float(value) for value in values[: len(fallback)] if float(value) > 0]
    if len(aligned) != len(fallback):
        return list(fallback)
    return aligned


def build_probability_feature_vector(
    values: list[float],
    volumes: list[float | None],
    market_values: list[float],
) -> list[float]:
    if len(values) < FEATURE_LOOKBACK_DAYS:
        raise ValueError("Legalább 201 napi adat szükséges a valószínűségi modellhez.")

    market_values = _aligned_series(market_values, values)
    daily_returns = _returns(values)
    recent_7 = daily_returns[-7:]
    recent_30 = daily_returns[-30:]
    downside = [value for value in recent_30 if value < 0]
    ema20 = _ema_last(values, 20)
    ema50 = _ema_last(values, 50)
    sma200 = mean(values[-200:])
    rolling_high = max(values[-30:])
    market_ema20 = _ema_last(market_values, 20)
    market_ema50 = _ema_last(market_values, 50)
    market_sma200 = mean(market_values[-200:])
    volume_ratio, volume_zscore, volume_change, volume_available = _volume_features(
        volumes
    )
    return_7 = _return_pct(values, 7)
    return_30 = _return_pct(values, 30)
    market_return_7 = _return_pct(market_values, 7)
    market_return_30 = _return_pct(market_values, 30)

    features = [
        _return_pct(values, 1),
        _return_pct(values, 3),
        return_7,
        _return_pct(values, 14),
        return_30,
        _return_pct(values, 60),
        stdev(recent_7) if len(recent_7) > 1 else 0.0,
        stdev(recent_30) if len(recent_30) > 1 else 0.0,
        stdev(downside) if len(downside) > 1 else 0.0,
        ((ema20 / ema50) - 1) * 100 if ema50 > 0 else 0.0,
        ((values[-1] / sma200) - 1) * 100 if sma200 > 0 else 0.0,
        _rsi(values),
        _zscore(values[-20:]),
        ((values[-1] / rolling_high) - 1) * 100 if rolling_high > 0 else 0.0,
        volume_ratio,
        volume_zscore,
        volume_change,
        volume_available,
        market_return_7,
        market_return_30,
        ((market_ema20 / market_ema50) - 1) * 100 if market_ema50 > 0 else 0.0,
        ((market_values[-1] / market_sma200) - 1) * 100 if market_sma200 > 0 else 0.0,
        return_7 - market_return_7,
        return_30 - market_return_30,
    ]
    return [_finite(value) for value in features]


def prepare_probability_data(
    values: list[float],
    volumes: list[float | None],
    market_values: list[float],
    horizon_days: int,
) -> PreparedProbabilityData:
    if horizon_days not in PROBABILITY_REGISTRY:
        raise ValueError("Az időtáv 1, 7 vagy 30 nap lehet.")

    aligned_volumes = list(volumes[: len(values)])
    if len(aligned_volumes) < len(values):
        aligned_volumes.extend([None] * (len(values) - len(aligned_volumes)))
    aligned_market = _aligned_series(market_values, values)
    threshold = PROBABILITY_REGISTRY[horizon_days].target_return_pct
    features_by_origin: dict[int, list[float]] = {}
    targets_by_origin: dict[int, int] = {}
    returns_by_origin: dict[int, float] = {}

    for origin in range(FEATURE_LOOKBACK_DAYS - 1, len(values)):
        features_by_origin[origin] = build_probability_feature_vector(
            values[: origin + 1],
            aligned_volumes[: origin + 1],
            aligned_market[: origin + 1],
        )
        if origin + horizon_days < len(values):
            future_return = (
                (values[origin + horizon_days] / values[origin]) - 1
            ) * 100
            returns_by_origin[origin] = future_return
            targets_by_origin[origin] = int(future_return >= threshold)

    return PreparedProbabilityData(
        horizon_days=horizon_days,
        features_by_origin=features_by_origin,
        targets_by_origin=targets_by_origin,
        returns_by_origin=returns_by_origin,
    )


def _recency_weights(count: int) -> list[float]:
    return [0.55 + 0.45 * ((index + 1) / count) for index in range(count)]


def _make_estimator(candidate_key: str):
    if candidate_key == "logistic":
        return Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "classifier",
                    LogisticRegression(
                        C=0.35,
                        max_iter=700,
                        solver="lbfgs",
                        random_state=42,
                    ),
                ),
            ]
        )
    return HistGradientBoostingClassifier(
        learning_rate=0.045,
        max_iter=110,
        max_leaf_nodes=15,
        min_samples_leaf=18,
        l2_regularization=2.0,
        early_stopping=False,
        random_state=42,
    )


def _candidate_family(candidate_key: str) -> str:
    if candidate_key == "logistic":
        return "Kalibrált Logistic Regression"
    return "Kalibrált HistGradientBoosting"


def _fit_estimator(estimator, features: list[list[float]], targets: list[int]):
    weights = _recency_weights(len(features))
    if isinstance(estimator, Pipeline):
        estimator.fit(features, targets, classifier__sample_weight=weights)
    else:
        estimator.fit(features, targets, sample_weight=weights)
    return estimator


def _raw_probabilities(estimator, features: list[list[float]]) -> list[float]:
    return [
        _clamp(float(value), 0.0001, 0.9999)
        for value in estimator.predict_proba(features)[:, 1]
    ]


def _logit(probability: float) -> float:
    value = _clamp(probability, 0.0001, 0.9999)
    return log(value / (1 - value))


def _fit_calibrator(probabilities: list[float], targets: list[int]):
    if len(set(targets)) < 2:
        return None
    calibrator = LogisticRegression(
        C=1.0,
        max_iter=500,
        solver="lbfgs",
        random_state=42,
    )
    calibrator.fit(
        [[_logit(value)] for value in probabilities],
        targets,
        sample_weight=_recency_weights(len(targets)),
    )
    return calibrator


def _apply_calibrator(calibrator, probabilities: list[float]) -> list[float]:
    if calibrator is None:
        return probabilities
    return [
        _clamp(float(value), 0.0001, 0.9999)
        for value in calibrator.predict_proba(
            [[_logit(probability)] for probability in probabilities]
        )[:, 1]
    ]


def calibrated_probabilities(estimator, calibrator, features: list[list[float]]) -> list[float]:
    return _apply_calibrator(calibrator, _raw_probabilities(estimator, features))


def _shrink_probabilities(
    probabilities: list[float],
    baseline: float,
    shrinkage: float,
) -> list[float]:
    return [
        _clamp(baseline + shrinkage * (probability - baseline), 0.0001, 0.9999)
        for probability in probabilities
    ]


def brier_score(actual: list[int], probabilities: list[float]) -> float:
    if not actual:
        return 0.0
    return mean(
        (probability - target) ** 2
        for target, probability in zip(actual, probabilities)
    )


def binary_log_loss(actual: list[int], probabilities: list[float]) -> float:
    if not actual:
        return 0.0
    return -mean(
        target * log(_clamp(probability, 0.0001, 0.9999))
        + (1 - target) * log(_clamp(1 - probability, 0.0001, 0.9999))
        for target, probability in zip(actual, probabilities)
    )


def reliability_bins(
    actual: list[int],
    probabilities: list[float],
    bin_count: int = 5,
) -> list[dict[str, Any]]:
    output = []
    for index in range(bin_count):
        lower = index / bin_count
        upper = (index + 1) / bin_count
        pairs = [
            (target, probability)
            for target, probability in zip(actual, probabilities)
            if lower <= probability < upper or (index == bin_count - 1 and probability == 1)
        ]
        if not pairs:
            continue
        output.append(
            {
                "from_pct": round(lower * 100),
                "to_pct": round(upper * 100),
                "samples": len(pairs),
                "mean_probability_pct": round(mean(item[1] for item in pairs) * 100, 2),
                "observed_frequency_pct": round(mean(item[0] for item in pairs) * 100, 2),
            }
        )
    return output


def calibration_error(actual: list[int], probabilities: list[float]) -> float:
    bins = reliability_bins(actual, probabilities)
    total = sum(item["samples"] for item in bins)
    if total == 0:
        return 0.0
    return sum(
        abs(item["mean_probability_pct"] - item["observed_frequency_pct"])
        / 100
        * item["samples"]
        / total
        for item in bins
    )


def safe_roc_auc(actual: list[int], probabilities: list[float]) -> float | None:
    if len(set(actual)) < 2:
        return None
    return float(roc_auc_score(actual, probabilities))


def _purged_blocks(
    origins: list[int],
    horizon_days: int,
) -> tuple[list[int], list[int], list[int], list[int]]:
    count = len(origins)
    first_cut = max(1, round(count * 0.55))
    second_cut = max(first_cut + 1, round(count * 0.70))
    third_cut = max(second_cut + 1, round(count * 0.82))
    raw_blocks = [
        origins[:first_cut],
        origins[first_cut:second_cut],
        origins[second_cut:third_cut],
        origins[third_cut:],
    ]
    blocks = []
    for index, block in enumerate(raw_blocks):
        if index < len(raw_blocks) - 1 and raw_blocks[index + 1]:
            next_start = raw_blocks[index + 1][0]
            block = [origin for origin in block if origin + horizon_days < next_start]
        blocks.append(block)
    return blocks[0], blocks[1], blocks[2], blocks[3]


def _event_rate(targets: list[int], default: float = 0.5) -> float:
    return mean(targets) if targets else default


def _brier_skill(actual: list[int], probabilities: list[float], baseline: float) -> tuple[float, float, float]:
    score = brier_score(actual, probabilities)
    baseline_score = brier_score(actual, [baseline] * len(actual))
    skill = (
        (baseline_score - score) / baseline_score * 100
        if baseline_score > 0
        else 0.0
    )
    return score, baseline_score, skill


def _choose_buy_threshold(
    probabilities: list[float],
    actual: list[int],
    baseline: float,
) -> float:
    minimum_signals = max(5, round(len(actual) * 0.08))
    choices = []
    positives = sum(actual)
    for threshold in BUY_THRESHOLDS:
        selected = [
            target
            for target, probability in zip(actual, probabilities)
            if probability >= threshold
        ]
        if len(selected) < minimum_signals:
            continue
        precision = mean(selected)
        coverage = len(selected) / len(actual)
        recall = sum(selected) / positives if positives else 0.0
        if precision >= max(0.52, baseline + 0.05):
            score = (precision - baseline) * sqrt(coverage) + recall * 0.03
            choices.append((score, threshold))
    if choices:
        return max(choices)[1]
    return _clamp(max(0.60, baseline + 0.10), 0.55, 0.75)


def _signal_metrics(
    actual: list[int],
    probabilities: list[float],
    threshold: float,
) -> tuple[float | None, float | None, float]:
    selected = [
        target
        for target, probability in zip(actual, probabilities)
        if probability >= threshold
    ]
    precision = mean(selected) * 100 if selected else None
    positives = sum(actual)
    recall = sum(selected) / positives * 100 if positives else None
    coverage = len(selected) / len(actual) * 100 if actual else 0.0
    return precision, recall, coverage


def _temporal_block_skills(
    actual: list[int],
    probabilities: list[float],
    baseline: float,
    block_count: int = 3,
) -> list[float]:
    skills = []
    for index in range(block_count):
        start = round(len(actual) * index / block_count)
        end = round(len(actual) * (index + 1) / block_count)
        if end <= start:
            continue
        block_actual = actual[start:end]
        block_probabilities = probabilities[start:end]
        _score, _baseline_score, skill = _brier_skill(
            block_actual,
            block_probabilities,
            baseline,
        )
        skills.append(skill)
    return skills


def _permutation_importance(
    estimator,
    calibrator,
    features: list[list[float]],
    targets: list[int],
    baseline: float,
    shrinkage: float,
) -> list[dict[str, Any]]:
    if not features:
        return []
    reference = brier_score(
        targets,
        _shrink_probabilities(
            calibrated_probabilities(estimator, calibrator, features),
            baseline,
            shrinkage,
        ),
    )
    importances = []
    for feature_index, name in enumerate(FEATURE_NAMES):
        shuffled = [list(row) for row in features]
        column = [row[feature_index] for row in shuffled]
        Random(42 + feature_index).shuffle(column)
        for row, value in zip(shuffled, column):
            row[feature_index] = value
        shuffled_score = brier_score(
            targets,
            _shrink_probabilities(
                calibrated_probabilities(estimator, calibrator, shuffled),
                baseline,
                shrinkage,
            ),
        )
        importances.append((name, max(0.0, shuffled_score - reference)))

    positive_total = sum(value for _name, value in importances)
    if positive_total <= 0:
        return []
    ranked = sorted(importances, key=lambda item: item[1], reverse=True)
    return [
        {
            "key": name,
            "label": FEATURE_LABELS[name],
            "importance_pct": round(value / positive_total * 100, 1),
        }
        for name, value in ranked[:5]
        if value > 0
    ]


def _empty_state(
    spec: ProbabilitySpec,
    targets: list[int],
    reason: str,
) -> ProbabilityState:
    baseline = _event_rate(targets)
    return ProbabilityState(
        spec=spec,
        estimator=None,
        calibrator=None,
        candidate_key=None,
        family="Logistic Regression / HistGradientBoosting",
        available=False,
        active=False,
        baseline_probability=baseline,
        shrinkage=0.0,
        buy_threshold=_clamp(max(0.60, baseline + 0.10), 0.55, 0.75),
        sell_threshold=_clamp(min(0.45, baseline - 0.10), 0.25, 0.45),
        training_samples=len(targets),
        holdout_samples=0,
        validation_brier_skill_pct=0.0,
        holdout_brier_score=None,
        baseline_brier_score=None,
        holdout_brier_skill_pct=0.0,
        holdout_log_loss=None,
        baseline_log_loss=None,
        roc_auc=None,
        calibration_error=None,
        buy_precision_pct=None,
        buy_recall_pct=None,
        buy_signal_coverage_pct=0.0,
        stability_mean_skill_pct=0.0,
        stability_positive_blocks=0,
        stability_total_blocks=0,
        historical_mean_skill_pct=0.0,
        historical_positive_checks=0,
        historical_total_checks=0,
        reliability_bins=[],
        top_features=[],
        validation_candidates={},
        reason=reason,
    )


def train_probability_model(
    prepared: PreparedProbabilityData,
    known_through_origin: int,
    verify_history: bool = True,
) -> ProbabilityState:
    spec = PROBABILITY_REGISTRY[prepared.horizon_days]
    origins = sorted(
        origin
        for origin in prepared.targets_by_origin
        if origin <= known_through_origin
    )[-spec.max_training_samples :]
    targets = [prepared.targets_by_origin[origin] for origin in origins]
    if len(origins) < spec.min_samples:
        return _empty_state(
            spec,
            targets,
            f"Még legalább {spec.min_samples} lezárt tanítóminta szükséges.",
        )
    if len(set(targets)) < 2:
        return _empty_state(
            spec,
            targets,
            "A célváltozó csak egy osztályt tartalmaz; valószínűségi modell nem tanítható.",
        )

    training, calibration, validation, holdout = _purged_blocks(
        origins,
        prepared.horizon_days,
    )
    if min(map(len, (training, calibration, validation, holdout))) < 24:
        return _empty_state(
            spec,
            targets,
            "Nincs elég minta a megtisztított négy időrendi adatszakaszhoz.",
        )

    def features(selected: list[int]) -> list[list[float]]:
        return [prepared.features_by_origin[origin] for origin in selected]

    def labels(selected: list[int]) -> list[int]:
        return [prepared.targets_by_origin[origin] for origin in selected]

    if any(len(set(labels(block))) < 2 for block in (training, calibration, validation, holdout)):
        return _empty_state(
            spec,
            targets,
            "Az egyik időrendi adatszakaszban nincs mindkét kimeneti osztály.",
        )

    validation_candidates: dict[str, dict[str, float]] = {}
    candidate_models: dict[str, tuple[Any, Any, float, list[float]]] = {}
    selected_key = None
    selected_validation_score = float("inf")
    validation_baseline = _event_rate(labels(training + calibration))
    validation_actual = labels(validation)

    for candidate_key in ("logistic", "hist_gradient_boosting"):
        estimator = _fit_estimator(
            _make_estimator(candidate_key),
            features(training),
            labels(training),
        )
        calibrator = _fit_calibrator(
            _raw_probabilities(estimator, features(calibration)),
            labels(calibration),
        )
        calibrated = calibrated_probabilities(
            estimator,
            calibrator,
            features(validation),
        )
        shrinkage = min(
            CALIBRATION_SHRINKAGE,
            key=lambda candidate: brier_score(
                validation_actual,
                _shrink_probabilities(
                    calibrated,
                    validation_baseline,
                    candidate,
                ),
            ),
        )
        probabilities = _shrink_probabilities(
            calibrated,
            validation_baseline,
            shrinkage,
        )
        score, baseline_score, skill = _brier_skill(
            validation_actual,
            probabilities,
            validation_baseline,
        )
        validation_candidates[candidate_key] = {
            "brier_score": round(score, 5),
            "baseline_brier_score": round(baseline_score, 5),
            "brier_skill_pct": round(skill, 2),
            "shrinkage": shrinkage,
        }
        candidate_models[candidate_key] = (
            estimator,
            calibrator,
            shrinkage,
            probabilities,
        )
        if score < selected_validation_score:
            selected_validation_score = score
            selected_key = candidate_key

    if selected_key is None:
        return _empty_state(spec, targets, "Egyik modelljelölt sem volt kiértékelhető.")

    evaluation_estimator, evaluation_calibrator, selected_shrinkage, validation_probabilities = (
        candidate_models[selected_key]
    )
    holdout_actual = labels(holdout)
    holdout_features = features(holdout)
    baseline_probability = _event_rate(labels(training + calibration + validation))
    holdout_probabilities = _shrink_probabilities(
        calibrated_probabilities(
            evaluation_estimator,
            evaluation_calibrator,
            holdout_features,
        ),
        baseline_probability,
        selected_shrinkage,
    )
    holdout_score, baseline_score, holdout_skill = _brier_skill(
        holdout_actual,
        holdout_probabilities,
        baseline_probability,
    )
    holdout_log_loss = binary_log_loss(holdout_actual, holdout_probabilities)
    baseline_log_loss = binary_log_loss(
        holdout_actual,
        [baseline_probability] * len(holdout_actual),
    )
    error = calibration_error(holdout_actual, holdout_probabilities)
    auc = safe_roc_auc(holdout_actual, holdout_probabilities)
    buy_threshold = _choose_buy_threshold(
        validation_probabilities,
        validation_actual,
        validation_baseline,
    )
    precision, recall, signal_coverage = _signal_metrics(
        holdout_actual,
        holdout_probabilities,
        buy_threshold,
    )
    block_skills = _temporal_block_skills(
        holdout_actual,
        holdout_probabilities,
        baseline_probability,
    )
    positive_blocks = sum(skill > 0 for skill in block_skills)
    stability_mean = mean(block_skills) if block_skills else 0.0
    validation_skill = validation_candidates[selected_key]["brier_skill_pct"]
    class_counts_are_safe = min(sum(holdout_actual), len(holdout_actual) - sum(holdout_actual)) >= 8
    active = (
        selected_shrinkage > 0
        and validation_skill > 0
        and holdout_skill >= spec.minimum_brier_skill_pct
        and holdout_log_loss <= baseline_log_loss
        and error <= spec.maximum_calibration_error
        and auc is not None
        and auc >= 0.52
        and positive_blocks >= 2
        and stability_mean > 0
        and class_counts_are_safe
    )

    if active:
        reason = (
            "A kalibrált modell az érintetlen holdouton és legalább két időblokkban "
            "felülteljesítette a historikus alapesélyt."
        )
    elif selected_shrinkage == 0:
        reason = "A validáció szerint a historikus alapesély volt a legpontosabb."
    elif holdout_skill < spec.minimum_brier_skill_pct:
        reason = "A Brier-előny nem érte el a bekapcsolási küszöböt az érintetlen holdouton."
    elif holdout_log_loss > baseline_log_loss:
        reason = "A modell log loss értéke rosszabb volt a historikus alapesélynél."
    elif error > spec.maximum_calibration_error:
        reason = "A becsült százalékok kalibrációs hibája túl nagy volt."
    elif positive_blocks < 2 or stability_mean <= 0:
        reason = "A valószínűségi modell nem volt stabil több időrendi blokkban."
    elif auc is None or auc < 0.52:
        reason = "A modell rangsorolási képessége még nem volt megfelelő."
    else:
        reason = "A holdout egyik eseményosztályából még túl kevés minta áll rendelkezésre."

    historical_states = []
    if active and verify_history:
        for fraction in (0.78, 0.88):
            earlier_index = min(len(origins) - 1, max(0, round(len(origins) * fraction) - 1))
            earlier_origin = origins[earlier_index]
            historical_states.append(
                train_probability_model(
                    prepared,
                    known_through_origin=earlier_origin,
                    verify_history=False,
                )
            )
    historical_positive_checks = sum(state.active for state in historical_states)
    historical_mean_skill = (
        mean(state.holdout_brier_skill_pct for state in historical_states)
        if historical_states
        else 0.0
    )
    if active and historical_states and (
        historical_positive_checks < 1 or historical_mean_skill <= 0
    ):
        active = False
        reason = (
            "Az aktuális holdout megfelelt, de a modell a korábbi teljes "
            "kapuvizsgálatokon még nem volt stabil."
        )

    final_calibration_count = max(50, round(len(origins) * 0.16))
    final_calibration_origins = origins[-final_calibration_count:]
    final_training_origins = [
        origin
        for origin in origins[:-final_calibration_count]
        if origin + prepared.horizon_days < final_calibration_origins[0]
    ]
    final_estimator = evaluation_estimator
    final_calibrator = evaluation_calibrator
    if (
        len(final_training_origins) >= 80
        and len(set(labels(final_training_origins))) == 2
        and len(set(labels(final_calibration_origins))) == 2
    ):
        final_estimator = _fit_estimator(
            _make_estimator(selected_key),
            features(final_training_origins),
            labels(final_training_origins),
        )
        final_calibrator = _fit_calibrator(
            _raw_probabilities(final_estimator, features(final_calibration_origins)),
            labels(final_calibration_origins),
        )
    live_baseline_probability = _event_rate(targets)

    return ProbabilityState(
        spec=spec,
        estimator=final_estimator,
        calibrator=final_calibrator,
        candidate_key=selected_key,
        family=_candidate_family(selected_key),
        available=True,
        active=active,
        baseline_probability=live_baseline_probability,
        shrinkage=selected_shrinkage,
        buy_threshold=buy_threshold,
        sell_threshold=_clamp(
            min(0.45, live_baseline_probability - 0.10),
            0.25,
            0.45,
        ),
        training_samples=len(origins),
        holdout_samples=len(holdout),
        validation_brier_skill_pct=round(validation_skill, 2),
        holdout_brier_score=round(holdout_score, 5),
        baseline_brier_score=round(baseline_score, 5),
        holdout_brier_skill_pct=round(holdout_skill, 2),
        holdout_log_loss=round(holdout_log_loss, 5),
        baseline_log_loss=round(baseline_log_loss, 5),
        roc_auc=round(auc, 4) if auc is not None else None,
        calibration_error=round(error, 4),
        buy_precision_pct=round(precision, 2) if precision is not None else None,
        buy_recall_pct=round(recall, 2) if recall is not None else None,
        buy_signal_coverage_pct=round(signal_coverage, 2),
        stability_mean_skill_pct=round(stability_mean, 2),
        stability_positive_blocks=positive_blocks,
        stability_total_blocks=len(block_skills),
        historical_mean_skill_pct=round(historical_mean_skill, 2),
        historical_positive_checks=historical_positive_checks,
        historical_total_checks=len(historical_states),
        reliability_bins=reliability_bins(holdout_actual, holdout_probabilities),
        top_features=_permutation_importance(
            evaluation_estimator,
            evaluation_calibrator,
            holdout_features,
            holdout_actual,
            baseline_probability,
            selected_shrinkage,
        ),
        validation_candidates=validation_candidates,
        reason=reason,
    )


def probability_from_state(
    state: ProbabilityState,
    current_features: list[float] | None,
) -> dict[str, Any]:
    candidate_probability = None
    if state.estimator is not None and current_features is not None:
        candidate_probability = _shrink_probabilities(
            calibrated_probabilities(
                state.estimator,
                state.calibrator,
                [current_features],
            ),
            state.baseline_probability,
            state.shrinkage,
        )[0]
    published_probability = (
        candidate_probability
        if state.active and candidate_probability is not None
        else state.baseline_probability
    )
    return {
        "probability": _clamp(published_probability, 0.0, 1.0),
        "candidate_probability": candidate_probability,
        "baseline_probability": _clamp(state.baseline_probability, 0.0, 1.0),
        "active": state.active,
        "available": state.available,
    }


@lru_cache(maxsize=48)
def _cached_live_state(
    closed_values: tuple[float, ...],
    closed_volumes: tuple[float | None, ...],
    closed_market_values: tuple[float, ...],
    horizon_days: int,
) -> ProbabilityState:
    values = list(closed_values)
    prepared = prepare_probability_data(
        values,
        list(closed_volumes),
        list(closed_market_values),
        horizon_days,
    )
    return train_probability_model(
        prepared,
        known_through_origin=len(values) - horizon_days - 1,
    )


def _historical_event_rate(
    values: list[float],
    horizon_days: int,
    threshold: float,
) -> float:
    first = max(0, len(values) - 400 - horizon_days)
    events = [
        int(((values[index + horizon_days] / values[index]) - 1) * 100 >= threshold)
        for index in range(first, len(values) - horizon_days)
        if values[index] > 0
    ]
    return _event_rate(events)


def build_probability_forecast(
    values: list[float],
    volumes: list[float | None],
    market_values: list[float],
    horizon_days: int,
    snapshot: dict[str, Any],
    lower_change_pct: float,
    upper_change_pct: float,
    market_context_available: bool,
) -> dict[str, Any]:
    spec = PROBABILITY_REGISTRY[horizon_days]
    aligned_market = _aligned_series(market_values, values)
    aligned_volumes = list(volumes[: len(values)])
    if len(aligned_volumes) < len(values):
        aligned_volumes.extend([None] * (len(values) - len(aligned_volumes)))

    closed_values = tuple(float(value) for value in values[:-1])
    closed_volumes = tuple(
        float(value) if value is not None else None
        for value in aligned_volumes[:-1]
    )
    closed_market = tuple(float(value) for value in aligned_market[:-1])
    state = _cached_live_state(
        closed_values,
        closed_volumes,
        closed_market,
        horizon_days,
    )
    if not state.training_samples:
        state.baseline_probability = _historical_event_rate(
            list(closed_values),
            horizon_days,
            spec.target_return_pct,
        )

    current_features = None
    if len(values) >= FEATURE_LOOKBACK_DAYS:
        current_features = build_probability_feature_vector(
            values,
            aligned_volumes,
            aligned_market,
        )
    probability = probability_from_state(state, current_features)

    sma200 = float(snapshot.get("sma200") or mean(values[-min(200, len(values)) :]))
    price_above_sma200 = values[-1] > sma200
    ema_alignment = float(snapshot["ema20"]) > float(snapshot["ema50"])
    rsi_in_band = 45 <= float(snapshot["rsi"]) <= 70
    market_ema20 = _ema_last(aligned_market, 20)
    market_ema50 = _ema_last(aligned_market, 50)
    market_sma200 = mean(aligned_market[-min(200, len(aligned_market)) :])
    market_trend = (
        aligned_market[-1] > market_sma200 and market_ema20 > market_ema50
        if market_context_available
        else True
    )
    trend_passed = price_above_sma200 and ema_alignment and rsi_in_band and market_trend

    interval_width = max(0.0, upper_change_pct - lower_change_pct)
    maximum_interval_width = {1: 8.0, 7: 22.0, 30: 45.0}[horizon_days]
    volatility_passed = float(snapshot["volatility"]) <= 120
    uncertainty_passed = interval_width <= maximum_interval_width
    risk_passed = volatility_passed and uncertainty_passed
    published = probability["probability"]

    if not state.active:
        decision_key = "hold"
        decision_label = "HOLD - modell tartalékban"
    elif published >= state.buy_threshold and trend_passed and risk_passed:
        decision_key = "buy_candidate"
        decision_label = "BUY-jelölt"
    elif published <= state.sell_threshold or not risk_passed:
        decision_key = "risk_off"
        decision_label = "Kivárás"
    else:
        decision_key = "hold"
        decision_label = "HOLD"

    return {
        "event": {
            "label": (
                f"Annak esélye, hogy a {horizon_days} napos hozam eléri "
                f"a +{spec.target_return_pct:g}%-ot"
            ),
            "formula": f"P({horizon_days} napos hozam >= +{spec.target_return_pct:g}%)",
            "horizon_days": horizon_days,
            "target_return_pct": spec.target_return_pct,
        },
        "probability_pct": round(probability["probability"] * 100, 2),
        "candidate_probability_pct": (
            round(probability["candidate_probability"] * 100, 2)
            if probability["candidate_probability"] is not None
            else None
        ),
        "baseline_probability_pct": round(probability["baseline_probability"] * 100, 2),
        "active": state.active,
        "available": state.available,
        "model": {
            "key": state.candidate_key,
            "family": state.family,
            "calibration": CALIBRATION_METHOD,
            "shrinkage": state.shrinkage,
            "refit_days": spec.refit_days,
        },
        "decision": {
            "key": decision_key,
            "label": decision_label,
            "manual_confirmation_required": True,
        },
        "thresholds": {
            "buy_probability_pct": round(state.buy_threshold * 100, 2),
            "risk_off_probability_pct": round(state.sell_threshold * 100, 2),
        },
        "calibration": {
            "training_samples": state.training_samples,
            "holdout_samples": state.holdout_samples,
            "validation_brier_skill_pct": state.validation_brier_skill_pct,
            "holdout_brier_score": state.holdout_brier_score,
            "baseline_brier_score": state.baseline_brier_score,
            "holdout_brier_skill_pct": state.holdout_brier_skill_pct,
            "holdout_log_loss": state.holdout_log_loss,
            "baseline_log_loss": state.baseline_log_loss,
            "roc_auc": state.roc_auc,
            "calibration_error_pct": (
                round(state.calibration_error * 100, 2)
                if state.calibration_error is not None
                else None
            ),
            "buy_precision_pct": state.buy_precision_pct,
            "buy_recall_pct": state.buy_recall_pct,
            "buy_signal_coverage_pct": state.buy_signal_coverage_pct,
            "reliability_bins": state.reliability_bins,
            "validation_candidates": state.validation_candidates,
        },
        "stability": {
            "mean_brier_skill_pct": state.stability_mean_skill_pct,
            "positive_blocks": state.stability_positive_blocks,
            "total_blocks": state.stability_total_blocks,
            "historical_mean_brier_skill_pct": state.historical_mean_skill_pct,
            "historical_positive_checks": state.historical_positive_checks,
            "historical_total_checks": state.historical_total_checks,
        },
        "filters": {
            "trend_passed": trend_passed,
            "risk_passed": risk_passed,
            "price_above_sma200": price_above_sma200,
            "ema20_above_ema50": ema_alignment,
            "rsi_in_45_70": rsi_in_band,
            "btc_market_trend_positive": market_trend,
            "market_context_available": market_context_available,
            "volatility_passed": volatility_passed,
            "uncertainty_passed": uncertainty_passed,
            "prediction_interval_width_pct": round(interval_width, 2),
        },
        "top_features": state.top_features,
        "reason": state.reason,
        "methodology": (
            "Időrendi train, kalibráció, validáció és érintetlen holdout; "
            "a szakaszhatárok körül a teljes előrejelzési horizont ki van zárva."
        ),
    }


def probability_registry_payload() -> list[dict[str, Any]]:
    return [
        {
            "horizon_days": horizon,
            "key": spec.key,
            "label": spec.label,
            "target_return_pct": spec.target_return_pct,
            "minimum_samples": spec.min_samples,
            "refit_days": spec.refit_days,
        }
        for horizon, spec in PROBABILITY_REGISTRY.items()
    ]
