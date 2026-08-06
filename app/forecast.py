from datetime import datetime, timezone
from math import isfinite, sqrt
from statistics import mean, stdev
from typing import Any

from app.probability_models import build_probability_forecast
from app.specialist_models import build_specialist_estimate

MODEL_NAME = "Kalibrált horizont-specialista ensemble"
MODEL_VERSION = "4.0.0"
DIRECTION_THRESHOLDS = {1: 0.20, 7: 0.75, 30: 1.50}
MINIMUM_FEATURE_DAYS = 50
MAX_CALIBRATION_SAMPLES = 60
MINIMUM_METHOD_SKILL = 0.02

HORIZON_CONFIG = {
    1: {"momentum_days": 3, "secondary_days": 7, "limit": 2.5},
    7: {"momentum_days": 7, "secondary_days": 21, "limit": 6.0},
    30: {"momentum_days": 30, "secondary_days": 60, "limit": 12.0},
}

REGIME_LABELS = {
    "trend": "Trendelő piac",
    "range": "Oldalazó piac",
    "high_volatility": "Magas volatilitás",
}

REGIME_PRIORS = {
    "trend": {
        "momentum": 0.32,
        "trend": 0.32,
        "drift": 0.18,
        "mean_reversion": 0.05,
        "volume_momentum": 0.13,
    },
    "range": {
        "momentum": 0.12,
        "trend": 0.08,
        "drift": 0.12,
        "mean_reversion": 0.52,
        "volume_momentum": 0.08,
    },
    "high_volatility": {
        "momentum": 0.12,
        "trend": 0.10,
        "drift": 0.08,
        "mean_reversion": 0.20,
        "volume_momentum": 0.10,
    },
}


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _clean_number(value: Any, digits: int = 2) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number):
        return None
    return round(number, digits)


def classify_direction(change_pct: float, horizon_days: int) -> tuple[str, str]:
    threshold = DIRECTION_THRESHOLDS.get(horizon_days, 1.0)
    if change_pct > threshold:
        return "Emelkedő", "bullish"
    if change_pct < -threshold:
        return "Csökkenő", "bearish"
    return "Semleges", "neutral"


def _ema(values: list[float], span: int) -> list[float]:
    alpha = 2 / (span + 1)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1 - alpha) * output[-1])
    return output


def _latest_rsi(values: list[float], window: int = 14) -> float:
    deltas = [current - previous for previous, current in zip(values, values[1:])]
    if len(deltas) < window:
        return 50.0

    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]
    average_gain = sum(gains[:window]) / window
    average_loss = sum(losses[:window]) / window

    for gain, loss in zip(gains[window:], losses[window:]):
        average_gain = ((window - 1) * average_gain + gain) / window
        average_loss = ((window - 1) * average_loss + loss) / window

    if average_gain == 0 and average_loss == 0:
        return 50.0
    if average_loss == 0:
        return 100.0
    if average_gain == 0:
        return 0.0
    relative_strength = average_gain / average_loss
    return 100 - (100 / (1 + relative_strength))


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = position - lower_index
    return ordered[lower_index] * (1 - weight) + ordered[upper_index] * weight


def _series_by_day(series: list[list[float]]) -> dict[str, dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for item in series:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            timestamp = datetime.fromtimestamp(float(item[0]) / 1000, timezone.utc)
            value = float(item[1])
        except (TypeError, ValueError, OSError):
            continue
        if not isfinite(value) or value < 0:
            continue
        by_day[timestamp.date().isoformat()] = {
            "timestamp": timestamp.isoformat(),
            "value": value,
        }
    return by_day


def daily_value_map(series: list[list[float]]) -> dict[str, float]:
    return {day: item["value"] for day, item in _series_by_day(series).items()}


def daily_points(prices: list[list[float]]) -> list[dict[str, Any]]:
    if not prices:
        raise ValueError("Nincsenek árfolyamadatok")

    points = [
        {"timestamp": item["timestamp"], "price": item["value"]}
        for _day, item in sorted(_series_by_day(prices).items())
        if item["value"] > 0
    ]
    if len(points) < 35:
        raise ValueError("Legalább 35 napi adat szükséges")
    return points


def _indicator_values(values: list[float]) -> dict[str, float]:
    ema20_values = _ema(values, 20)
    ema50_values = _ema(values, 50)
    ema12_values = _ema(values, 12)
    ema26_values = _ema(values, 26)
    macd_values = [fast - slow for fast, slow in zip(ema12_values, ema26_values)]
    macd_signal_values = _ema(macd_values, 9)
    recent = values[-20:]
    middle = mean(recent)
    deviation = stdev(recent) if len(recent) > 1 else 0.0

    return {
        "price": values[-1],
        "ema20": ema20_values[-1],
        "ema50": ema50_values[-1],
        "sma200": mean(values[-min(200, len(values)) :]),
        "rsi": _latest_rsi(values),
        "macd_histogram": macd_values[-1] - macd_signal_values[-1],
        "bollinger_middle": middle,
        "bollinger_upper": middle + 2 * deviation,
        "bollinger_lower": middle - 2 * deviation,
    }


def calculate_indicators(prices: list[list[float]]) -> dict[str, Any]:
    points = daily_points(prices)
    values = [point["price"] for point in points]
    return {"points": points, **_indicator_values(values)}


def _lookback_change(values: list[float], days: int) -> float:
    lookback = min(days, len(values) - 1)
    return ((values[-1] / values[-lookback - 1]) - 1) * 100


def _trimmed_daily_drift(returns: list[float], horizon_days: int) -> float:
    recent = sorted(returns[-30:])
    if len(recent) >= 10:
        trim = max(1, len(recent) // 10)
        recent = recent[trim:-trim]
    return mean(recent) * horizon_days * 100 if recent else 0.0


def _volume_ratio(volumes: list[float | None]) -> float | None:
    valid = [float(value) for value in volumes if value is not None and value > 0]
    if len(valid) < 21:
        return None
    short = mean(valid[-7:])
    long = mean(valid[-30:])
    return short / long if long > 0 else None


def technical_snapshot(
    values: list[float],
    horizon_days: int,
    volumes: list[float | None] | None = None,
) -> dict[str, Any]:
    config = HORIZON_CONFIG[horizon_days]
    indicators = _indicator_values(values)
    price = values[-1]
    ema20 = indicators["ema20"]
    ema50 = indicators["ema50"]
    returns = [(current / previous) - 1 for previous, current in zip(values, values[1:])]
    recent_returns = returns[-30:]
    daily_volatility = stdev(recent_returns) if len(recent_returns) > 1 else 0.0
    annualized_volatility = daily_volatility * sqrt(365) * 100

    momentum = _lookback_change(values, config["momentum_days"])
    secondary_momentum = _lookback_change(values, config["secondary_days"])
    trend_pct = ((ema20 / ema50) - 1) * 100
    deviation_pct = ((price / ema20) - 1) * 100
    trend_noise = max(daily_volatility * sqrt(20) * 100, 0.35)
    trend_strength = abs(trend_pct) / trend_noise
    aligned_trend = trend_pct * momentum > 0

    if annualized_volatility >= 85:
        regime = "high_volatility"
    elif aligned_trend and trend_strength >= 0.28:
        regime = "trend"
    else:
        regime = "range"

    limit = config["limit"]
    horizon_scale = max(0.22, sqrt(horizon_days / 20))
    momentum_candidate = _clamp(
        momentum * 0.30 + secondary_momentum * 0.10,
        -limit,
        limit,
    )
    trend_candidate = _clamp(trend_pct * horizon_scale * 0.85, -limit, limit)
    drift_candidate = _clamp(_trimmed_daily_drift(returns, horizon_days), -limit, limit)
    mean_reversion_candidate = _clamp(
        -deviation_pct * 0.35 * max(0.45, sqrt(horizon_days / 7)),
        -limit,
        limit,
    )
    volume_ratio = _volume_ratio(volumes or [])
    volume_candidate = momentum_candidate
    if volume_ratio is not None:
        volume_candidate *= _clamp(volume_ratio, 0.7, 1.3)
    volume_candidate = _clamp(volume_candidate, -limit, limit)

    return {
        "regime": regime,
        "regime_label": REGIME_LABELS[regime],
        "trend_strength": trend_strength,
        "volatility": annualized_volatility,
        "daily_volatility": daily_volatility,
        "momentum_pct": momentum,
        "secondary_momentum_pct": secondary_momentum,
        "trend_pct": trend_pct,
        "deviation_pct": deviation_pct,
        "volume_ratio": volume_ratio,
        "candidates": {
            "momentum": momentum_candidate,
            "trend": trend_candidate,
            "drift": drift_candidate,
            "mean_reversion": mean_reversion_candidate,
            "volume_momentum": volume_candidate,
        },
        **indicators,
    }


def _calibration_samples(
    values: list[float],
    horizon_days: int,
    volumes: list[float | None],
) -> list[dict[str, Any]]:
    last_anchor = len(values) - horizon_days - 1
    first_anchor = MINIMUM_FEATURE_DAYS - 1
    if last_anchor < first_anchor:
        return []
    first_anchor = max(first_anchor, last_anchor - MAX_CALIBRATION_SAMPLES + 1)

    samples = []
    for anchor in range(first_anchor, last_anchor + 1):
        snapshot = technical_snapshot(
            values[: anchor + 1],
            horizon_days,
            volumes[: anchor + 1],
        )
        actual_change = ((values[anchor + horizon_days] / values[anchor]) - 1) * 100
        samples.append(
            {
                "regime": snapshot["regime"],
                "candidates": snapshot["candidates"],
                "actual_change_pct": actual_change,
            }
        )
    return samples


def _weighted_mae(
    samples: list[dict[str, Any]],
    prediction,
    current_regime: str,
) -> float:
    weighted_error = 0.0
    total_weight = 0.0
    count = len(samples)
    for index, sample in enumerate(samples):
        recency = 0.65 + 0.35 * ((index + 1) / count)
        regime_weight = 1.25 if sample["regime"] == current_regime else 1.0
        weight = recency * regime_weight
        weighted_error += abs(prediction(sample) - sample["actual_change_pct"]) * weight
        total_weight += weight
    return weighted_error / total_weight if total_weight else 0.0


def _empty_calibration(
    samples: list[dict[str, Any]],
    validation_skill_pct: float = 0.0,
) -> dict[str, Any]:
    return {
        "weights": {},
        "reliability": 0.0,
        "reliability_score": 0,
        "validation_samples": len(samples),
        "holdout_samples": 0,
        "holdout_directional_accuracy": None,
        "holdout_signal_coverage_pct": 0.0,
        "validation_skill_pct": round(validation_skill_pct, 2),
        "method_skills": {},
        "residuals": [sample["actual_change_pct"] for sample in samples[-30:]],
    }


def _fit_candidate_weights(
    samples: list[dict[str, Any]],
    current_regime: str,
) -> dict[str, Any]:
    baseline_mae = _weighted_mae(samples, lambda _sample: 0.0, current_regime)
    priors = REGIME_PRIORS[current_regime]
    scores = {}
    method_skills = {}

    for method, prior in priors.items():
        method_mae = _weighted_mae(
            samples,
            lambda sample, key=method: sample["candidates"][key],
            current_regime,
        )
        skill = (baseline_mae - method_mae) / baseline_mae if baseline_mae else 0.0
        method_skills[method] = skill
        if skill > MINIMUM_METHOD_SKILL:
            scores[method] = ((skill - MINIMUM_METHOD_SKILL) ** 1.5) * prior

    total_score = sum(scores.values())
    if total_score <= 0:
        return {
            "weights": {},
            "method_skills": method_skills,
            "strongest_skill": max(method_skills.values(), default=0.0),
        }

    weights = {method: score / total_score for method, score in scores.items()}
    return {
        "weights": weights,
        "method_skills": method_skills,
        "strongest_skill": max(method_skills.values()),
    }


def _calibrate_ensemble(
    samples: list[dict[str, Any]],
    current_regime: str,
    horizon_days: int,
) -> dict[str, Any]:
    if len(samples) < 24:
        return _empty_calibration(samples)

    split_index = min(len(samples) - 8, max(14, round(len(samples) * 0.65)))
    training = samples[:split_index]
    holdout = samples[split_index:]
    fit = _fit_candidate_weights(training, current_regime)
    weights = fit["weights"]
    if not weights:
        empty = _empty_calibration(holdout)
        empty["validation_samples"] = len(samples)
        empty["holdout_samples"] = len(holdout)
        empty["method_skills"] = fit["method_skills"]
        return empty

    sample_factor = _clamp(len(training) / 45, 0.45, 1.0)
    reliability = _clamp(
        (fit["strongest_skill"] - MINIMUM_METHOD_SKILL) * 2.8,
        0.0,
        0.72,
    ) * sample_factor

    def prediction(sample: dict[str, Any], scale: float) -> float:
        return scale * sum(
            weights[method] * sample["candidates"][method]
            for method in weights
        )

    baseline_mae = _weighted_mae(holdout, lambda _sample: 0.0, current_regime)
    initial_mae = _weighted_mae(
        holdout,
        lambda sample: prediction(sample, reliability),
        current_regime,
    )
    initial_skill = (baseline_mae - initial_mae) / baseline_mae if baseline_mae else 0.0
    if initial_skill <= 0.01:
        empty = _empty_calibration(holdout, initial_skill * 100)
        empty["validation_samples"] = len(samples)
        empty["holdout_samples"] = len(holdout)
        empty["method_skills"] = fit["method_skills"]
        return empty

    reliability *= _clamp(initial_skill * 4, 0.15, 1.0)
    holdout_active_hits = []
    for sample in holdout:
        _, predicted_direction = classify_direction(
            prediction(sample, reliability),
            horizon_days,
        )
        _, actual_direction = classify_direction(
            sample["actual_change_pct"],
            horizon_days,
        )
        if predicted_direction != "neutral":
            holdout_active_hits.append(predicted_direction == actual_direction)

    holdout_accuracy = (
        mean(holdout_active_hits) * 100 if holdout_active_hits else None
    )
    holdout_coverage = len(holdout_active_hits) / len(holdout) * 100
    if len(holdout_active_hits) >= 4 and holdout_accuracy < 52:
        empty = _empty_calibration(holdout, initial_skill * 100)
        empty["validation_samples"] = len(samples)
        empty["holdout_samples"] = len(holdout)
        empty["holdout_directional_accuracy"] = round(holdout_accuracy, 2)
        empty["holdout_signal_coverage_pct"] = round(holdout_coverage, 2)
        empty["method_skills"] = fit["method_skills"]
        return empty
    ensemble_mae = _weighted_mae(
        holdout,
        lambda sample: prediction(sample, reliability),
        current_regime,
    )
    validation_skill = (baseline_mae - ensemble_mae) / baseline_mae * 100
    residuals = [
        sample["actual_change_pct"] - prediction(sample, reliability)
        for sample in holdout
    ]

    return {
        "weights": weights,
        "reliability": reliability,
        "reliability_score": round(reliability * 100),
        "validation_samples": len(samples),
        "holdout_samples": len(holdout),
        "holdout_directional_accuracy": _clean_number(holdout_accuracy, 2),
        "holdout_signal_coverage_pct": round(holdout_coverage, 2),
        "validation_skill_pct": round(validation_skill, 2),
        "method_skills": fit["method_skills"],
        "residuals": residuals,
    }


def _prediction_interval(
    expected_change: float,
    calibration: dict[str, Any],
    snapshot: dict[str, Any],
    horizon_days: int,
) -> tuple[float, float]:
    residuals = calibration["residuals"]
    if len(residuals) >= 18:
        lower = expected_change + _quantile(residuals, 0.10)
        upper = expected_change + _quantile(residuals, 0.90)
    else:
        daily_risk_pct = snapshot["daily_volatility"] * 100
        range_size = max(daily_risk_pct * sqrt(horizon_days) * 1.28, 0.5)
        lower = expected_change - range_size
        upper = expected_change + range_size

    return max(min(lower, expected_change), -95.0), min(max(upper, expected_change), 300.0)


def build_model_estimate(
    snapshot: dict[str, Any],
    samples: list[dict[str, Any]],
    horizon_days: int,
) -> dict[str, Any]:
    calibration = _calibrate_ensemble(samples, snapshot["regime"], horizon_days)
    raw_ensemble = sum(
        calibration["weights"][method] * snapshot["candidates"][method]
        for method in calibration["weights"]
    )
    expected_change = raw_ensemble * calibration["reliability"]
    limit = HORIZON_CONFIG[horizon_days]["limit"]
    expected_change = _clamp(expected_change, -limit, limit)
    direction, direction_key = classify_direction(expected_change, horizon_days)
    lower_change, upper_change = _prediction_interval(
        expected_change,
        calibration,
        snapshot,
        horizon_days,
    )

    candidate_signs = [
        1 if value > 0.1 else -1 if value < -0.1 else 0
        for value in snapshot["candidates"].values()
    ]
    positive = sum(sign > 0 for sign in candidate_signs)
    negative = sum(sign < 0 for sign in candidate_signs)
    agreement = max(positive, negative) / len(candidate_signs)
    quality = 40 + calibration["reliability"] * 35 + agreement * 12
    if snapshot["regime"] == "high_volatility":
        quality -= 5

    return {
        "calibration": calibration,
        "model_reliability": calibration["reliability"],
        "validation_skill_pct": calibration["validation_skill_pct"],
        "raw_ensemble": raw_ensemble,
        "expected_change": expected_change,
        "direction": direction,
        "direction_key": direction_key,
        "lower_change": lower_change,
        "upper_change": upper_change,
        "confidence": _clamp(quality, 38, 78),
    }


def apply_specialist_estimate(
    estimate: dict[str, Any],
    specialist: dict[str, Any],
    horizon_days: int,
) -> dict[str, Any]:
    output = dict(estimate)
    output["specialist"] = specialist
    if not specialist["active"]:
        return output

    technical_change = float(estimate["expected_change"])
    specialist_change = float(specialist["prediction_pct"])
    blend_weight = float(specialist["blend_weight"])
    expected_change = (
        technical_change * (1 - blend_weight)
        + specialist_change * blend_weight
    )
    limit = HORIZON_CONFIG[horizon_days]["limit"]
    expected_change = _clamp(expected_change, -limit, limit)
    interval_shift = expected_change - technical_change
    lower_change = _clamp(
        float(estimate["lower_change"]) + interval_shift,
        -95.0,
        expected_change,
    )
    upper_change = _clamp(
        float(estimate["upper_change"]) + interval_shift,
        expected_change,
        300.0,
    )
    direction, direction_key = classify_direction(expected_change, horizon_days)
    skill_bonus = _clamp(float(specialist["validation_skill_pct"]) / 4, 0.0, 5.0)

    output.update(
        {
            "expected_change": expected_change,
            "direction": direction,
            "direction_key": direction_key,
            "lower_change": lower_change,
            "upper_change": upper_change,
            "confidence": _clamp(float(estimate["confidence"]) + skill_bonus, 38, 82),
        }
    )
    return output


def build_forecast(
    prices: list[list[float]],
    horizon_days: int,
    current_price: float | None = None,
    volumes: list[list[float]] | None = None,
    market_prices: list[list[float]] | None = None,
) -> dict[str, Any]:
    if horizon_days not in HORIZON_CONFIG:
        raise ValueError("Az időtáv 1, 7 vagy 30 nap lehet.")

    indicators = calculate_indicators(prices)
    points = indicators["points"]
    values = [float(point["price"]) for point in points]
    base_price = float(current_price or values[-1])
    values[-1] = base_price

    volume_by_day = daily_value_map(volumes or [])
    volume_values: list[float | None] = [
        volume_by_day.get(_parse_point_day(point["timestamp"]))
        for point in points
    ]
    market_by_day = daily_value_map(market_prices or prices)
    market_values = []
    previous_market_value = None
    matched_market_days = 0
    for point, fallback_value in zip(points, values):
        market_value = market_by_day.get(_parse_point_day(point["timestamp"]))
        if market_value is not None and market_value > 0:
            previous_market_value = market_value
            matched_market_days += 1
        market_values.append(float(previous_market_value or fallback_value))
    market_context_available = bool(market_prices) and matched_market_days >= min(
        200,
        round(len(points) * 0.90),
    )
    snapshot = technical_snapshot(values, horizon_days, volume_values)
    samples = _calibration_samples(values, horizon_days, volume_values)
    estimate = build_model_estimate(snapshot, samples, horizon_days)
    specialist = build_specialist_estimate(
        values,
        volume_values,
        horizon_days,
        DIRECTION_THRESHOLDS[horizon_days],
    )
    estimate = apply_specialist_estimate(estimate, specialist, horizon_days)
    calibration = estimate["calibration"]
    expected_change = estimate["expected_change"]
    direction = estimate["direction"]
    direction_key = estimate["direction_key"]
    lower_change = estimate["lower_change"]
    upper_change = estimate["upper_change"]
    probability_forecast = build_probability_forecast(
        values,
        volume_values,
        market_values,
        horizon_days,
        snapshot,
        lower_change,
        upper_change,
        market_context_available,
    )

    recent_prices = values[-20:]
    support = _quantile(recent_prices, 0.10)
    resistance = _quantile(recent_prices, 0.90)
    daily_std = snapshot["daily_volatility"] or 0.01
    if support >= base_price:
        support = base_price * (1 - max(daily_std, 0.01) * 2)
    if resistance <= base_price:
        resistance = base_price * (1 + max(daily_std, 0.01) * 2)

    if snapshot["volatility"] < 45:
        volatility_label = "Alacsony"
    elif snapshot["volatility"] < 80:
        volatility_label = "Közepes"
    else:
        volatility_label = "Magas"

    confidence = estimate["confidence"]

    validation_signal = (
        "A kalibrált ensemble jobb volt a semleges alapmodellnél"
        if estimate["validation_skill_pct"] > 0
        else "A semleges alapmodell kapta a legnagyobb súlyt"
    )
    specialist_signal = specialist["reason"]
    volume_signal = (
        f"A rövid távú forgalom a 30 napos átlag {snapshot['volume_ratio']:.2f}-szerese"
        if snapshot["volume_ratio"] is not None
        else "A forgalmi megerősítéshez nincs elegendő adat"
    )

    probability_signal = (
        f"{probability_forecast['event']['formula']}: "
        f"{probability_forecast['probability_pct']:.1f}%"
        if probability_forecast["active"]
        else f"Valószínűségi kapu: {probability_forecast['reason']}"
    )
    decision_signal = f"Döntési kapu: {probability_forecast['decision']['label']}"

    chart_points = [
        {"timestamp": point["timestamp"], "price": round(float(point["price"]), 8)}
        for point in points[-60:]
    ]

    return {
        "base_price": round(base_price, 8),
        "direction": direction,
        "direction_key": direction_key,
        "expected_change_pct": round(expected_change, 2),
        "target_price": round(base_price * (1 + expected_change / 100), 8),
        "prediction_interval": {
            "confidence_level": 80,
            "lower_change_pct": round(lower_change, 2),
            "upper_change_pct": round(upper_change, 2),
            "lower_price": round(base_price * (1 + lower_change / 100), 8),
            "upper_price": round(base_price * (1 + upper_change / 100), 8),
        },
        "confidence": round(confidence),
        "confidence_label": "Visszamért jelminőség",
        "horizon_days": horizon_days,
        "support": round(support, 8),
        "resistance": round(resistance, 8),
        "volatility": round(snapshot["volatility"], 1),
        "volatility_label": volatility_label,
        "regime": {
            "key": snapshot["regime"],
            "label": snapshot["regime_label"],
            "trend_strength": round(snapshot["trend_strength"], 2),
        },
        "model": f"{MODEL_NAME} v4",
        "model_version": MODEL_VERSION,
        "ensemble": {
            "weights": {
                method: round(weight, 4)
                for method, weight in calibration["weights"].items()
            },
            "validation_samples": calibration["validation_samples"],
            "holdout_samples": calibration["holdout_samples"],
            "holdout_directional_accuracy": calibration["holdout_directional_accuracy"],
            "holdout_signal_coverage_pct": calibration["holdout_signal_coverage_pct"],
            "validation_skill_pct": estimate["validation_skill_pct"],
            "reliability_score": round(estimate["model_reliability"] * 100),
            "candidate_predictions": {
                method: round(value, 2)
                for method, value in snapshot["candidates"].items()
            },
        },
        "specialist": {
            key: value
            for key, value in specialist.items()
            if key != "residuals"
        },
        "probability_forecast": probability_forecast,
        "signals": [
            snapshot["regime_label"],
            probability_signal,
            decision_signal,
            specialist_signal,
            validation_signal,
            volume_signal,
        ],
        "indicators": {
            "rsi": _clean_number(snapshot["rsi"], 1),
            "ema20": _clean_number(snapshot["ema20"], 8),
            "ema50": _clean_number(snapshot["ema50"], 8),
            "sma200": _clean_number(snapshot["sma200"], 8),
            "macd": _clean_number(snapshot["macd_histogram"], 8),
            "momentum_pct": _clean_number(snapshot["momentum_pct"], 2),
            "volume_ratio": _clean_number(snapshot["volume_ratio"], 2),
        },
        "series": chart_points,
    }


def _parse_point_day(value: str) -> str:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).date().isoformat()
