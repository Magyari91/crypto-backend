from typing import Any

from app.intraday_models import (
    INTRADAY_HORIZON_HOURS,
    INTRADAY_LOOKBACK_HOURS,
    build_intraday_feature_vector,
    clean_intraday_candles,
    intraday_estimate_from_state,
    prepare_intraday_data,
    train_intraday_specialist,
)
from app.intraday_risk import risk_estimate_from_state, train_intraday_risk


MODEL_LAB_VERSION = "1.0.0"


def build_model_lab(
    candles: list[dict[str, Any]],
    horizon_days: int,
    direction_threshold: float,
) -> dict[str, Any]:
    if horizon_days not in INTRADAY_HORIZON_HOURS:
        raise ValueError("Az órás modelllabor csak 1 vagy 7 napos időtávhoz érhető el.")

    cleaned = clean_intraday_candles(candles)
    if len(cleaned) < INTRADAY_LOOKBACK_HOURS + 1:
        raise ValueError("Nincs elegendő órás adat a modelllaborhoz.")

    closed = cleaned[:-1]
    prepared = prepare_intraday_data(closed, horizon_days)
    known_through = (
        len(closed) - 1 - INTRADAY_HORIZON_HOURS[horizon_days]
    )
    current_features = build_intraday_feature_vector(cleaned)
    direction_state = train_intraday_specialist(
        prepared,
        known_through_origin=known_through,
        direction_threshold=direction_threshold,
    )
    risk_state = train_intraday_risk(
        prepared,
        known_through_origin=known_through,
    )
    direction = intraday_estimate_from_state(
        direction_state,
        current_features,
        horizon_days,
    )
    direction.pop("residuals", None)

    return {
        "version": MODEL_LAB_VERSION,
        "horizon_days": horizon_days,
        "history_hours": len(cleaned),
        "data_resolution": "1h",
        "direction_candidate": direction,
        "risk_candidate": risk_estimate_from_state(
            risk_state,
            current_features,
        ),
        "methodology": (
            "A jelöltek csak lezárt órás gyertyákon tanulnak. A tanító-, "
            "validációs és holdout-határok körül a teljes előrejelzési "
            "horizont kimarad, a kockázati modell pedig három korábbi "
            "időblokkon is stabil előnyt követel."
        ),
    }
