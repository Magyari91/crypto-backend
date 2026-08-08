import argparse
import asyncio
import json

from app.forecast import DIRECTION_THRESHOLDS
from app.intraday_models import (
    build_intraday_estimate,
    walk_forward_intraday_backtest,
)
from app.market_data import MarketDataService


COINS = ("bitcoin", "ethereum", "solana", "ripple", "dogecoin")
HORIZONS = (1, 7)


async def run(hours: int, walk_forward_samples: int) -> list[dict]:
    service = MarketDataService()
    await service.start()
    rows = []
    try:
        for coin in COINS:
            history = await service.forecast_intraday_history(coin, hours=hours)
            candles = history["candles"]
            for horizon in HORIZONS:
                estimate = build_intraday_estimate(
                    candles,
                    horizon_days=horizon,
                    direction_threshold=DIRECTION_THRESHOLDS[horizon],
                    cache_key=coin,
                )
                row = {
                    "coin": coin,
                    "horizon": horizon,
                    "family": estimate["family"],
                    "active": estimate["active"],
                    "prediction_pct": estimate["prediction_pct"],
                    "training_samples": estimate["training_samples"],
                    "holdout_samples": estimate["holdout_samples"],
                    "holdout_skill_pct": estimate["validation_skill_pct"],
                    "holdout_directional_accuracy": estimate[
                        "holdout_directional_accuracy"
                    ],
                    "holdout_signal_coverage_pct": estimate[
                        "holdout_signal_coverage_pct"
                    ],
                    "reason": estimate["reason"],
                }
                if walk_forward_samples > 0:
                    row["walk_forward"] = walk_forward_intraday_backtest(
                        candles,
                        horizon_days=horizon,
                        direction_threshold=DIRECTION_THRESHOLDS[horizon],
                        max_samples=walk_forward_samples,
                    )["summary"]
                rows.append(row)
    finally:
        await service.close()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=6480)
    parser.add_argument("--walk-forward-samples", type=int, default=0)
    args = parser.parse_args()
    rows = asyncio.run(run(args.hours, args.walk_forward_samples))
    print(json.dumps(rows, ensure_ascii=False))


if __name__ == "__main__":
    main()
