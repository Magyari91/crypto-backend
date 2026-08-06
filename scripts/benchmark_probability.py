import argparse
import asyncio
import json

from app.dashboard import SUPPORTED_COINS
from app.backtest import walk_forward_backtest
from app.forecast import build_forecast
from app.market_data import MarketDataService


async def benchmark(
    coins: list[str],
    horizons: list[int],
    walk_forward_samples: int,
) -> list[dict]:
    service = MarketDataService()
    await service.start()
    try:
        bitcoin_history = await service.forecast_history("bitcoin", days=2000)
        output = []
        for coin in coins:
            history = (
                bitcoin_history
                if coin == "bitcoin"
                else await service.forecast_history(coin, days=2000)
            )
            prices = history["prices"]
            for horizon in horizons:
                forecast = await asyncio.to_thread(
                    build_forecast,
                    prices,
                    horizon,
                    prices[-1][1],
                    history.get("total_volumes", []),
                    bitcoin_history["prices"],
                    funding_rates=history.get("funding_rates", []),
                )
                probability = forecast["probability_forecast"]
                result = {
                        "coin": coin,
                        "horizon_days": horizon,
                        "source": history.get("source"),
                        "history_days": len(prices),
                        "event": probability["event"]["formula"],
                        "published_probability_pct": probability["probability_pct"],
                        "candidate_probability_pct": probability[
                            "candidate_probability_pct"
                        ],
                        "baseline_probability_pct": probability[
                            "baseline_probability_pct"
                        ],
                        "active": probability["active"],
                        "family": probability["model"]["family"],
                        "validation_candidates": probability["calibration"][
                            "validation_candidates"
                        ],
                        "decision": probability["decision"]["key"],
                        "holdout_brier_skill_pct": probability["calibration"][
                            "holdout_brier_skill_pct"
                        ],
                        "roc_auc": probability["calibration"]["roc_auc"],
                        "calibration_error_pct": probability["calibration"][
                            "calibration_error_pct"
                        ],
                        "stability_mean_skill_pct": probability["stability"][
                            "mean_brier_skill_pct"
                        ],
                        "positive_blocks": probability["stability"][
                            "positive_blocks"
                        ],
                        "total_blocks": probability["stability"]["total_blocks"],
                        "historical_mean_skill_pct": probability["stability"][
                            "historical_mean_brier_skill_pct"
                        ],
                        "historical_positive_checks": probability["stability"][
                            "historical_positive_checks"
                        ],
                        "historical_total_checks": probability["stability"][
                            "historical_total_checks"
                        ],
                        "distribution_shift": probability["distribution_shift"],
                        "funding_history_days": history.get("derivatives", {}).get(
                            "funding_history_days",
                            0,
                        ),
                        "reason": probability["reason"],
                    }
                if walk_forward_samples > 0:
                    backtest = await asyncio.to_thread(
                        walk_forward_backtest,
                        prices,
                        horizon,
                        history.get("total_volumes", []),
                        walk_forward_samples,
                        bitcoin_history["prices"],
                        funding_rates=history.get("funding_rates", []),
                    )
                    result["walk_forward"] = backtest["summary"]["probability"]
                output.append(result)
        return output
    finally:
        await service.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the calibrated daily probability gate on Binance history."
    )
    parser.add_argument(
        "--coins",
        nargs="+",
        choices=sorted(SUPPORTED_COINS),
        default=list(SUPPORTED_COINS),
    )
    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        choices=(1, 7, 30),
        default=[1, 7, 30],
    )
    parser.add_argument(
        "--walk-forward-samples",
        type=int,
        default=0,
        help="Also run the rolling probability audit for the latest N samples.",
    )
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                benchmark(
                    arguments.coins,
                    arguments.horizons,
                    max(0, arguments.walk_forward_samples),
                )
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
