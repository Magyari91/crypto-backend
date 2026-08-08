import argparse
import asyncio
import json

from app.intraday_risk import (
    build_intraday_risk_estimate,
    walk_forward_intraday_risk,
)
from app.market_data import MarketDataService


async def run(
    coin: str,
    horizon: int,
    hours: int,
    samples: int,
) -> dict:
    service = MarketDataService()
    await service.start()
    try:
        history = await service.forecast_intraday_history(coin, hours=hours)
        estimate = build_intraday_risk_estimate(
            history["candles"],
            horizon_days=horizon,
            cache_key=coin,
        )
        backtest = walk_forward_intraday_risk(
            history["candles"],
            horizon_days=horizon,
            max_samples=samples,
        )
        return {
            "coin": coin,
            "horizon": horizon,
            "estimate": estimate,
            "walk_forward": backtest["summary"],
        }
    finally:
        await service.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coin", default="bitcoin")
    parser.add_argument("--horizon", type=int, choices=(1, 7), default=7)
    parser.add_argument("--hours", type=int, default=6480)
    parser.add_argument("--samples", type=int, default=60)
    args = parser.parse_args()
    result = asyncio.run(
        run(args.coin, args.horizon, args.hours, args.samples)
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
