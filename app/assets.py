from typing import Any


ANALYSIS_ASSETS: dict[str, dict[str, Any]] = {
    "bitcoin": {"symbol": "BTC", "name": "Bitcoin", "analysis_rank": 1},
    "ethereum": {"symbol": "ETH", "name": "Ethereum", "analysis_rank": 2},
    "binancecoin": {"symbol": "BNB", "name": "BNB", "analysis_rank": 3},
    "ripple": {"symbol": "XRP", "name": "XRP", "analysis_rank": 4},
    "solana": {"symbol": "SOL", "name": "Solana", "analysis_rank": 5},
    "tron": {"symbol": "TRX", "name": "TRON", "analysis_rank": 6},
    "hyperliquid": {"symbol": "HYPE", "name": "Hyperliquid", "analysis_rank": 7},
    "dogecoin": {"symbol": "DOGE", "name": "Dogecoin", "analysis_rank": 8},
    "zcash": {"symbol": "ZEC", "name": "Zcash", "analysis_rank": 9},
    "cardano": {"symbol": "ADA", "name": "Cardano", "analysis_rank": 10},
}

ANALYSIS_LIMIT = len(ANALYSIS_ASSETS)


def analysis_asset_list() -> list[dict[str, Any]]:
    return [
        {"id": coin_id, **metadata}
        for coin_id, metadata in ANALYSIS_ASSETS.items()
    ]
