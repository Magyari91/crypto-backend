from app.derivatives import DAY_MS, normalize_derivatives


def test_derivatives_normalization_builds_daily_funding_and_snapshot():
    funding = [
        {
            "fundingTime": DAY_MS + offset * 8 * 3_600_000,
            "fundingRate": value,
        }
        for offset, value in enumerate(("0.0001", "0.0002", "-0.0001"))
    ]
    open_interest = [
        {"timestamp": DAY_MS * day, "sumOpenInterestValue": str(100 + day * 10)}
        for day in range(1, 10)
    ]
    long_short = [
        {
            "timestamp": DAY_MS * 9,
            "longShortRatio": "1.25",
            "longAccount": "0.5556",
            "shortAccount": "0.4444",
        }
    ]
    taker = [
        {
            "timestamp": DAY_MS * 9,
            "buySellRatio": "1.5",
            "buyVol": "150",
            "sellVol": "100",
        }
    ]

    payload = normalize_derivatives(funding, open_interest, long_short, taker)
    snapshot = payload["snapshot"]

    assert payload["funding_rates"] == [[DAY_MS, 0.006666666666666667]]
    assert snapshot["available"] is True
    assert snapshot["funding_rate_pct"] == 0.006667
    assert snapshot["open_interest_change_7d_pct"] > 0
    assert snapshot["long_short_ratio"] == 1.25
    assert snapshot["taker_buy_share_pct"] == 60.0


def test_empty_derivatives_payload_is_explicitly_unavailable():
    payload = normalize_derivatives()

    assert payload["funding_rates"] == []
    assert payload["snapshot"]["available"] is False
    assert payload["snapshot"]["funding_rate_pct"] is None
