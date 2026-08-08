from app.news import (
    aggregate_news_sentiment,
    article_sentiment,
    merge_articles,
    normalize_articles,
    parse_rss_feed,
)


RSS_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
  <channel>
    <title>Example crypto feed</title>
    <item>
      <title>Bitcoin and Ethereum market update</title>
      <link>https://example.test/story?utm_source=rss</link>
      <guid isPermaLink="false">story-1</guid>
      <pubDate>Fri, 10 Jul 2026 05:13:48 +0000</pubDate>
      <description><![CDATA[<p>BTC and ETH moved higher.</p>]]></description>
      <media:content url="https://example.test/image.jpg" type="image/jpeg" />
      <category>Markets</category>
    </item>
  </channel>
</rss>
"""


def test_parse_rss_feed_extracts_display_fields():
    articles = parse_rss_feed(RSS_SAMPLE, "Example")

    assert len(articles) == 1
    assert articles[0]["source"] == "Example"
    assert articles[0]["published_at"] == "2026-07-10T05:13:48+00:00"
    assert articles[0]["image"] == "https://example.test/image.jpg"
    assert articles[0]["summary"] == "BTC and ETH moved higher."
    assert articles[0]["category"] == "Markets"
    assert articles[0]["related_coin_ids"] == ["bitcoin", "ethereum"]


def test_parse_rss_feed_repairs_common_utf8_mojibake():
    payload = RSS_SAMPLE.replace(
        b"Bitcoin and Ethereum market update",
        "Fridayâs Bitcoin update".encode("utf-8"),
    )

    articles = parse_rss_feed(payload, "Example")

    assert articles[0]["title"] == "Friday’s Bitcoin update"


def test_normalize_cryptocompare_article():
    articles = normalize_articles(
        [
            {
                "id": "provider-1",
                "title": "Solana update",
                "url": "https://example.test/solana",
                "source_info": {"name": "Provider"},
                "published_on": 1_735_689_600,
                "imageurl": "https://example.test/solana.jpg",
                "body": "SOL market summary",
            }
        ]
    )

    assert articles[0]["source"] == "Provider"
    assert articles[0]["image"] == "https://example.test/solana.jpg"
    assert articles[0]["related_coin_ids"] == ["solana"]


def test_merge_deduplicates_and_keeps_sources_varied():
    articles = [
        {
            "title": f"CoinDesk story {index}",
            "url": f"https://coindesk.test/{index}",
            "source": "CoinDesk",
            "published_at": f"2026-07-10T0{5 - index}:00:00+00:00",
        }
        for index in range(4)
    ]
    articles.extend(
        [
            {
                "title": "Decrypt story",
                "url": "https://decrypt.test/story",
                "source": "Decrypt",
                "published_at": "2026-07-10T00:30:00+00:00",
            },
            {
                "title": "Cointelegraph story",
                "url": "https://cointelegraph.test/story",
                "source": "Cointelegraph",
                "published_at": "2026-07-10T00:20:00+00:00",
            },
            {
                "title": "Duplicate URL",
                "url": "https://decrypt.test/story?utm_source=copy",
                "source": "Other",
                "published_at": "2026-07-09T23:00:00+00:00",
            },
        ]
    )

    merged = merge_articles(articles)

    assert len(merged) == 6
    assert [article["source"] for article in merged[:4]] == [
        "CoinDesk",
        "CoinDesk",
        "Decrypt",
        "Cointelegraph",
    ]


def test_news_sentiment_is_coin_specific_and_auditable():
    articles = normalize_articles(
        [
            {
                "title": "Bitcoin ETF inflow supports a bullish rally",
                "summary": "Adoption growth continues",
                "url": "https://example.test/bitcoin-positive",
                "source": "Example",
            },
            {
                "title": "Solana exploit triggers losses",
                "url": "https://example.test/solana-negative",
                "source": "Example",
            },
        ]
    )

    bitcoin = aggregate_news_sentiment(articles, "bitcoin")
    assert article_sentiment("Market update")["label"] == "neutral"
    assert bitcoin["coin_specific"] is True
    assert bitcoin["sample_size"] == 1
    assert bitcoin["label"] == "positive"
