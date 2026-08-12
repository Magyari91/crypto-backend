from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import math
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import xml.etree.ElementTree as ElementTree

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


MAX_FEED_BYTES = 2_000_000
_HTML_TAG = re.compile(r"<[^>]+>")
_IMAGE_SOURCE = re.compile(r"<img[^>]+src=[\"']([^\"']+)", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")

SENTIMENT_METHOD = "vader_crypto_lexicon_v2"
_SENTIMENT_THRESHOLD = 0.12
_DOMAIN_LEXICON = {
    "adoption": 1.8,
    "approval": 1.8,
    "approved": 1.9,
    "breakout": 2.1,
    "bullish": 2.7,
    "gain": 1.6,
    "gains": 1.7,
    "growth": 1.5,
    "inflow": 1.6,
    "inflows": 1.7,
    "launch": 1.3,
    "partnership": 1.5,
    "rally": 2.4,
    "surge": 2.3,
    "upgrade": 1.4,
    "attack": -2.5,
    "ban": -2.4,
    "bearish": -2.7,
    "bankruptcy": -3.2,
    "breach": -3.0,
    "crash": -3.2,
    "crackdown": -2.7,
    "decline": -1.7,
    "exploit": -3.1,
    "fraud": -3.3,
    "hack": -3.3,
    "hacked": -3.4,
    "lawsuit": -2.0,
    "liquidation": -2.4,
    "liquidations": -2.5,
    "loss": -2.0,
    "losses": -2.1,
    "outflow": -1.6,
    "outflows": -1.7,
    "plunge": -2.8,
    "stalled": -1.9,
    "suspended": -2.4,
}
_PHRASE_ADJUSTMENTS = {
    "all-time high": 0.9,
    "record high": 0.7,
    "etf approval": 0.8,
    "etf inflow": 0.6,
    "institutional adoption": 0.6,
    "regulatory clarity": 0.5,
    "network upgrade": 0.4,
    "bullish breakout": 0.8,
    "breaks resistance": 0.6,
    "security breach": -0.9,
    "rug pull": -1.0,
    "etf outflow": -0.6,
    "regulatory crackdown": -0.7,
    "liquidation cascade": -0.9,
    "bearish breakdown": -0.8,
    "fails support": -0.6,
    "flash crash": -1.0,
    "bankruptcy filing": -0.9,
    "removed as": -0.5,
}
_SENTIMENT_ANALYZER = SentimentIntensityAnalyzer()
_SENTIMENT_ANALYZER.lexicon.update(_DOMAIN_LEXICON)
_BITCOIN_IMPROVEMENT_PROPOSAL = re.compile(
    r"\bbitcoin improvement proposals?\b",
    re.IGNORECASE,
)

_COIN_KEYWORDS = {
    "bitcoin": ("bitcoin", "btc"),
    "ethereum": ("ethereum", "ether", "eth"),
    "binancecoin": ("bnb", "binance coin"),
    "solana": ("solana", "sol"),
    "ripple": ("ripple", "xrp"),
    "tron": ("tron", "trx"),
    "hyperliquid": ("hyperliquid", "hype"),
    "dogecoin": ("dogecoin", "doge"),
    "zcash": ("zcash", "zec"),
    "cardano": ("cardano", "ada"),
}


def _repair_mojibake(value: str) -> str:
    if not any(marker in value for marker in ("â", "Ã", "Â")):
        return value
    try:
        repaired = value.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value
    return repaired if "�" not in repaired else value


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _clean_text(value: str | None, limit: int | None = None) -> str:
    text = _repair_mojibake(unescape(_HTML_TAG.sub(" ", value or "")))
    text = _WHITESPACE.sub(" ", text).strip()
    if limit and len(text) > limit:
        return f"{text[: limit - 1].rstrip()}…"
    return text


def _direct_text(element: ElementTree.Element, names: tuple[str, ...]) -> str:
    for name in names:
        for child in element:
            if _local_name(child.tag) == name and child.text:
                return child.text.strip()
    return ""


def _article_link(element: ElementTree.Element) -> str:
    for child in element:
        if _local_name(child.tag) != "link":
            continue
        candidate = (child.text or child.attrib.get("href") or "").strip()
        if candidate:
            return candidate
    return ""


def _valid_web_url(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    value = unescape(value.strip())
    if urlsplit(value).scheme not in {"http", "https"}:
        return None
    return value


def _article_image(element: ElementTree.Element, description: str) -> str | None:
    for preferred_name in ("content", "thumbnail", "enclosure"):
        for child in element:
            if _local_name(child.tag) != preferred_name:
                continue
            candidate = _valid_web_url(child.attrib.get("url"))
            media_type = child.attrib.get("type", "")
            medium = child.attrib.get("medium", "")
            if candidate and (
                preferred_name != "enclosure"
                or media_type.startswith("image/")
                or medium == "image"
            ):
                return candidate

    match = _IMAGE_SOURCE.search(description)
    return _valid_web_url(match.group(1)) if match else None


def _published_at(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _related_coins(title: str, summary: str) -> list[str]:
    haystack = f"{title} {summary}".lower()
    related = []
    for coin_id, keywords in _COIN_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(keyword)}\b", haystack) for keyword in keywords):
            related.append(coin_id)
    return related


def _canonical_url(value: str) -> str:
    parsed = urlsplit(value)
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def _sentiment_label(score: float) -> str:
    if score > _SENTIMENT_THRESHOLD:
        return "positive"
    if score < -_SENTIMENT_THRESHOLD:
        return "negative"
    return "neutral"


def _term_present(term: str, text: str) -> bool:
    return bool(re.search(rf"(?<!\w){re.escape(term)}(?!\w)", text))


def _percentage_distribution(
    positive: float,
    neutral: float,
    negative: float,
) -> tuple[int, int, int]:
    total = positive + neutral + negative
    if total <= 0:
        return 0, 100, 0
    positive_pct = round(positive / total * 100)
    negative_pct = round(negative / total * 100)
    neutral_pct = max(0, 100 - positive_pct - negative_pct)
    return positive_pct, neutral_pct, negative_pct


def article_sentiment(title: str, summary: str = "") -> dict[str, Any]:
    parts = [(title, 1.0)] if not summary else [(title, 0.72), (summary, 0.28)]
    weighted = {"compound": 0.0, "pos": 0.0, "neu": 0.0, "neg": 0.0}
    for text, weight in parts:
        analysis_text = _BITCOIN_IMPROVEMENT_PROPOSAL.sub("BIP", text)
        scores = _SENTIMENT_ANALYZER.polarity_scores(analysis_text)
        for key in weighted:
            weighted[key] += float(scores[key]) * weight

    haystack = f"{title} {summary}".casefold()
    matched: list[dict[str, str]] = []
    for term, polarity in _DOMAIN_LEXICON.items():
        if _term_present(term, haystack):
            matched.append(
                {"term": term, "polarity": "positive" if polarity > 0 else "negative"}
            )

    phrase_adjustment = 0.0
    for phrase, adjustment in _PHRASE_ADJUSTMENTS.items():
        if not _term_present(phrase, haystack):
            continue
        phrase_adjustment += adjustment
        matched.append(
            {
                "term": phrase,
                "polarity": "positive" if adjustment > 0 else "negative",
            }
        )

    score = weighted["compound"] + max(-0.24, min(0.24, phrase_adjustment * 0.12))
    score = max(-1.0, min(1.0, score))
    positive_pct, neutral_pct, negative_pct = _percentage_distribution(
        weighted["pos"],
        weighted["neu"],
        weighted["neg"],
    )
    polarized_share = weighted["pos"] + weighted["neg"]
    confidence = (
        0.45 * abs(score)
        + 0.35 * min(1.0, polarized_share * 2)
        + 0.20 * min(1.0, len(matched) / 3)
    )
    return {
        "score": round(score, 3),
        "label": _sentiment_label(score),
        "confidence": round(min(1.0, confidence), 3),
        "confidence_pct": round(min(1.0, confidence) * 100),
        "positive_pct": positive_pct,
        "neutral_pct": neutral_pct,
        "negative_pct": negative_pct,
        "matched_terms": matched[:8],
        "method": SENTIMENT_METHOD,
        "language": "en",
    }


def _article_age_hours(article: dict[str, Any], now: datetime) -> float | None:
    value = article.get("published_at")
    if not value:
        return None
    try:
        published = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    return max(0.0, (now - published.astimezone(timezone.utc)).total_seconds() / 3600)


def _trend(selected: list[dict[str, Any]]) -> tuple[str, float]:
    if len(selected) < 4:
        return "stable", 0.0

    ordered = sorted(
        selected,
        key=lambda article: str(article.get("published_at") or ""),
        reverse=True,
    )
    split = (len(ordered) + 1) // 2
    recent = [
        float(row.get("sentiment", {}).get("score", 0.0))
        for row in ordered[:split]
    ]
    previous = [
        float(row.get("sentiment", {}).get("score", 0.0))
        for row in ordered[split:]
    ]
    delta = sum(recent) / len(recent) - sum(previous) / len(previous)
    if delta > 0.08:
        return "improving", delta
    if delta < -0.08:
        return "deteriorating", delta
    return "stable", delta


def aggregate_news_sentiment(
    articles: list[dict[str, Any]],
    coin_id: str | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    relevant = (
        [
            article
            for article in articles
            if coin_id in article.get("related_coin_ids", [])
        ]
        if coin_id
        else []
    )
    selected = relevant or articles
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    observations = []
    ages = []
    for article in selected:
        sentiment = article.get("sentiment", {})
        score = float(sentiment.get("score", 0.0))
        confidence = max(0.0, min(1.0, float(sentiment.get("confidence", 0.0))))
        age_hours = _article_age_hours(article, current_time)
        if age_hours is not None:
            ages.append(age_hours)
        recency_weight = 0.35 if age_hours is None else 0.5 ** (age_hours / 36.0)
        weight = recency_weight * (0.35 + 0.65 * confidence)
        observations.append((score, confidence, weight, sentiment.get("label", "neutral")))

    total_weight = sum(item[2] for item in observations)
    score = (
        sum(item[0] * item[2] for item in observations) / total_weight
        if total_weight
        else 0.0
    )
    dispersion = (
        math.sqrt(
            sum(item[2] * (item[0] - score) ** 2 for item in observations)
            / total_weight
        )
        if total_weight
        else 0.0
    )
    source_count = len({str(row.get("source") or "unknown") for row in selected})
    average_evidence = (
        sum(item[1] * item[2] for item in observations) / total_weight
        if total_weight
        else 0.0
    )
    sample_factor = min(1.0, len(observations) / 8)
    source_factor = min(1.0, source_count / 4)
    agreement = max(0.0, 1.0 - dispersion / 0.65) if observations else 0.0
    specificity = 1.0 if relevant or not coin_id else 0.82
    confidence = specificity * (
        0.35 * average_evidence
        + 0.25 * sample_factor
        + 0.20 * source_factor
        + 0.20 * agreement
    )
    counts = {
        label: sum(1 for item in observations if item[3] == label)
        for label in ("positive", "neutral", "negative")
    }
    positive_pct, neutral_pct, negative_pct = _percentage_distribution(
        counts["positive"], counts["neutral"], counts["negative"]
    )
    trend, trend_delta = _trend(selected)
    return {
        "score": round(score, 3),
        "label": _sentiment_label(score),
        "confidence": round(min(1.0, confidence), 3),
        "confidence_pct": round(min(1.0, confidence) * 100),
        "sample_size": len(observations),
        "source_count": source_count,
        "coin_specific": bool(relevant),
        "scope": "asset" if relevant else "market",
        "positive_pct": positive_pct,
        "neutral_pct": neutral_pct,
        "negative_pct": negative_pct,
        "dispersion": round(dispersion, 3),
        "trend": trend,
        "trend_delta": round(trend_delta, 3),
        "freshness_hours": round(min(ages), 1) if ages else None,
        "role": "context_only",
        "forecast_weight_pct": 0.0,
        "method": SENTIMENT_METHOD,
        "language": "en",
    }


def parse_rss_feed(payload: bytes, source: str) -> list[dict[str, Any]]:
    if not payload or len(payload) > MAX_FEED_BYTES:
        raise ValueError("A hírfolyam mérete érvénytelen")

    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise ValueError("A hírfolyam XML-formátuma hibás") from exc
    entries = [
        element
        for element in root.iter()
        if _local_name(element.tag) in {"item", "entry"}
    ]
    articles = []
    for entry in entries[:30]:
        title = _clean_text(_direct_text(entry, ("title",)), limit=180)
        url = _valid_web_url(_article_link(entry))
        if not title or not url:
            continue

        description_html = _direct_text(entry, ("description", "summary", "content"))
        summary = _clean_text(description_html, limit=240)
        date_value = _direct_text(entry, ("pubdate", "published", "updated", "date"))
        category = _clean_text(_direct_text(entry, ("category",)), limit=80) or None
        guid = _clean_text(_direct_text(entry, ("guid", "id")), limit=220)
        articles.append(
            {
                "id": guid or _canonical_url(url),
                "title": title,
                "url": url,
                "source": source,
                "published_at": _published_at(date_value),
                "image": _article_image(entry, description_html),
                "summary": summary,
                "category": category,
                "related_coin_ids": _related_coins(title, summary),
            }
        )
    return articles


def normalize_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for article in articles:
        source_info = article.get("source_info") or {}
        title = _clean_text(str(article.get("title", "")), limit=180)
        url = _valid_web_url(article.get("url"))
        if not title or not url:
            continue

        published_at = article.get("published_at")
        if not published_at and article.get("published_on"):
            try:
                published_at = datetime.fromtimestamp(
                    int(article["published_on"]), timezone.utc
                ).isoformat()
            except (TypeError, ValueError, OverflowError):
                published_at = None

        summary = _clean_text(
            str(article.get("summary") or article.get("body") or ""),
            limit=240,
        )
        related = article.get("related_coin_ids") or _related_coins(title, summary)
        normalized.append(
            {
                "id": article.get("id") or article.get("guid") or _canonical_url(url),
                "title": title,
                "url": url,
                "source": source_info.get("name") or article.get("source") or "Kriptopiac",
                "published_at": published_at,
                "image": _valid_web_url(article.get("image") or article.get("imageurl")),
                "summary": summary,
                "category": article.get("category") or article.get("categories"),
                "related_coin_ids": list(related),
                "sentiment": article_sentiment(title, summary),
            }
        )
    return normalized


def merge_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    for article in normalize_articles(articles):
        canonical_url = _canonical_url(article["url"])
        normalized_title = article["title"].casefold()
        if canonical_url in seen_urls or normalized_title in seen_titles:
            continue
        seen_urls.add(canonical_url)
        seen_titles.add(normalized_title)
        unique.append(article)

    unique.sort(key=lambda article: article.get("published_at") or "", reverse=True)

    # Keep the first visible row varied without losing overall recency.
    balanced = []
    deferred = []
    source_counts: dict[str, int] = {}
    for article in unique:
        source = article["source"]
        if source_counts.get(source, 0) < 2:
            balanced.append(article)
            source_counts[source] = source_counts.get(source, 0) + 1
        else:
            deferred.append(article)
    return balanced + deferred
