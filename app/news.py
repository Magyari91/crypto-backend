from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import xml.etree.ElementTree as ElementTree


MAX_FEED_BYTES = 2_000_000
_HTML_TAG = re.compile(r"<[^>]+>")
_IMAGE_SOURCE = re.compile(r"<img[^>]+src=[\"']([^\"']+)", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")

_POSITIVE_TERMS = (
    "adoption",
    "approval",
    "approved",
    "bullish",
    "gain",
    "growth",
    "inflow",
    "launch",
    "partnership",
    "rally",
    "record high",
    "surge",
    "upgrade",
)
_NEGATIVE_TERMS = (
    "attack",
    "ban",
    "bearish",
    "crash",
    "decline",
    "exploit",
    "fraud",
    "hack",
    "lawsuit",
    "liquidation",
    "loss",
    "outflow",
    "plunge",
)

_COIN_KEYWORDS = {
    "bitcoin": ("bitcoin", "btc"),
    "ethereum": ("ethereum", "ether", "eth"),
    "solana": ("solana", "sol"),
    "ripple": ("ripple", "xrp"),
    "dogecoin": ("dogecoin", "doge"),
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


def article_sentiment(title: str, summary: str = "") -> dict[str, Any]:
    haystack = f"{title} {title} {summary}".casefold()
    positive = sum(haystack.count(term) for term in _POSITIVE_TERMS)
    negative = sum(haystack.count(term) for term in _NEGATIVE_TERMS)
    observations = positive + negative
    score = (positive - negative) / observations if observations else 0.0
    if score > 0.15:
        label = "positive"
    elif score < -0.15:
        label = "negative"
    else:
        label = "neutral"
    return {
        "score": round(max(-1.0, min(1.0, score)), 3),
        "label": label,
        "method": "headline_lexicon_v1",
    }


def aggregate_news_sentiment(
    articles: list[dict[str, Any]],
    coin_id: str | None = None,
) -> dict[str, Any]:
    relevant = [
        article
        for article in articles
        if not coin_id or coin_id in article.get("related_coin_ids", [])
    ]
    selected = relevant or articles
    scores = [
        float(article.get("sentiment", {}).get("score", 0.0))
        for article in selected
    ]
    score = sum(scores) / len(scores) if scores else 0.0
    label = "positive" if score > 0.15 else "negative" if score < -0.15 else "neutral"
    return {
        "score": round(score, 3),
        "label": label,
        "sample_size": len(scores),
        "coin_specific": bool(relevant),
        "method": "headline_lexicon_v1",
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
