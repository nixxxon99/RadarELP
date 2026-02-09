from __future__ import annotations

import logging
from typing import Iterable
import feedparser
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def clean_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "lxml")
    return soup.get_text(" ", strip=True)


def fetch_rss_items(feed_url: str) -> list[dict]:
    feed = feedparser.parse(feed_url)
    source_title = feed.feed.get("title", "")
    if not feed.entries:
        status = getattr(feed, "status", None)
        bozo_exception = getattr(feed, "bozo_exception", None)
        logger.warning(
            "RSS feed returned 0 entries. url=%s status=%s bozo_exception=%s",
            feed_url,
            status,
            bozo_exception,
        )
    items: list[dict] = []
    for entry in feed.entries:
        url = entry.get("link")
        if not url:
            continue
        title = entry.get("title", "")
        published = entry.get("published") or entry.get("updated") or ""
        summary = clean_html(entry.get("summary", ""))
        items.append(
            {
                "title": title,
                "url": url,
                "published": published,
                "source": source_title,
                "summary": summary,
            }
        )
    return items


def chunked(items: Iterable, size: int) -> list[list]:
    batch: list = []
    batches: list[list] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    return batches
