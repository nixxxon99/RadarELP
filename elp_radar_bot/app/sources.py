from __future__ import annotations

from urllib.parse import quote_plus

SIGNAL_QUERIES_RU = [
    "складской комплекс Казахстан",
    "логистический центр Алматы",
    "распределительный центр открытие",
    "логистический хаб инвестиции",
    "строительство склада",
    "тендер складские услуги",
    "3PL оператор Казахстан",
    "вакансии склад логистика",
    "supply chain вакансии Алматы",
    "холодная цепь склад",
    "фулфилмент центр",
    "e-commerce логистика Казахстан",
]

SIGNAL_QUERIES_EN = [
    "warehouse expansion Kazakhstan",
    "distribution center Kazakhstan",
    "logistics hub Almaty",
    "supply chain jobs Kazakhstan",
    "3PL provider Kazakhstan",
    "cold chain warehouse",
    "fulfillment center launch",
    "logistics tender Kazakhstan",
    "industrial park logistics",
]

OPTIONAL_FEEDS = [
    # Add public RSS feeds here when needed.
]


def build_google_news_rss(query: str, hl: str, gl: str, ceid: str) -> str:
    encoded_query = quote_plus(query)
    return (
        "https://news.google.com/rss/search?q="
        f"{encoded_query}&hl={hl}&gl={gl}&ceid={ceid}"
    )


def get_all_feed_urls() -> list[str]:
    feeds: list[str] = []
    for query in SIGNAL_QUERIES_RU:
        feeds.append(build_google_news_rss(query, hl="ru", gl="KZ", ceid="KZ:ru"))
    for query in SIGNAL_QUERIES_EN:
        feeds.append(build_google_news_rss(query, hl="en", gl="KZ", ceid="KZ:en"))
    feeds.extend(OPTIONAL_FEEDS)
    return feeds
