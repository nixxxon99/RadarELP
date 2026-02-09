from __future__ import annotations

from urllib.parse import quote_plus

SIGNAL_QUERIES_RU = [
    "склад",
    "аренда склада",
    "склад в Алматы",
    "складской комплекс",
    "строительство склада",
    "ввод в эксплуатацию склад",
    "склад класс A",
    "склад класс B",
    "склад класс C",
    "складская недвижимость",
    "складская логистика",
    "складской оператор",
    "складской парк",
    "складской терминал",
    "распределительный центр",
    "распределительный центр открытие",
    "дистрибуционный центр",
    "дистрибуция",
    "РЦ",
    "логистический парк",
    "логистический центр",
    "логистический хаб",
    "индустриальная зона",
    "индустриальный парк",
    "строительство РЦ",
    "фулфилмент",
    "центр обработки заказов",
    "3PL оператор",
    "dark store",
    "cold chain",
    "холодная цепь",
    "кросс-докинг",
    "WMS внедрение",
]

SIGNAL_QUERIES_EN = [
    "warehouse",
    "warehouse leasing",
    "warehouse Almaty",
    "distribution center",
    "distribution centre",
    "logistics hub",
    "logistics park",
    "industrial park logistics",
    "industrial zone",
    "supply chain",
    "supply chain expansion",
    "warehouse construction",
    "warehouse class A",
    "warehouse class B",
    "warehouse class C",
    "fulfillment center",
    "fulfilment center",
    "3PL provider",
    "dark store",
    "cold chain warehouse",
    "cross-dock",
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
