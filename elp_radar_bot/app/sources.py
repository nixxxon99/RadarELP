from __future__ import annotations

from urllib.parse import quote_plus

SIGNAL_QUERIES_RU = [
    "аренда склада Казахстан",
    "склад в Алматы",
    "склад в Астане",
    "складская недвижимость Казахстан",
    "складской комплекс Казахстан",
    "строительство склада Казахстан",
    "ввод в эксплуатацию склад Казахстан",
    "склад класс A Казахстан",
    "склад класс B Казахстан",
    "склад класс C Казахстан",
    "складской парк Казахстан",
    "складской терминал Казахстан",
    "распределительный центр Казахстан",
    "дистрибуционный центр Казахстан",
    "дистрибуция Казахстан",
    "логистический парк Казахстан",
    "логистический центр Казахстан",
    "логистический хаб Казахстан",
    "индустриальная зона Казахстан",
    "индустриальный парк Казахстан",
    "строительство РЦ Казахстан",
    "фулфилмент Казахстан",
    "центр обработки заказов Казахстан",
    "3PL оператор Казахстан",
    "dark store Казахстан",
    "cold chain Казахстан",
    "холодная цепь Казахстан",
    "кросс-докинг Казахстан",
    "WMS внедрение Казахстан",
    "дистрибьютор FMCG Казахстан",
    "оптовый дистрибьютор Казахстан",
    "e-commerce склад Казахстан",
    "складская техника Казахстан",
    "погрузчики склад Казахстан",
]

SIGNAL_QUERIES_EN = [
    "warehouse leasing Kazakhstan",
    "warehouse Almaty",
    "warehouse Astana",
    "distribution center Kazakhstan",
    "distribution centre Kazakhstan",
    "logistics hub Kazakhstan",
    "logistics park Kazakhstan",
    "industrial park logistics Kazakhstan",
    "industrial zone Kazakhstan",
    "supply chain Kazakhstan",
    "supply chain expansion Kazakhstan",
    "warehouse construction Kazakhstan",
    "warehouse class A Kazakhstan",
    "warehouse class B Kazakhstan",
    "warehouse class C Kazakhstan",
    "fulfillment center Kazakhstan",
    "fulfilment center Kazakhstan",
    "3PL provider Kazakhstan",
    "dark store Kazakhstan",
    "cold chain warehouse Kazakhstan",
    "cross-dock Kazakhstan",
    "FMCG distributor Kazakhstan",
    "e-commerce warehouse Kazakhstan",
    "material handling equipment Kazakhstan",
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
        feeds.append(build_google_news_rss(query, hl="ru", gl="RU", ceid="RU:ru"))
    for query in SIGNAL_QUERIES_EN:
        feeds.append(build_google_news_rss(query, hl="en", gl="RU", ceid="RU:en"))
    feeds.extend(OPTIONAL_FEEDS)
    return feeds
