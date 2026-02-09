from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
import logging
from typing import Iterable
from urllib.parse import quote_plus

import httpx

from app.config import Settings

YANDEX_XML_URL = "https://yandex.com/search/xml"
YANDEX_XML_USER_AGENT = "ELP-Radar/1.0 (contact: support@example.com)"

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class YandexXmlResult:
    title: str
    url: str
    snippet: str


def _is_enabled(settings: Settings) -> bool:
    return bool(settings.yandex_xml_enabled and settings.yandex_xml_user and settings.yandex_xml_key)


def _build_url(settings: Settings, query: str) -> str:
    encoded_query = quote_plus(query)
    return (
        f"{YANDEX_XML_URL}?user={settings.yandex_xml_user}"
        f"&key={settings.yandex_xml_key}"
        f"&query={encoded_query}"
        "&l10n=ru"
        "&sortby=tm.order%3Ddescending"
        "&filter=moderate"
        "&maxpassages=2"
    )


def fetch_yandex_xml_results(
    settings: Settings,
    queries: Iterable[str],
    max_results_per_query: int = 5,
    errors: list[str] | None = None,
) -> list[YandexXmlResult]:
    if not _is_enabled(settings):
        return []
    results: list[YandexXmlResult] = []
    with httpx.Client(timeout=20.0, headers={"User-Agent": YANDEX_XML_USER_AGENT}) as client:
        for query in queries:
            response = client.get(_build_url(settings, query))
            if response.status_code != 200:
                continue
            try:
                root = ET.fromstring(response.text)
            except ET.ParseError as exc:
                logger.exception("Yandex XML parse error for query=%s", query)
                if errors is not None:
                    errors.append(f"Yandex XML parse error for query='{query}': {exc}")
                continue
            for doc in root.findall(".//doc")[:max_results_per_query]:
                url = doc.findtext("url") or ""
                title = doc.findtext("title") or ""
                passage_parts = [node.text or "" for node in doc.findall("passage")]
                snippet = " ".join(part.strip() for part in passage_parts if part)
                if url:
                    results.append(YandexXmlResult(title=title, url=url, snippet=snippet))
    return results
