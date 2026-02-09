from __future__ import annotations

from typing import Iterable

import httpx

HH_API_URL = "https://api.hh.ru/vacancies"

HH_QUERIES: list[str] = [
    "warehouse manager",
    "warehouse supervisor",
    "warehouse operations",
    "logistics manager",
    "logistics coordinator",
    "supply chain manager",
    "supply chain analyst",
    "fulfillment manager",
    "fulfillment operations",
    "WMS",
    "inventory manager",
    "distribution center",
]

HH_STRONG_KEYWORDS = [
    "warehouse",
    "логист",
    "logistics",
    "supply chain",
    "склад",
    "fulfillment",
    "fulfilment",
    "wms",
    "distribution center",
    "распределительный центр",
]


def fetch_hh_vacancies(query: str, area: int, pages: int = 1) -> list[dict]:
    items: list[dict] = []
    with httpx.Client(timeout=15.0) as client:
        for page in range(max(pages, 1)):
            response = client.get(
                HH_API_URL,
                params={
                    "text": query,
                    "area": area,
                    "page": page,
                    "per_page": 20,
                    "order_by": "publication_time",
                },
            )
            response.raise_for_status()
            payload = response.json()
            items.extend(payload.get("items", []))
            total_pages = payload.get("pages", 0)
            if page >= total_pages - 1:
                break
    return items


def vacancy_summary(vacancy: dict) -> str:
    snippet = vacancy.get("snippet") or {}
    requirement = snippet.get("requirement") or ""
    responsibility = snippet.get("responsibility") or ""
    parts = [part.strip() for part in (requirement, responsibility) if part]
    return " | ".join(parts)


def vacancy_city(vacancy: dict) -> str:
    area = vacancy.get("area") or {}
    return area.get("name") or ""


def vacancy_company(vacancy: dict) -> str:
    employer = vacancy.get("employer") or {}
    return employer.get("name") or ""


def vacancy_url(vacancy: dict) -> str:
    return vacancy.get("alternate_url") or vacancy.get("html_url") or ""


def build_title(name: str, company: str, city: str) -> str:
    parts = [part for part in (name, company, city) if part]
    return " — ".join(parts)


def strong_signal_bonus(text: str) -> int:
    normalized = text.lower()
    if any(keyword in normalized for keyword in HH_STRONG_KEYWORDS):
        return 40
    return 0


def iter_queries(queries: Iterable[str] | None = None) -> Iterable[str]:
    return queries or HH_QUERIES
