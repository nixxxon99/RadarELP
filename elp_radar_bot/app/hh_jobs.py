from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Iterable

import httpx

HH_AREA_URL = "https://api.hh.ru/areas"
HH_API_URL = "https://api.hh.ru/vacancies"
HH_VACANCY_URL = "https://api.hh.ru/vacancies/{vacancy_id}"
HH_USER_AGENT = "ELP-Radar/1.0 (contact: support@example.com)"

HH_QUERIES: list[str] = [
    "руководитель склада",
    "начальник склада",
    "менеджер склада",
    "директор по логистике",
    "руководитель РЦ",
    "распределительный центр",
    "дистрибуционный центр",
    "складская логистика",
    "транспортная логистика",
    "управление запасами",
    "планирование поставок",
    "специалист по цепям поставок",
    "supply chain",
    "WMS",
    "фулфилмент",
    "кросс-докинг",
    "комплектация заказов",
    "начальник смены склад",
    "операционный менеджер склад",
    "руководитель 3PL",
    "warehouse manager",
    "head of logistics",
    "supply chain manager",
    "distribution center",
    "fulfillment manager",
    "inventory manager",
    "cross-dock",
    "operations manager logistics",
    "transport manager",
]

HH_STRONG_KEYWORDS = [
    "warehouse",
    "inventory",
    "логист",
    "logistics",
    "supply chain",
    "склад",
    "fulfillment",
    "fulfilment",
    "wms",
    "distribution center",
    "распределительный центр",
    "дистрибуционный центр",
    "кросс-док",
    "cross-dock",
    "cold",
    "pick-pack",
    "пик-энд-пак",
]

@dataclass(frozen=True)
class HhSearchResult:
    items: list[dict]
    found: int
    status_code: int


def _client() -> httpx.Client:
    return httpx.Client(timeout=15.0, headers={"User-Agent": HH_USER_AGENT})


def fetch_hh_areas() -> list[dict]:
    with _client() as client:
        response = client.get(HH_AREA_URL)
        response.raise_for_status()
        return response.json()


def _find_area(areas: Iterable[dict], predicate) -> dict | None:
    for area in areas:
        if predicate(area):
            return area
        children = area.get("areas") or []
        found = _find_area(children, predicate)
        if found:
            return found
    return None


@lru_cache(maxsize=1)
def get_hh_area_ids() -> dict:
    tree = fetch_hh_areas()
    kazakhstan = _find_area(tree, lambda area: area.get("name", "").lower() == "казахстан")
    if not kazakhstan:
        return {"kz": None, "almaty": []}
    kz_children = kazakhstan.get("areas") or []
    almaty = _find_area(kz_children, lambda area: area.get("name", "").lower() == "алматы")
    almaty_region = _find_area(
        kz_children, lambda area: area.get("name", "").lower() == "алматинская область"
    )
    almaty_ids = [area.get("id") for area in (almaty, almaty_region) if area and area.get("id")]
    return {"kz": kazakhstan.get("id"), "almaty": almaty_ids}


def fetch_hh_vacancies(query: str, area: int, pages: int = 5) -> HhSearchResult:
    items: list[dict] = []
    found_total = 0
    status_code = 0
    date_from = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
    with _client() as client:
        for page in range(max(pages, 1)):
            response = client.get(
                HH_API_URL,
                params={
                    "text": query,
                    "area": area,
                    "page": page,
                    "per_page": 50,
                    "order_by": "publication_time",
                    "search_field": "name,company_name,description",
                    "date_from": date_from,
                },
            )
            status_code = response.status_code
            response.raise_for_status()
            payload = response.json()
            found_total = payload.get("found", found_total)
            items.extend(payload.get("items", []))
            total_pages = payload.get("pages", 0)
            if page >= total_pages - 1:
                break
    return HhSearchResult(items=items, found=found_total, status_code=status_code)


def fetch_hh_vacancy_detail(vacancy_id: str) -> dict | None:
    if not vacancy_id:
        return None
    with _client() as client:
        response = client.get(HH_VACANCY_URL.format(vacancy_id=vacancy_id))
        if response.status_code != 200:
            return None
        return response.json()


def vacancy_summary(vacancy: dict, detail: dict | None = None) -> str:
    snippet = vacancy.get("snippet") or {}
    requirement = snippet.get("requirement") or ""
    responsibility = snippet.get("responsibility") or ""
    skills = ""
    if detail:
        key_skills = detail.get("key_skills") or []
        skills = ", ".join(skill.get("name") for skill in key_skills if skill.get("name"))
    parts = [part.strip() for part in (requirement, responsibility, skills) if part]
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
    bonus = 0
    if any(keyword in normalized for keyword in HH_STRONG_KEYWORDS):
        bonus += 20
    if "wms" in normalized:
        bonus += 10
    if "cross-dock" in normalized or "кросс-док" in normalized:
        bonus += 10
    if "fulfillment" in normalized or "фулфилмент" in normalized:
        bonus += 10
    if "inventory" in normalized or "запас" in normalized:
        bonus += 10
    if "cold" in normalized or "холод" in normalized:
        bonus += 10
    if "pick-pack" in normalized or "пик-энд-пак" in normalized:
        bonus += 10
    return bonus


def iter_queries(queries: Iterable[str] | None = None) -> Iterable[str]:
    return queries or HH_QUERIES
