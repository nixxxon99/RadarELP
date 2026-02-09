from __future__ import annotations

import re

STRONG_SIGNAL_KEYWORDS = [
    "warehouse",
    "distribution center",
    "distribution centre",
    "logistics hub",
    "fulfillment center",
    "fulfilment center",
    "supply chain",
    "warehouse jobs",
    "logistics jobs",
    "ваканс",
    "склад",
    "распределительный центр",
    "логистический центр",
    "логистический хаб",
    "фулфилмент",
    "центр обработки заказов",
    "тендер",
    "закупк",
    "инвестиц",
    "расширен",
    "открыт",
    "запуск",
    "строительств",
]

REGION_KEYWORDS = [
    "kazakhstan",
    "алматы",
    "almaty",
    "астана",
    "astana",
    "kazakh",
    "казахстан",
    "центральная азия",
    "central asia",
]

SEGMENT_KEYWORDS = {
    "E-COM": ["e-commerce", "ecommerce", "онлайн", "маркетплейс", "marketplace"],
    "3PL": ["3pl", "logistics provider", "логистический оператор", "фулфилмент"],
    "FMCG": ["fmcg", "food", "retail", "ритейл", "продукт", "товары повседневного спроса"],
    "Distribution": [
        "дистрибуц",
        "дистрибьют",
        "distribution",
        "дистрибуционный центр",
        "распределительный центр",
        "оптовый",
    ],
    "Warehouse Real Estate": [
        "аренда склада",
        "складская недвижимость",
        "складской комплекс",
        "складской парк",
        "логистический парк",
        "индустриальный парк",
        "индустриальная зона",
        "склад класс a",
        "склад класс b",
        "склад класс c",
        "warehousing",
        "warehouse leasing",
    ],
    "Auto/Spec Tech": [
        "автоспецтех",
        "спецтех",
        "складская техника",
        "погрузчик",
        "forklift",
        "reach truck",
        "material handling",
    ],
    "Industrial": ["industrial", "manufacturing", "завод", "production", "промышлен"],
    "Cold Chain": ["cold chain", "холодная цепь", "температурн", "холодильн"],
}

TIMING_RULES = {
    "0–3 мес": ["opens", "opening", "launch", "запуск", "открытие", "открыл"],
    "0–6 мес": ["tender", "тендер", "rfp", "закуп"],
    "3–12 мес": ["construction", "строитель", "planning", "проект"],
    "6–24 мес": ["announce", "announced", "announces", "announced", "invest", "инвест"],
}


def _normalize(text: str) -> str:
    return text.lower() if text else ""


def detect_segment(title: str, summary: str) -> str:
    text = _normalize(f"{title} {summary}")
    for segment, keywords in SEGMENT_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return segment
    return "Other"


def detect_timing(title: str, summary: str) -> str:
    text = _normalize(f"{title} {summary}")
    for timing, keywords in TIMING_RULES.items():
        if any(keyword in text for keyword in keywords):
            return timing
    return "3–12 мес"


def demand_score(title: str, summary: str) -> int:
    text = _normalize(f"{title} {summary}")
    score = 0
    matches = 0
    for keyword in STRONG_SIGNAL_KEYWORDS:
        if keyword in text:
            matches += 1
    if matches:
        score += min(60, matches * 10)

    if any(keyword in text for keyword in REGION_KEYWORDS):
        score += 10

    if "vacanc" in text or "ваканс" in text or "jobs" in text:
        score += 10

    if "tender" in text or "тендер" in text or "закуп" in text:
        score += 10

    if "investment" in text or "инвест" in text:
        score += 10

    if score == 0:
        score = 5
    return min(score, 100)


def guess_company(title: str) -> str:
    if not title:
        return ""
    parts = re.split(r"\s[-|:]\s", title)
    candidate = parts[0].strip()
    candidate = candidate.strip('"“”')
    return candidate


def tenant_match_score(profile: dict, listing: dict) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    price = listing.get("price")
    budget_min = profile.get("budget_min")
    budget_max = profile.get("budget_max")
    if price is not None:
        if budget_min is not None and budget_max is not None:
            if budget_min <= price <= budget_max:
                score += 35
                reasons.append("в бюджете")
            else:
                delta = min(abs(price - budget_min), abs(price - budget_max))
                if delta <= max(1, (budget_max or budget_min) * 0.1):
                    score += 15
                    reasons.append("почти в бюджете")
        elif budget_max is not None and price <= budget_max:
            score += 30
            reasons.append("в пределах бюджета")
        elif budget_min is not None and price >= budget_min:
            score += 20
            reasons.append("выше минимального бюджета")

    district = (profile.get("district") or "").lower()
    listing_district = (listing.get("district") or "").lower()
    if district and listing_district:
        if district == listing_district or district in listing_district:
            score += 25
            reasons.append("нужный район")

    property_type = (profile.get("property_type") or "").lower()
    listing_type = (listing.get("property_type") or "").lower()
    if property_type and listing_type:
        if property_type in listing_type or listing_type in property_type:
            score += 15
            reasons.append("подходящий тип объекта")

    pets_required = (profile.get("pets") or "").lower()
    if pets_required in {"да", "yes"}:
        if listing.get("pets_allowed"):
            score += 10
            reasons.append("можно с животными")
        else:
            score -= 5

    parking_required = (profile.get("parking") or "").lower()
    if parking_required in {"да", "yes"}:
        if listing.get("parking"):
            score += 10
            reasons.append("есть парковка")
        else:
            score -= 5

    if listing.get("verified_at"):
        score += 5
        reasons.append("актуальное объявление")

    return max(score, 0), reasons
