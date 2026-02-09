from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_chat_id: int
    tz: str
    daily_hour: int
    daily_minute: int
    max_items_per_run: int
    max_send_per_run: int
    db_path: str
    jobs_scan_enabled: bool
    jobs_scan_interval_hours: int
    hh_areas: list[int]
    yandex_xml_enabled: bool
    yandex_xml_user: str
    yandex_xml_key: str
    yandex_serpapi_enabled: bool
    yandex_serpapi_key: str


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _get_int_list(name: str, default: list[int]) -> list[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    items = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        items.append(int(raw))
    return items or default


def load_settings() -> Settings:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")
    admin_chat_id = _get_int("ADMIN_CHAT_ID", 0)
    if admin_chat_id == 0:
        raise RuntimeError("ADMIN_CHAT_ID is required")

    return Settings(
        bot_token=bot_token,
        admin_chat_id=admin_chat_id,
        tz=os.getenv("TZ", "Asia/Almaty"),
        daily_hour=_get_int("DAILY_HOUR", 9),
        daily_minute=_get_int("DAILY_MINUTE", 0),
        max_items_per_run=_get_int("MAX_ITEMS_PER_RUN", 50),
        max_send_per_run=_get_int("MAX_SEND_PER_RUN", 10),
        db_path=os.getenv("DB_PATH", "radar.db"),
        jobs_scan_enabled=_get_bool("JOBS_SCAN_ENABLED", True),
        jobs_scan_interval_hours=_get_int("JOBS_SCAN_INTERVAL_HOURS", 6),
        hh_areas=_get_int_list("HH_AREAS", [40, 160]),
        yandex_xml_enabled=_get_bool("YANDEX_XML_ENABLED", False),
        yandex_xml_user=os.getenv("YANDEX_XML_USER", "").strip(),
        yandex_xml_key=os.getenv("YANDEX_XML_KEY", "").strip(),
        yandex_serpapi_enabled=_get_bool("YANDEX_SERPAPI_ENABLED", False),
        yandex_serpapi_key=os.getenv("YANDEX_SERPAPI_KEY", "").strip(),
    )
