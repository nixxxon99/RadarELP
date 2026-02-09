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


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


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
    )
