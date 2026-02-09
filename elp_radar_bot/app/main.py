from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import Settings, load_settings
from app.scoring import detect_segment, detect_timing, demand_score, guess_company
from app.sources import get_all_feed_urls
from app.storage import Storage
from app.utils import fetch_rss_items


def escape_markdown(text: str) -> str:
    if not text:
        return ""
    escape_chars = "_*`[]"
    for char in escape_chars:
        text = text.replace(char, f"\\{char}")
    return text


def format_lead(lead: dict) -> str:
    company = escape_markdown(lead.get("company_guess") or "—")
    title = escape_markdown(lead.get("title") or "")
    source = escape_markdown(lead.get("source") or "")
    published = escape_markdown(lead.get("published") or "")
    segment = escape_markdown(lead.get("segment") or "Other")
    timing = escape_markdown(lead.get("timing") or "")
    score = lead.get("demand_score") or 0
    url = lead.get("url") or ""

    return (
        f"*Компания:* {company}\n"
        f"*Demand Score:* {score}\n"
        f"*Сегмент:* {segment}\n"
        f"*Тайминг:* {timing}\n"
        f"*Дата/источник:* {published} | {source}\n"
        f"*Сигнал:* {title}\n"
        f"[Ссылка]({url})"
    )


async def run_radar_once(bot: Bot, storage: Storage, settings: Settings) -> dict:
    feeds = get_all_feed_urls()
    collected = 0
    sent = 0
    new_leads = 0

    for feed_url in feeds:
        items = await asyncio.to_thread(fetch_rss_items, feed_url)
        for item in items:
            if collected >= settings.max_items_per_run:
                break
            collected += 1
            url = item.get("url")
            if not url or storage.is_seen(url):
                continue

            title = item.get("title", "")
            summary = item.get("summary", "")
            lead = {
                **item,
                "demand_score": demand_score(title, summary),
                "segment": detect_segment(title, summary),
                "timing": detect_timing(title, summary),
                "company_guess": guess_company(title),
            }

            saved = storage.save_lead(lead)
            storage.mark_seen(url)
            if saved:
                new_leads += 1
                if lead["demand_score"] >= 60 and sent < settings.max_send_per_run:
                    await bot.send_message(
                        chat_id=settings.admin_chat_id,
                        text=format_lead(lead),
                        parse_mode="Markdown",
                        disable_web_page_preview=True,
                    )
                    sent += 1
        if collected >= settings.max_items_per_run:
            break

    return {"collected": collected, "new_leads": new_leads, "sent": sent}


def build_dispatcher(storage: Storage, settings: Settings) -> Dispatcher:
    dispatcher = Dispatcher()

    @dispatcher.message(Command("start"))
    async def handle_start(message: Message) -> None:
        await message.answer(
            "ELP Market Radar готов. Доступные команды: /scan_now /radar /hot"
        )

    @dispatcher.message(Command("scan_now"))
    async def handle_scan_now(message: Message, bot: Bot) -> None:
        result = await run_radar_once(bot, storage, settings)
        await message.answer(
            "Сканирование завершено. "
            f"Собрано: {result['collected']}, "
            f"Новые лиды: {result['new_leads']}, "
            f"Отправлено hot: {result['sent']}"
        )

    @dispatcher.message(Command("radar"))
    async def handle_radar(message: Message) -> None:
        rows = storage.top_latest(limit=10)
        if not rows:
            await message.answer("Пока нет лидов.")
            return
        for row in rows:
            await message.answer(
                format_lead(dict(row)),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )

    @dispatcher.message(Command("hot"))
    async def handle_hot(message: Message) -> None:
        rows = storage.top_latest(limit=10, min_score=60)
        if not rows:
            await message.answer("Пока нет hot лидов.")
            return
        for row in rows:
            await message.answer(
                format_lead(dict(row)),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )

    return dispatcher


def schedule_jobs(scheduler: AsyncIOScheduler, bot: Bot, storage: Storage, settings: Settings) -> None:
    trigger = CronTrigger(
        hour=settings.daily_hour,
        minute=settings.daily_minute,
        timezone=ZoneInfo(settings.tz),
    )

    scheduler.add_job(run_radar_once, trigger=trigger, args=[bot, storage, settings])


def main() -> None:
    settings = load_settings()
    storage = Storage(settings.db_path)
    bot = Bot(token=settings.bot_token)
    dispatcher = build_dispatcher(storage, settings)

    scheduler = AsyncIOScheduler(timezone=ZoneInfo(settings.tz))
    schedule_jobs(scheduler, bot, storage, settings)
    scheduler.start()

    dispatcher.run_polling(bot)


if __name__ == "__main__":
    main()
