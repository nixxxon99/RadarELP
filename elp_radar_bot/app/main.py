from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import Settings, load_settings
from app.hh_jobs import (
    build_title,
    fetch_hh_vacancies,
    iter_queries,
    strong_signal_bonus,
    vacancy_city,
    vacancy_company,
    vacancy_summary,
    vacancy_url,
)
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


def build_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔥 Лиды"), KeyboardButton(text="📡 Радар")],
            [KeyboardButton(text="⏱ Период"), KeyboardButton(text="🔎 Скан сейчас")],
            [KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def build_period_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="24 часа", callback_data="period:24h"),
                InlineKeyboardButton(text="3 дня", callback_data="period:3d"),
            ],
            [
                InlineKeyboardButton(text="7 дней", callback_data="period:7d"),
                InlineKeyboardButton(text="30 дней", callback_data="period:30d"),
            ],
        ]
    )


def describe_period(hours: int) -> str:
    mapping = {24: "24 часа", 72: "3 дня", 168: "7 дней", 720: "30 дней"}
    return mapping.get(hours, f"{hours} ч")


async def run_radar_once(bot: Bot, storage: Storage, settings: Settings) -> dict:
    feeds = get_all_feed_urls()
    collected = 0
    sent = 0
    new_leads = 0
    now = datetime.utcnow()

    async def scan_hh_jobs(remaining: int) -> dict:
        hh_collected = 0
        hh_new = 0
        hh_sent = 0
        if not settings.jobs_scan_enabled:
            return {"collected": 0, "new": 0, "sent": 0}

        last_scan = storage.get_last_scan("hh_jobs")
        if last_scan:
            delta_hours = (now - last_scan).total_seconds() / 3600
            if delta_hours < settings.jobs_scan_interval_hours:
                return {"collected": 0, "new": 0, "sent": 0}

        for area in settings.hh_areas:
            for query in iter_queries():
                if hh_collected >= remaining:
                    break
                vacancies = await asyncio.to_thread(fetch_hh_vacancies, query, area, 1)
                for vacancy in vacancies:
                    if hh_collected >= remaining:
                        break
                    hh_collected += 1
                    url = vacancy_url(vacancy)
                    if not url or storage.is_seen(url):
                        continue
                    name = vacancy.get("name") or ""
                    company = vacancy_company(vacancy)
                    city = vacancy_city(vacancy)
                    title = build_title(name, company, city)
                    summary = vacancy_summary(vacancy)
                    published = vacancy.get("published_at") or ""
                    score = demand_score(title, summary)
                    score = min(100, score + strong_signal_bonus(f"{title} {summary}"))
                    lead = {
                        "title": title,
                        "url": url,
                        "published": published,
                        "source": "HeadHunter",
                        "summary": summary,
                        "demand_score": score,
                        "segment": detect_segment(title, summary),
                        "timing": "0–3 мес",
                        "company_guess": company or guess_company(title),
                    }
                    saved = storage.save_lead(lead)
                    storage.mark_seen(url)
                    if saved:
                        hh_new += 1
                        if lead["demand_score"] >= 60 and sent + hh_sent < settings.max_send_per_run:
                            await bot.send_message(
                                chat_id=settings.admin_chat_id,
                                text=format_lead(lead),
                                parse_mode="Markdown",
                                disable_web_page_preview=True,
                            )
                            hh_sent += 1
                if hh_collected >= remaining:
                    break
            if hh_collected >= remaining:
                break

        storage.set_last_scan("hh_jobs", now)
        return {"collected": hh_collected, "new": hh_new, "sent": hh_sent}

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

    remaining = settings.max_items_per_run - collected
    if remaining > 0:
        hh_result = await scan_hh_jobs(remaining)
        collected += hh_result["collected"]
        new_leads += hh_result["new"]
        sent += hh_result["sent"]

    return {"collected": collected, "new_leads": new_leads, "sent": sent}


def build_dispatcher(storage: Storage, settings: Settings) -> Dispatcher:
    dispatcher = Dispatcher()

    @dispatcher.message(Command("start"))
    async def handle_start(message: Message) -> None:
        await message.answer(
            "ELP Market Radar готов. Доступные команды: /scan_now /radar /hot",
            reply_markup=build_main_keyboard(),
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
        period_hours = storage.get_period_hours(message.chat.id)
        rows = storage.leads_since(hours=period_hours, limit=10)
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
        period_hours = storage.get_period_hours(message.chat.id)
        rows = storage.leads_since(hours=period_hours, min_score=60, limit=10)
        if not rows:
            await message.answer("Пока нет hot лидов.")
            return
        for row in rows:
            await message.answer(
                format_lead(dict(row)),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )

    @dispatcher.message(F.text == "🔥 Лиды")
    async def handle_hot_button(message: Message) -> None:
        await handle_hot(message)

    @dispatcher.message(F.text == "📡 Радар")
    async def handle_radar_button(message: Message) -> None:
        await handle_radar(message)

    @dispatcher.message(F.text == "🔎 Скан сейчас")
    async def handle_scan_now_button(message: Message, bot: Bot) -> None:
        await handle_scan_now(message, bot)

    @dispatcher.message(F.text == "⏱ Период")
    async def handle_period_button(message: Message) -> None:
        await message.answer(
            "Выбери период поиска сигналов:",
            reply_markup=build_period_keyboard(),
        )

    @dispatcher.message(F.text == "⚙️ Настройки")
    async def handle_settings_button(message: Message) -> None:
        period_hours = storage.get_period_hours(message.chat.id)
        await message.answer(
            "Настройки:\n"
            f"Период: {describe_period(period_hours)}\n"
            "Hot: >=60\n"
            f"Лимит отправки: {settings.max_send_per_run}"
        )

    @dispatcher.callback_query(F.data.startswith("period:"))
    async def handle_period_select(callback: CallbackQuery) -> None:
        data = callback.data or ""
        value = data.split("period:", maxsplit=1)[-1]
        mapping = {"24h": 24, "3d": 72, "7d": 168, "30d": 720}
        hours = mapping.get(value, 168)
        chat_id = callback.message.chat.id if callback.message else settings.admin_chat_id
        storage.set_period_hours(chat_id, hours)
        await callback.answer("Период обновлен")
        if callback.message:
            await callback.message.answer(f"Ок. Период: {describe_period(hours)}")

    return dispatcher


def schedule_jobs(scheduler: AsyncIOScheduler, bot: Bot, storage: Storage, settings: Settings) -> None:
    trigger = CronTrigger(
        hour=settings.daily_hour,
        minute=settings.daily_minute,
        timezone=ZoneInfo(settings.tz),
    )

    scheduler.add_job(run_radar_once, trigger=trigger, args=[bot, storage, settings])


async def main_async() -> None:
    settings = load_settings()
    storage = Storage(settings.db_path)
    bot = Bot(token=settings.bot_token)
    dispatcher = build_dispatcher(storage, settings)

    scheduler = AsyncIOScheduler(
        timezone=ZoneInfo(settings.tz),
        event_loop=asyncio.get_running_loop(),
    )
    schedule_jobs(scheduler, bot, storage, settings)
    scheduler.start()

    await dispatcher.start_polling(bot)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
