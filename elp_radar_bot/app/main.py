from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import logging
import os
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
import httpx

from app.config import Settings, load_settings
from app.hh_jobs import (
    build_title,
    fetch_hh_areas,
    fetch_hh_vacancies,
    fetch_hh_vacancy_detail,
    get_hh_area_ids,
    iter_queries,
    strong_signal_bonus,
    vacancy_city,
    vacancy_company,
    vacancy_summary,
    vacancy_url,
    HH_API_URL,
    HH_USER_AGENT,
)
from app.scoring import detect_segment, detect_timing, demand_score, guess_company
from app.sources import SIGNAL_QUERIES_RU, get_all_feed_urls
from app.storage import Storage
from app.utils import fetch_rss_items
from app.yandex_serpapi import fetch_yandex_serpapi_results
from app.yandex_xml import fetch_yandex_xml_results

logger = logging.getLogger(__name__)


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
            [KeyboardButton(text="🧲 Лиды HH"), KeyboardButton(text="⏱ Период")],
            [KeyboardButton(text="🔎 Скан сейчас"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def build_period_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="24 часа", callback_data="period:24"),
                InlineKeyboardButton(text="3 дня", callback_data="period:72"),
            ],
            [
                InlineKeyboardButton(text="7 дней", callback_data="period:168"),
                InlineKeyboardButton(text="30 дней", callback_data="period:720"),
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

        try:
            area_ids = get_hh_area_ids()
        except Exception:
            logger.exception("Failed to load HH area ids")
            area_ids = {"kz": None, "almaty": []}
        almaty_ids = area_ids.get("almaty") or []
        kz_id = area_ids.get("kz")
        area_candidates = almaty_ids or settings.hh_areas
        for query in iter_queries():
            if hh_collected >= remaining:
                break
            vacancies = []
            found_total = 0
            for area in area_candidates:
                try:
                    result = await asyncio.to_thread(fetch_hh_vacancies, query, area, 5)
                except Exception:
                    logger.exception("HH vacancies fetch failed for query=%s area=%s", query, area)
                    continue
                vacancies = result.items
                found_total = result.found
                if found_total > 0:
                    break
            if found_total == 0 and kz_id:
                try:
                    result = await asyncio.to_thread(fetch_hh_vacancies, query, kz_id, 5)
                except Exception:
                    logger.exception("HH vacancies fallback failed for query=%s area=%s", query, kz_id)
                    result = None
                if result:
                    vacancies = result.items

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
                try:
                    detail = await asyncio.to_thread(
                        fetch_hh_vacancy_detail, vacancy.get("id", "")
                    )
                except Exception:
                    logger.exception("HH vacancy detail failed for id=%s", vacancy.get("id", ""))
                    detail = None
                summary = vacancy_summary(vacancy, detail)
                published = vacancy.get("published_at") or ""
                base_score = max(70, demand_score(title, summary))
                score = min(100, base_score + strong_signal_bonus(f"{title} {summary}"))
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

        storage.set_last_scan("hh_jobs", now)
        return {"collected": hh_collected, "new": hh_new, "sent": hh_sent}

    async def scan_yandex_xml(remaining: int) -> dict:
        if remaining <= 0:
            return {"collected": 0, "new": 0}
        yandex_collected = 0
        yandex_new = 0
        results = await asyncio.to_thread(fetch_yandex_xml_results, settings, SIGNAL_QUERIES_RU)
        for result in results:
            if yandex_collected >= remaining:
                break
            yandex_collected += 1
            if storage.is_seen(result.url):
                continue
            title = result.title or ""
            summary = result.snippet or ""
            lead = {
                "title": title,
                "url": result.url,
                "published": "",
                "source": "YandexXML",
                "summary": summary,
                "demand_score": demand_score(title, summary),
                "segment": detect_segment(title, summary),
                "timing": detect_timing(title, summary),
                "company_guess": guess_company(title),
            }
            saved = storage.save_lead(lead)
            storage.mark_seen(result.url)
            if saved:
                yandex_new += 1
        return {"collected": yandex_collected, "new": yandex_new}

    async def scan_yandex_serpapi(remaining: int) -> dict:
        if remaining <= 0:
            return {"collected": 0, "new": 0}
        yandex_collected = 0
        yandex_new = 0
        results = await asyncio.to_thread(
            fetch_yandex_serpapi_results, settings, SIGNAL_QUERIES_RU
        )
        for result in results:
            if yandex_collected >= remaining:
                break
            yandex_collected += 1
            url = result.get("url") or ""
            if not url or storage.is_seen(url):
                continue
            title = result.get("title") or ""
            summary = result.get("snippet") or ""
            lead = {
                "title": title,
                "url": url,
                "published": result.get("published") or "",
                "source": "YandexSerpAPI",
                "summary": summary,
                "demand_score": demand_score(title, summary),
                "segment": detect_segment(title, summary),
                "timing": detect_timing(title, summary),
                "company_guess": guess_company(title),
            }
            saved = storage.save_lead(lead)
            storage.mark_seen(url)
            if saved:
                yandex_new += 1
        return {"collected": yandex_collected, "new": yandex_new}

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
        if settings.yandex_xml_enabled:
            yandex_result = await scan_yandex_xml(remaining)
            collected += yandex_result["collected"]
            new_leads += yandex_result["new"]
        elif settings.yandex_serpapi_enabled:
            yandex_result = await scan_yandex_serpapi(remaining)
            collected += yandex_result["collected"]
            new_leads += yandex_result["new"]

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
            "ELP Market Radar готов. Доступные команды: /scan_now /radar /hot /hh_test",
            reply_markup=build_main_keyboard(),
        )

    @dispatcher.message(Command("scan_now"))
    async def handle_scan_now(message: Message, bot: Bot) -> None:
        result = await run_radar_once(bot, storage, settings)
        await message.answer(
            "Сканирование завершено. "
            f"Собрано: {result['collected']}, "
            f"Новые лиды: {result['new_leads']}, "
            f"Отправлено hot: {result['sent']}",
            reply_markup=build_main_keyboard(),
        )

    @dispatcher.message(Command("radar"))
    async def handle_radar(message: Message) -> None:
        period_hours = storage.get_period_hours(message.chat.id)
        rows = storage.leads_since(message.chat.id, hours=period_hours, limit=10)
        if not rows:
            await message.answer("Пока нет лидов.", reply_markup=build_main_keyboard())
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
        rows = storage.leads_since(message.chat.id, hours=period_hours, min_score=60, limit=10)
        if not rows:
            await message.answer("Пока нет hot лидов.", reply_markup=build_main_keyboard())
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

    @dispatcher.message(F.text == "🧲 Лиды HH")
    async def handle_hh_leads_button(message: Message) -> None:
        period_hours = storage.get_period_hours(message.chat.id)
        rows = storage.leads_since(
            message.chat.id,
            hours=period_hours,
            min_score=60,
            source="HeadHunter",
            limit=10,
        )
        if not rows:
            await message.answer("Пока нет hot лидов HH.", reply_markup=build_main_keyboard())
            return
        for row in rows:
            await message.answer(
                format_lead(dict(row)),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )

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
            f"Лимит отправки: {settings.max_send_per_run}",
            reply_markup=build_main_keyboard(),
        )

    @dispatcher.callback_query(F.data.startswith("period:"))
    async def handle_period_select(callback: CallbackQuery) -> None:
        data = callback.data or ""
        value = data.split("period:", maxsplit=1)[-1]
        hours = int(value) if value.isdigit() else 168
        chat_id = callback.message.chat.id if callback.message else settings.admin_chat_id
        storage.set_period_hours(chat_id, hours)
        await callback.answer("Период обновлен")
        if callback.message:
            await callback.message.answer(f"Ок. Период: {describe_period(hours)}")

    @dispatcher.message(Command("hh_test"))
    async def handle_hh_test(message: Message) -> None:
        if message.chat.id != settings.admin_chat_id:
            await message.answer("Команда доступна только администратору.")
            return
        def find_area_id(areas: list[dict], names: set[str]) -> str | None:
            queue = list(areas)
            while queue:
                area = queue.pop(0)
                name = (area.get("name") or "").strip().lower()
                if name in names:
                    return area.get("id")
                queue.extend(area.get("areas") or [])
            return None

        try:
            area_tree = await asyncio.to_thread(fetch_hh_areas)
        except Exception as exc:
            await message.answer(f"HH error: {exc}")
            return

        kz_id = find_area_id(area_tree, {"kazakhstan", "казахстан"})
        almaty_id = find_area_id(area_tree, {"almaty", "алматы"})
        lines = [
            "HH debug:",
            f"Kazakhstan ID: {kz_id or 'не найдено'}",
            f"Almaty ID: {almaty_id or 'не найдено'}",
        ]
        test_queries = ["warehouse manager", "руководитель склада", "supply chain"]
        headers = {"User-Agent": HH_USER_AGENT}
        date_from = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
        for query in test_queries:
            area_to_use = almaty_id or kz_id
            if not area_to_use:
                lines.append(f"- {query}: area not found")
                continue
            status_code = None
            found_total = 0
            links: list[str] = []
            error_text = None

            def run_pages(area_id: str) -> bool:
                nonlocal status_code, found_total, links, error_text
                with httpx.Client(timeout=15.0, headers=headers) as client:
                    for page in range(3):
                        response = client.get(
                            HH_API_URL,
                            params={
                                "text": query,
                                "area": area_id,
                                "page": page,
                                "per_page": 50,
                                "order_by": "publication_time",
                                "search_field": "name,company_name,description",
                                "date_from": date_from,
                            },
                        )
                        status_code = response.status_code
                        if status_code != 200:
                            error_text = response.text
                            return False
                        payload = response.json()
                        found_total = payload.get("found", found_total)
                        for item in payload.get("items", []):
                            url = vacancy_url(item)
                            if url and len(links) < 2:
                                links.append(url)
                        if len(links) >= 2:
                            continue
                return True

            try:
                ok = await asyncio.to_thread(run_pages, str(area_to_use))
            except httpx.HTTPError as exc:
                lines.append(f"- {query}: ошибка {exc}")
                continue

            if ok and found_total == 0 and kz_id and area_to_use != kz_id:
                try:
                    ok = await asyncio.to_thread(run_pages, str(kz_id))
                    area_to_use = kz_id
                except httpx.HTTPError as exc:
                    lines.append(f"- {query}: ошибка {exc}")
                    continue

            if not ok or status_code in {403, 429}:
                lines.append(f"- {query}: status={status_code}, ошибка={error_text}")
                continue
            link_text = ", ".join(links) if links else "нет ссылок"
            lines.append(
                f"- {query}: area={area_to_use}, status={status_code}, found={found_total}, links={link_text}"
            )
        await message.answer("\n".join(lines), reply_markup=build_main_keyboard())

    @dispatcher.message(Command("debug_status"))
    async def handle_debug_status(message: Message) -> None:
        if message.chat.id != settings.admin_chat_id:
            await message.answer("Команда доступна только администратору.")
            return
        period_hours = storage.get_period_hours(message.chat.id)
        total_leads = storage.count_leads()
        leads_period = storage.count_leads_since(period_hours)
        hot_period = storage.count_leads_since(period_hours, min_score=60)
        hh_period = storage.count_leads_since(period_hours, source="HeadHunter")
        hh_hot_period = storage.count_leads_since(
            period_hours, min_score=60, source="HeadHunter"
        )
        latest_created = storage.latest_created_at(2)
        latest_text = ", ".join(latest_created) if latest_created else "нет данных"
        jobs_scan_env = os.getenv("JOBS_SCAN_ENABLED", "")
        max_send_env = os.getenv("MAX_SEND_PER_RUN", "")
        await message.answer(
            "Debug status:\n"
            f"period_hours: {period_hours}\n"
            f"total leads: {total_leads}\n"
            f"leads за период: {leads_period}\n"
            f"hot leads за период: {hot_period}\n"
            f"HH leads за период: {hh_period}\n"
            f"HH hot за период: {hh_hot_period}\n"
            f"latest created_at: {latest_text}\n"
            f"JOBS_SCAN_ENABLED env: {jobs_scan_env or 'unset'}\n"
            f"MAX_SEND_PER_RUN env: {max_send_env or 'unset'}\n",
            reply_markup=build_main_keyboard(),
        )

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
