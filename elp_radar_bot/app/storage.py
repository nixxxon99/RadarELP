from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Iterable


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen (
                    url TEXT PRIMARY KEY,
                    first_seen TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    url TEXT UNIQUE,
                    published TEXT,
                    source TEXT,
                    summary TEXT,
                    demand_score INTEGER,
                    segment TEXT,
                    timing TEXT,
                    company_guess TEXT,
                    created_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    chat_id INTEGER PRIMARY KEY,
                    period_hours INTEGER,
                    updated_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_state (
                    name TEXT PRIMARY KEY,
                    last_run TEXT
                )
                """
            )

    def is_seen(self, url: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,)).fetchone()
        return row is not None

    def mark_seen(self, url: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO seen (url, first_seen) VALUES (?, ?)",
                (url, datetime.utcnow().isoformat()),
            )

    def save_lead(self, lead: dict) -> bool:
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO leads
                    (title, url, published, source, summary, demand_score, segment, timing, company_guess, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead.get("title"),
                    lead.get("url"),
                    lead.get("published"),
                    lead.get("source"),
                    lead.get("summary"),
                    lead.get("demand_score"),
                    lead.get("segment"),
                    lead.get("timing"),
                    lead.get("company_guess"),
                    datetime.utcnow().isoformat(),
                ),
            )
        return cursor.rowcount > 0

    def top_latest(self, limit: int = 10, min_score: int | None = None) -> list[sqlite3.Row]:
        if min_score is None:
            query = "SELECT * FROM leads ORDER BY created_at DESC LIMIT ?"
            params: Iterable = (limit,)
        else:
            query = (
                "SELECT * FROM leads WHERE demand_score >= ? ORDER BY created_at DESC LIMIT ?"
            )
            params = (min_score, limit)
        rows = self._conn.execute(query, params).fetchall()
        return list(rows)

    def count_leads(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS total FROM leads").fetchone()
        return int(row["total"]) if row else 0

    def count_seen(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS total FROM seen").fetchone()
        return int(row["total"]) if row else 0

    def latest_leads(self, limit: int = 3) -> list[sqlite3.Row]:
        rows = self._conn.execute(
            "SELECT title, source, created_at FROM leads ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return list(rows)

    def count_leads_since(
        self,
        hours: int,
        min_score: int | None = None,
        source: str | None = None,
    ) -> int:
        since = datetime.utcnow() - timedelta(hours=hours)
        since_iso = since.isoformat()
        filters = ["created_at >= ?"]
        params: list = [since_iso]
        if min_score is not None:
            filters.append("demand_score >= ?")
            params.append(min_score)
        if source:
            filters.append("source = ?")
            params.append(source)
        where_clause = " AND ".join(filters)
        row = self._conn.execute(
            f"SELECT COUNT(*) AS total FROM leads WHERE {where_clause}",
            params,
        ).fetchone()
        return int(row["total"]) if row else 0

    def latest_created_at(self, limit: int = 2) -> list[str]:
        rows = self._conn.execute(
            "SELECT created_at FROM leads ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [row["created_at"] for row in rows if row and row["created_at"]]

    def get_period_hours(self, chat_id: int) -> int:
        row = self._conn.execute(
            "SELECT period_hours FROM settings WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        if row:
            return int(row["period_hours"])
        default_hours = 168
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO settings (chat_id, period_hours, updated_at) VALUES (?, ?, ?)",
                (chat_id, default_hours, datetime.utcnow().isoformat()),
            )
        return default_hours

    def set_period_hours(self, chat_id: int, hours: int) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO settings (chat_id, period_hours, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    period_hours = excluded.period_hours,
                    updated_at = excluded.updated_at
                """,
                (chat_id, hours, datetime.utcnow().isoformat()),
            )

    def leads_since(
        self,
        chat_id: int,
        hours: int,
        min_score: int | None = None,
        source: str | None = None,
        limit: int = 10,
    ) -> list[sqlite3.Row]:
        since = datetime.utcnow() - timedelta(hours=hours)
        since_iso = since.isoformat()
        filters = ["created_at >= ?"]
        params: list = [since_iso]
        if min_score is not None:
            filters.append("demand_score >= ?")
            params.append(min_score)
        if source:
            filters.append("source = ?")
            params.append(source)
        where_clause = " AND ".join(filters)
        query = f"SELECT * FROM leads WHERE {where_clause} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return list(rows)

    def close(self) -> None:
        self._conn.close()

    def get_last_scan(self, name: str) -> datetime | None:
        row = self._conn.execute(
            "SELECT last_run FROM scan_state WHERE name = ?",
            (name,),
        ).fetchone()
        if not row or not row["last_run"]:
            return None
        return datetime.fromisoformat(row["last_run"])

    def set_last_scan(self, name: str, when: datetime) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO scan_state (name, last_run)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_run = excluded.last_run
                """,
                (name, when.isoformat()),
            )
