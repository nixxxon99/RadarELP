from __future__ import annotations

import sqlite3
from datetime import datetime
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

    def close(self) -> None:
        self._conn.close()
