from __future__ import annotations

from typing import Iterable

from app.config import Settings


def fetch_yandex_serpapi_results(
    settings: Settings,
    queries: Iterable[str],
    max_results_per_query: int = 5,
) -> list[dict]:
    _ = (settings, queries, max_results_per_query)
    return []
