"""Helpers for monthly thread discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class DiscoveryQuery:
    """One search query used to locate a monthly hiring thread."""

    thread_month: str
    search_query: str


def month_label_to_date(thread_month: str) -> date:
    """Convert a YYYY-MM label into a date anchored to the first day of that month."""

    year_text, month_text = thread_month.split("-", maxsplit=1)
    return date(int(year_text), int(month_text), 1)


def google_query_variants(thread_month: str) -> list[DiscoveryQuery]:
    """Return the preferred Google query variants for one target month."""

    month_date = month_label_to_date(thread_month)
    month_name = month_date.strftime("%B")
    year = month_date.year
    return [
        DiscoveryQuery(
            thread_month=thread_month,
            search_query=f"news.ycombinator hiring for {month_name} {year}",
        ),
        DiscoveryQuery(
            thread_month=thread_month,
            search_query=f"Hacker News hiring for {month_name} {year}",
        ),
    ]
