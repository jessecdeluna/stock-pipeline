"""Pipeline settings, read from environment variables with sensible defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_FALLBACK_TICKERS = (
    "NVDA", "TSLA", "AAPL", "AMD", "AMZN", "MSFT", "META", "GOOGL", "INTC", "BAC",
)


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    return tuple(t.strip().upper() for t in raw.split(",") if t.strip())


@dataclass(frozen=True)
class Settings:
    db_path: str = field(default_factory=lambda: os.getenv("STOCKS_DB_PATH", "data/stocks.duckdb"))
    screener: str = field(default_factory=lambda: os.getenv("STOCKS_SCREENER", "most_actives"))
    top_n: int = field(default_factory=lambda: int(os.getenv("STOCKS_TOP_N", "25")))
    min_rows: int = field(default_factory=lambda: int(os.getenv("STOCKS_MIN_ROWS", "5")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("STOCKS_MAX_RETRIES", "3")))
    fallback_tickers: tuple[str, ...] = field(
        default_factory=lambda: _env_list("STOCKS_FALLBACK_TICKERS", DEFAULT_FALLBACK_TICKERS)
    )
