"""Extract: pull the most-active stocks and their quote stats from Yahoo Finance.

Replaces the old CNN Selenium scrape (which silently fell back to a hardcoded list
on every run) and the Yahoo HTML scrape (which broke whenever Yahoo changed its
markup). Uses Yahoo's screener API via `yfinance`, which returns tickers AND the
stats we need in a single request.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

import yfinance as yf

log = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class ExtractResult:
    records: list[dict[str, Any]]  # raw quote payloads, untouched
    source: str                    # "screener" or "fallback"


def retry(max_attempts: int = 3, base_delay: float = 2.0) -> Callable:
    """Retry a flaky network call with exponential backoff (2s, 4s, 8s...)."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - network libs raise many types
                    if attempt == max_attempts:
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    log.warning("%s failed (attempt %d/%d): %s; retrying in %.0fs",
                                fn.__name__, attempt, max_attempts, exc, delay)
                    time.sleep(delay)
            raise RuntimeError("unreachable")

        return wrapper

    return decorator


def fetch_screener(screener: str, count: int, max_retries: int = 3) -> list[dict[str, Any]]:
    """Return raw quote dicts from a predefined Yahoo screener (e.g. 'most_actives')."""

    @retry(max_attempts=max_retries)
    def _call() -> list[dict[str, Any]]:
        resp = yf.screen(screener, count=count)
        quotes = resp.get("quotes", []) if isinstance(resp, dict) else []
        if not quotes:
            raise ValueError(f"Screener '{screener}' returned no quotes")
        return quotes

    return _call()


def fetch_ticker_info(ticker: str, max_retries: int = 3) -> dict[str, Any]:
    """Return the raw info dict for one ticker (used only on the fallback path)."""

    @retry(max_attempts=max_retries)
    def _call() -> dict[str, Any]:
        info = yf.Ticker(ticker).info or {}
        info.setdefault("symbol", ticker)
        return info

    return _call()


def extract(screener: str, top_n: int, fallback_tickers: tuple[str, ...],
            max_retries: int = 3) -> ExtractResult:
    """Try the screener first; if it fails, look up a fixed ticker list one by one.

    Unlike the original notebook, the fallback is logged loudly AND recorded in the
    database (source='fallback'), so a silent degradation can't hide for weeks.
    """
    try:
        quotes = fetch_screener(screener, top_n, max_retries)
        log.info("Screener '%s' returned %d quotes", screener, len(quotes))
        return ExtractResult(records=quotes, source="screener")
    except Exception as exc:  # noqa: BLE001
        log.error("Screener failed after retries (%s); using fallback list of %d tickers",
                  exc, len(fallback_tickers))

    records = []
    for ticker in fallback_tickers:
        try:
            records.append(fetch_ticker_info(ticker, max_retries))
        except Exception as exc:  # noqa: BLE001
            log.error("Could not fetch %s: %s", ticker, exc)
        time.sleep(0.5)  # be polite to Yahoo
    return ExtractResult(records=records, source="fallback")
