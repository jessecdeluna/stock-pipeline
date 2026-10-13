"""Transform: normalize raw Yahoo payloads into typed, clean rows."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("America/New_York")

# Screener payloads and Ticker.info payloads name the same fields differently,
# so each output column lists the keys to try in priority order.
FIELD_KEYS = {
    "open_price": ("regularMarketOpen", "open"),
    "avg_volume": ("averageDailyVolume3Month", "averageVolume"),
    "pe_ratio":   ("trailingPE",),
    "volume":     ("regularMarketVolume", "volume"),
    "company":    ("shortName", "longName"),
}


@dataclass
class QuoteRow:
    snapshot_date: date
    ticker: str
    company: str | None
    open_price: float | None
    avg_volume: int | None
    pe_ratio: float | None   # NULL is legitimate: no P/E when earnings are negative
    volume: int | None
    rank: int                # position in the most-actives list (1 = most active)
    quote_date: date | None = None  # trading date of this ticker's own last quote

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _first(raw: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        v = raw.get(k)
        if isinstance(v, dict):          # some payloads wrap values: {"raw": 1.2, "fmt": "1.20"}
            v = v.get("raw")
        if v not in (None, "", "N/A", "--", "Infinity"):
            return v
    return None


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(str(v).replace(",", ""))
    except ValueError:
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def to_int(v: Any) -> int | None:
    f = to_float(v)
    return None if f is None else int(f)


def market_date(raw: dict[str, Any], fallback: datetime) -> date:
    """Trading date the quote belongs to, in New York time (not your laptop's clock)."""
    ts = raw.get("regularMarketTime")
    if isinstance(ts, int | float) and ts > 0:
        return datetime.fromtimestamp(ts, tz=UTC).astimezone(MARKET_TZ).date()
    return fallback.astimezone(MARKET_TZ).date()


def transform(records: list[dict[str, Any]], extracted_at: datetime) -> list[QuoteRow]:
    rows: list[QuoteRow] = []
    seen: set[str] = set()
    for raw in records:
        ticker = str(raw.get("symbol") or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        company = _first(raw, FIELD_KEYS["company"])
        rows.append(QuoteRow(
            snapshot_date=market_date(raw, extracted_at),  # replaced with batch date below
            ticker=ticker,
            company=str(company) if company else None,
            open_price=to_float(_first(raw, FIELD_KEYS["open_price"])),
            avg_volume=to_int(_first(raw, FIELD_KEYS["avg_volume"])),
            pe_ratio=to_float(_first(raw, FIELD_KEYS["pe_ratio"])),
            volume=to_int(_first(raw, FIELD_KEYS["volume"])),
            rank=len(rows) + 1,
            quote_date=market_date(raw, extracted_at),
        ))

    # One run = one trading day. A ticker that hasn't traded today (halted, thin
    # volume) still carries an older quote time; it belongs to *this* run's
    # snapshot, not to a past day's partition. Its own date is kept in quote_date.
    if rows:
        batch_date = max(r.quote_date for r in rows)
        for r in rows:
            r.snapshot_date = batch_date
    return rows
