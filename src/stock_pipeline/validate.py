"""Data quality checks. Errors block the load; warnings are logged and recorded."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .transform import QuoteRow

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


@dataclass
class Issue:
    severity: str  # "error" | "warning"
    check: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.check}: {self.detail}"


def validate(rows: list[QuoteRow], min_rows: int) -> list[Issue]:
    issues: list[Issue] = []

    if len(rows) < min_rows:
        issues.append(Issue("error", "row_count", f"got {len(rows)} rows, expected >= {min_rows}"))

    tickers = [r.ticker for r in rows]
    dupes = {t for t in tickers if tickers.count(t) > 1}
    if dupes:
        issues.append(Issue("error", "unique_ticker", f"duplicates: {sorted(dupes)}"))

    for r in rows:
        if not TICKER_RE.match(r.ticker):
            issues.append(Issue("error", "ticker_format", f"invalid ticker {r.ticker!r}"))
        if r.open_price is None:
            issues.append(Issue("warning", "open_price_not_null", f"{r.ticker} has no open price"))
        elif r.open_price <= 0:
            issues.append(Issue("error", "open_price_positive", f"{r.ticker} open={r.open_price}"))
        if r.avg_volume is not None and r.avg_volume < 0:
            issues.append(Issue("error", "avg_volume_non_negative", f"{r.ticker} avg_vol={r.avg_volume}"))
        if r.pe_ratio is not None and not (0 < r.pe_ratio < 10_000):
            issues.append(Issue("warning", "pe_ratio_range", f"{r.ticker} P/E={r.pe_ratio}"))

    # If most of the batch is missing prices, the source probably changed shape.
    if rows:
        missing = sum(r.open_price is None for r in rows) / len(rows)
        if missing > 0.5:
            issues.append(Issue("error", "open_price_completeness",
                                f"{missing:.0%} of rows missing open price"))
    return issues


def has_errors(issues: list[Issue]) -> bool:
    return any(i.severity == "error" for i in issues)
