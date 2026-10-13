from datetime import date

from stock_pipeline.transform import QuoteRow
from stock_pipeline.validate import has_errors, validate


def row(ticker="AAPL", open_price=100.0, avg_volume=1000, pe=20.0, rank=1):
    return QuoteRow(date(2026, 10, 9), ticker, None, open_price, avg_volume, pe, None, rank)


def checks(issues):
    return {i.check for i in issues}


def test_clean_batch_passes():
    rows = [row(t, rank=i) for i, t in enumerate(["AAPL", "MSFT", "NVDA"], 1)]
    assert validate(rows, min_rows=3) == []


def test_too_few_rows_is_error():
    issues = validate([row()], min_rows=5)
    assert "row_count" in checks(issues) and has_errors(issues)


def test_bad_values_flagged():
    issues = validate([row("bad ticker"), row("X", open_price=-1), row("Y", avg_volume=-5)], 1)
    assert {"ticker_format", "open_price_positive", "avg_volume_non_negative"} <= checks(issues)


def test_null_pe_is_fine_but_extreme_pe_warns():
    assert validate([row(pe=None)], 1) == []
    issues = validate([row(pe=50_000)], 1)
    assert checks(issues) == {"pe_ratio_range"} and not has_errors(issues)


def test_mostly_missing_prices_fails_batch():
    rows = [row("A", open_price=None), row("B", open_price=None), row("C")]
    assert "open_price_completeness" in checks(validate(rows, 1))
