from datetime import UTC, date, datetime

from conftest import screener_quote

from stock_pipeline.transform import to_float, to_int, transform

NOW = datetime(2026, 10, 10, 2, 0, tzinfo=UTC)  # 10pm Oct 9 in New York


def test_screener_payload_maps_to_columns(sample_quotes):
    rows = transform(sample_quotes, NOW)
    nvda = rows[0]
    assert nvda.ticker == "NVDA"
    assert nvda.open_price == 221.59
    assert nvda.avg_volume == 151_325_148
    assert nvda.pe_ratio == 34.24
    assert nvda.rank == 1
    assert nvda.snapshot_date == date(2026, 10, 9)


def test_missing_pe_is_null_not_error(sample_quotes):
    intc = next(r for r in transform(sample_quotes, NOW) if r.ticker == "INTC")
    assert intc.pe_ratio is None


def test_ticker_info_payload_keys_also_work():
    info = {"symbol": "msft", "open": "499.21", "averageVolume": "41,354,858", "trailingPE": 27.85}
    row = transform([info], NOW)[0]
    assert (row.ticker, row.open_price, row.avg_volume, row.pe_ratio) == ("MSFT", 499.21, 41_354_858, 27.85)
    assert row.snapshot_date == date(2026, 10, 9)  # falls back to extract time, NY date


def test_duplicates_and_blank_symbols_dropped():
    rows = transform([screener_quote("AMD"), screener_quote("AMD"), {"symbol": ""}], NOW)
    assert [r.ticker for r in rows] == ["AMD"]


def test_wrapped_raw_values():
    q = {"symbol": "X", "regularMarketOpen": {"raw": 12.5, "fmt": "12.50"}}
    assert transform([q], NOW)[0].open_price == 12.5


def test_numeric_parsers():
    assert to_float("1,234.5") == 1234.5
    assert to_float("abc") is None
    assert to_float(float("nan")) is None
    assert to_int("4,521,300") == 4_521_300
    assert to_int(None) is None
