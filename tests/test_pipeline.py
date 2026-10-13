"""End-to-end tests with Yahoo mocked out, so CI never touches the network."""

import duckdb
import pytest
from conftest import screener_quote

from stock_pipeline import extract as ex
from stock_pipeline import pipeline
from stock_pipeline.config import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(db_path=str(tmp_path / "test.duckdb"), top_n=10, min_rows=3,
                    max_retries=1, fallback_tickers=("AAPL", "MSFT", "NVDA"))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(ex.time, "sleep", lambda *_: None)


def query(settings, sql):
    con = duckdb.connect(settings.db_path)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_successful_run_loads_raw_and_clean(monkeypatch, settings, sample_quotes):
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": sample_quotes})
    summary = pipeline.run(settings)

    assert summary["status"] == "success" and summary["source"] == "screener"
    assert query(settings, "SELECT count(*) FROM daily_quotes")[0][0] == 5
    assert query(settings, "SELECT count(*) FROM raw.quotes_raw")[0][0] == 5
    assert query(settings, "SELECT status FROM pipeline_runs")[0][0] == "success"


def test_rerun_same_day_is_idempotent(monkeypatch, settings, sample_quotes):
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": sample_quotes})
    pipeline.run(settings)
    pipeline.run(settings)
    assert query(settings, "SELECT count(*) FROM daily_quotes")[0][0] == 5
    assert query(settings, "SELECT count(*) FROM pipeline_runs")[0][0] == 2


def test_history_is_kept_across_days(monkeypatch, settings, sample_quotes):
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": sample_quotes})
    pipeline.run(settings)
    next_day = [{**q, "regularMarketTime": q["regularMarketTime"] + 86_400} for q in sample_quotes]
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": next_day})
    pipeline.run(settings)
    assert query(settings, "SELECT count(DISTINCT snapshot_date) FROM daily_quotes")[0][0] == 2


def test_screener_failure_uses_fallback_and_records_it(monkeypatch, settings):
    def boom(*a, **k):
        raise ConnectionError("blocked")

    class FakeTicker:
        def __init__(self, t):
            self.info = screener_quote(t)

    monkeypatch.setattr(ex.yf, "screen", boom)
    monkeypatch.setattr(ex.yf, "Ticker", FakeTicker)
    summary = pipeline.run(settings)

    assert summary["status"] == "success" and summary["source"] == "fallback"
    assert query(settings, "SELECT source FROM pipeline_runs")[0][0] == "fallback"


def test_total_outage_fails_cleanly(monkeypatch, settings):
    def boom(*a, **k):
        raise ConnectionError("blocked")

    monkeypatch.setattr(ex.yf, "screen", boom)
    monkeypatch.setattr(ex.yf, "Ticker", boom)
    summary = pipeline.run(settings)

    assert summary["status"] == "failed_validation"
    assert query(settings, "SELECT status, rows_extracted FROM pipeline_runs")[0] == ("failed_validation", 0)


def test_bad_batch_is_not_loaded(monkeypatch, settings):
    junk = [{"symbol": "AAPL"}, {"symbol": "MSFT"}, {"symbol": "NVDA"}]  # no prices at all
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": junk})
    summary = pipeline.run(settings)

    assert summary["status"] == "failed_validation"
    assert query(settings, "SELECT count(*) FROM daily_quotes")[0][0] == 0
    assert query(settings, "SELECT count(*) FROM raw.quotes_raw")[0][0] == 3  # raw kept for debugging


def test_export_csv_matches_legacy_format(monkeypatch, settings, sample_quotes, tmp_path):
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": sample_quotes})
    pipeline.run(settings)
    out = tmp_path / "stocks.txt"
    pipeline.export_csv(settings.db_path, str(out))
    lines = out.read_text().splitlines()
    assert lines[0] == "Ticker,OpenPrice,AvgVolume,PERatio"
    assert lines[1] == "NVDA,221.59,151325148,34.24"
    assert lines[4] == "INTC,102.34,123393238,"


def test_stale_quote_stays_in_todays_snapshot(monkeypatch, settings, sample_quotes):
    """Regression: 25 loaded / 24 exported when one ticker had an older quote time."""
    quotes = [dict(q) for q in sample_quotes]
    quotes[2]["regularMarketTime"] -= 3 * 86_400  # AAPL last traded 3 days earlier
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": quotes})
    summary = pipeline.run(settings)

    assert summary["status"] == "success"
    assert any("stale_quote" in i and "AAPL" in i for i in summary["issues"])
    assert query(settings, "SELECT count(DISTINCT snapshot_date) FROM daily_quotes")[0][0] == 1
    assert query(settings, "SELECT count(*) FROM latest_quotes")[0][0] == 5
    assert query(settings, "SELECT quote_date < snapshot_date FROM daily_quotes WHERE ticker='AAPL'")[0][0]
