"""Tests for the Parquet warehouse, the legacy migration, and the query CLI."""

import json
from datetime import UTC, date, datetime

import duckdb
import pytest

from stock_pipeline import extract as ex
from stock_pipeline import pipeline, query
from stock_pipeline.config import Settings
from stock_pipeline.load import Warehouse, connect, migrate_legacy_db

# The v0.2 schema, as stocks.duckdb was created before this release.
LEGACY_DDL = """
CREATE SCHEMA raw;
CREATE TABLE raw.quotes_raw (run_id VARCHAR, extracted_at TIMESTAMPTZ, source VARCHAR,
                             ticker VARCHAR, payload JSON);
CREATE TABLE daily_quotes (snapshot_date DATE, ticker VARCHAR, company VARCHAR, open_price DOUBLE,
    avg_volume BIGINT, pe_ratio DOUBLE, volume BIGINT, rank INTEGER, run_id VARCHAR,
    loaded_at TIMESTAMPTZ, quote_date DATE, PRIMARY KEY (snapshot_date, ticker));
CREATE TABLE pipeline_runs (run_id VARCHAR PRIMARY KEY, started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ, status VARCHAR, source VARCHAR, rows_extracted INTEGER,
    rows_loaded INTEGER, issues JSON);
"""


@pytest.fixture
def settings(tmp_path):
    return Settings(warehouse_dir=str(tmp_path / "warehouse"), top_n=10, min_rows=3,
                    max_retries=1, fallback_tickers=("AAPL",))


@pytest.fixture
def mock_yahoo(monkeypatch, sample_quotes):
    monkeypatch.setattr(ex.time, "sleep", lambda *_: None)
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": sample_quotes})


def files(settings, sub, pattern="*"):
    return sorted(p.name for p in (Warehouse(settings.warehouse_dir).root / sub).glob(pattern))


def test_run_writes_one_file_per_day_run_and_raw_batch(settings, mock_yahoo):
    pipeline.run(settings)
    assert files(settings, "daily_quotes") == ["2026-10-09.parquet"]
    assert len(files(settings, "pipeline_runs", "*.parquet")) == 1
    assert len(files(settings, "raw", "*.jsonl.gz")) == 1


def test_rerun_replaces_the_day_file_instead_of_adding_one(settings, mock_yahoo):
    pipeline.run(settings)
    pipeline.run(settings)
    assert files(settings, "daily_quotes") == ["2026-10-09.parquet"]
    assert len(files(settings, "pipeline_runs", "*.parquet")) == 2
    assert not files(settings, "daily_quotes", "*.tmp")  # no leftover temp files


def test_failed_batch_writes_no_day_file(settings, monkeypatch):
    monkeypatch.setattr(ex.yf, "screen", lambda *a, **k: {"quotes": [{"symbol": "A"}] * 3})
    assert pipeline.run(settings)["status"] == "failed_validation"
    assert files(settings, "daily_quotes") == []
    assert len(files(settings, "pipeline_runs", "*.parquet")) == 1


def test_empty_warehouse_can_be_queried(tmp_path):
    con = connect(tmp_path / "nothing-here")
    assert con.execute("SELECT count(*) FROM latest_quotes").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 0


def make_legacy_db(root):
    root.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(root / "stocks.duckdb"))
    con.execute(LEGACY_DDL)
    t = datetime(2026, 10, 13, 0, 35, tzinfo=UTC)
    for day, run in [(date(2026, 10, 12), "run-a"), (date(2026, 10, 13), "run-b")]:
        for rank, sym in enumerate(["NVDA", "PSKY"], 1):
            con.execute("INSERT INTO daily_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        [day, sym, None, 10.0 * rank, 1000, None, 2000, rank, run, t,
                         date(2026, 10, 5) if sym == "PSKY" else day])
            con.execute("INSERT INTO raw.quotes_raw VALUES (?,?,?,?,?)",
                        [run, t, "screener", sym, json.dumps({"symbol": sym})])
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                    [run, t, t, "success", "screener", 2, 2, "[]"])
    con.close()


def test_legacy_database_is_migrated_and_removed(tmp_path):
    root = tmp_path / "warehouse"
    make_legacy_db(root)
    assert migrate_legacy_db(Warehouse(root)) == 2
    assert not (root / "stocks.duckdb").exists()

    con = connect(root)
    assert con.execute("SELECT count(*) FROM daily_quotes").fetchone()[0] == 4
    assert con.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM raw_quotes").fetchone()[0] == 4
    assert con.execute("SELECT quote_date FROM daily_quotes WHERE ticker = 'PSKY' "
                       "AND snapshot_date = '2026-10-13'").fetchone()[0] == date(2026, 10, 5)
    assert migrate_legacy_db(Warehouse(root)) == 0  # second call is a no-op


def test_pipeline_run_migrates_automatically(settings, mock_yahoo):
    make_legacy_db(Warehouse(settings.warehouse_dir).root)
    pipeline.run(settings)
    assert files(settings, "daily_quotes") == ["2026-10-09.parquet", "2026-10-12.parquet",
                                               "2026-10-13.parquet"]
    assert not (Warehouse(settings.warehouse_dir).root / "stocks.duckdb").exists()


def test_query_cli_formats(settings, mock_yahoo, capsys):
    pipeline.run(settings)
    wh = ["--warehouse", settings.warehouse_dir]

    assert query.main([*wh, "--format", "plain", "SELECT max(snapshot_date) FROM daily_quotes"]) == 0
    assert capsys.readouterr().out.strip() == "2026-10-09"

    assert query.main([*wh, "--summary"]) == 0
    out = capsys.readouterr().out
    assert "### Latest run" in out and "| 1 | NVDA |" in out

    assert query.main([*wh, "SELECT * FROM no_such_table"]) == 1
