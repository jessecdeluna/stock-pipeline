"""Load: write raw payloads and clean rows to DuckDB.

Two layers:
  raw.quotes_raw      - every payload exactly as received (append-only, for replay/debugging)
  main.daily_quotes   - clean, typed rows; one row per (snapshot_date, ticker)

The original notebook ran DROP TABLE on every run, so history was lost. Here each
run replaces only its own trading day (partition overwrite), which makes reruns
idempotent while keeping every previous day.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from .transform import QuoteRow
from .validate import Issue

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.quotes_raw (
    run_id        VARCHAR     NOT NULL,
    extracted_at  TIMESTAMPTZ NOT NULL,
    source        VARCHAR     NOT NULL,
    ticker        VARCHAR,
    payload       JSON
);

CREATE TABLE IF NOT EXISTS daily_quotes (
    snapshot_date DATE        NOT NULL,
    ticker        VARCHAR     NOT NULL,
    company       VARCHAR,
    open_price    DOUBLE,
    avg_volume    BIGINT,
    pe_ratio      DOUBLE,
    volume        BIGINT,
    rank          INTEGER,
    run_id        VARCHAR     NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_date, ticker)
);

ALTER TABLE daily_quotes ADD COLUMN IF NOT EXISTS quote_date DATE;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id         VARCHAR PRIMARY KEY,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    status         VARCHAR,      -- success | failed_validation | failed
    source         VARCHAR,      -- screener | fallback
    rows_extracted INTEGER,
    rows_loaded    INTEGER,
    issues         JSON
);

CREATE OR REPLACE VIEW latest_quotes AS
SELECT * FROM daily_quotes
WHERE snapshot_date = (SELECT max(snapshot_date) FROM daily_quotes)
ORDER BY rank;
"""


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(DDL)
    return con


def load_raw(con: duckdb.DuckDBPyConnection, run_id: str, extracted_at: datetime,
             source: str, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    con.executemany(
        "INSERT INTO raw.quotes_raw VALUES (?, ?, ?, ?, ?)",
        [(run_id, extracted_at, source, r.get("symbol"), json.dumps(r, default=str))
         for r in records],
    )


def load_clean(con: duckdb.DuckDBPyConnection, run_id: str, loaded_at: datetime,
               rows: list[QuoteRow]) -> int:
    if not rows:
        return 0
    dates = sorted({r.snapshot_date for r in rows})
    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany("DELETE FROM daily_quotes WHERE snapshot_date = ?", [(d,) for d in dates])
        con.executemany(
            """INSERT INTO daily_quotes (snapshot_date, ticker, company, open_price, avg_volume,
                   pe_ratio, volume, rank, run_id, loaded_at, quote_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(r.snapshot_date, r.ticker, r.company, r.open_price, r.avg_volume,
              r.pe_ratio, r.volume, r.rank, run_id, loaded_at, r.quote_date) for r in rows],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)


def record_run(con: duckdb.DuckDBPyConnection, *, run_id: str, started_at: datetime,
               finished_at: datetime, status: str, source: str | None,
               rows_extracted: int, rows_loaded: int, issues: list[Issue]) -> None:
    con.execute(
        "INSERT OR REPLACE INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, started_at, finished_at, status, source, rows_extracted, rows_loaded,
         json.dumps([i.__dict__ for i in issues])],
    )
