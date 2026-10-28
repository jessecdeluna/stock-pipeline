"""Load: write the warehouse as plain files, query it with DuckDB.

    warehouse/
      daily_quotes/2026-10-27.parquet      clean rows, one file per trading day
      pipeline_runs/<time>_<run_id>.parquet one row per run (the audit log)
      raw/<time>_<run_id>.jsonl.gz          every API payload exactly as received

Files are the source of truth. DuckDB is only the query engine: `connect()` opens an
in-memory database with views over the files, so there is no binary database to
keep in sync or commit. Each day's file is replaced as a whole (write to a temp
file, then rename), which keeps reruns idempotent.

The same layout works unchanged in cloud object storage (S3, GCS) later.
"""

from __future__ import annotations

import gzip
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from .transform import QuoteRow
from .validate import Issue

QUOTE_COLUMNS = """
    snapshot_date DATE NOT NULL,
    ticker        VARCHAR NOT NULL,
    company       VARCHAR,
    open_price    DOUBLE,
    avg_volume    BIGINT,
    pe_ratio      DOUBLE,
    volume        BIGINT,
    rank          INTEGER,
    quote_date    DATE,
    run_id        VARCHAR NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL
"""

RUN_COLUMNS = """
    run_id         VARCHAR NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    status         VARCHAR,
    source         VARCHAR,
    rows_extracted INTEGER,
    rows_loaded    INTEGER,
    issues         VARCHAR
"""

RAW_COLUMNS = ("{run_id: 'VARCHAR', extracted_at: 'TIMESTAMPTZ', source: 'VARCHAR', "
               "ticker: 'VARCHAR', payload: 'JSON'}")


@dataclass(frozen=True)
class Warehouse:
    root: Path

    def __init__(self, root: str | os.PathLike) -> None:
        object.__setattr__(self, "root", Path(root))

    @property
    def quotes_dir(self) -> Path:
        return self.root / "daily_quotes"

    @property
    def runs_dir(self) -> Path:
        return self.root / "pipeline_runs"

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    def quote_file(self, day: date) -> Path:
        return self.quotes_dir / f"{day.isoformat()}.parquet"


def _stamp(ts: datetime) -> str:
    """Sortable file-name prefix, e.g. 20261027T194312Z."""
    return ts.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _sql_path(p: Path) -> str:
    return p.as_posix().replace("'", "''")


def _write_parquet(columns: str, rows: list[tuple], dest: Path) -> None:
    """Write rows to a Parquet file atomically (temp file, then rename)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute(f"CREATE TABLE t ({columns})")
        if rows:
            con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(rows[0]))})", rows)
        con.execute(f"COPY (SELECT * FROM t) TO '{_sql_path(tmp)}' (FORMAT parquet, COMPRESSION zstd)")
    finally:
        con.close()
    os.replace(tmp, dest)


# ---------------------------------------------------------------- writers

def write_raw(wh: Warehouse, run_id: str, extracted_at: datetime, source: str,
              records: list[dict[str, Any]]) -> Path | None:
    if not records:
        return None
    wh.raw_dir.mkdir(parents=True, exist_ok=True)
    dest = wh.raw_dir / f"{_stamp(extracted_at)}_{run_id}.jsonl.gz"
    with gzip.GzipFile(dest, "wb", mtime=0) as f:
        for r in records:
            line = {"run_id": run_id, "extracted_at": extracted_at.isoformat(), "source": source,
                    "ticker": r.get("symbol"), "payload": r}
            f.write((json.dumps(line, default=str) + "\n").encode())
    return dest


def write_clean(wh: Warehouse, run_id: str, loaded_at: datetime, rows: list[QuoteRow]) -> int:
    """Replace each trading day's file with these rows (idempotent per day)."""
    by_day: dict[date, list[QuoteRow]] = {}
    for r in rows:
        by_day.setdefault(r.snapshot_date, []).append(r)
    for day, day_rows in by_day.items():
        _write_parquet(QUOTE_COLUMNS, [
            (r.snapshot_date, r.ticker, r.company, r.open_price, r.avg_volume, r.pe_ratio,
             r.volume, r.rank, r.quote_date, run_id, loaded_at) for r in day_rows
        ], wh.quote_file(day))
    return len(rows)


def record_run(wh: Warehouse, *, run_id: str, started_at: datetime, finished_at: datetime,
               status: str, source: str | None, rows_extracted: int, rows_loaded: int,
               issues: list[Issue]) -> Path:
    dest = wh.runs_dir / f"{_stamp(started_at)}_{run_id}.parquet"
    _write_parquet(RUN_COLUMNS, [(
        run_id, started_at, finished_at, status, source, rows_extracted, rows_loaded,
        json.dumps([i.__dict__ for i in issues]),
    )], dest)
    return dest


# ---------------------------------------------------------------- reader

def _view(con: duckdb.DuckDBPyConnection, name: str, folder: Path, pattern: str,
          reader: str, empty_columns: str) -> None:
    if any(folder.glob(pattern)):
        source = reader.replace("{glob}", _sql_path(folder / pattern))
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM {source}")
    else:
        con.execute(f"CREATE TABLE {name} ({empty_columns})")


def connect(wh: Warehouse | str | os.PathLike) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with views over the warehouse files."""
    wh = wh if isinstance(wh, Warehouse) else Warehouse(wh)
    con = duckdb.connect()
    _view(con, "daily_quotes", wh.quotes_dir, "*.parquet",
          "read_parquet('{glob}', hive_partitioning = false, union_by_name = true)", QUOTE_COLUMNS)
    _view(con, "pipeline_runs", wh.runs_dir, "*.parquet",
          "read_parquet('{glob}', hive_partitioning = false, union_by_name = true)", RUN_COLUMNS)
    _view(con, "raw_quotes", wh.raw_dir, "*.jsonl.gz",
          "read_json('{glob}', format = 'newline_delimited', columns = " + RAW_COLUMNS + ")",
          "run_id VARCHAR, extracted_at TIMESTAMPTZ, source VARCHAR, ticker VARCHAR, payload JSON")
    con.execute("""
        CREATE VIEW latest_quotes AS
        SELECT * FROM daily_quotes
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM daily_quotes)
        ORDER BY rank
    """)
    return con


# ---------------------------------------------------------------- one-time migration

def migrate_legacy_db(wh: Warehouse) -> int:
    """Convert a v0.2 `stocks.duckdb` in the warehouse folder to files, then delete it.

    Returns the number of trading days migrated (0 if there was nothing to do).
    """
    legacy = wh.root / "stocks.duckdb"
    if not legacy.exists():
        return 0
    src = duckdb.connect(str(legacy), read_only=True)
    try:
        src.execute("SET TimeZone = 'UTC'")
        cols = {r[0] for r in src.execute("DESCRIBE daily_quotes").fetchall()}
        quote_date = "quote_date" if "quote_date" in cols else "NULL::DATE"
        quotes = src.execute(f"""
            SELECT snapshot_date, ticker, company, open_price, avg_volume, pe_ratio, volume, rank,
                   {quote_date}, run_id, loaded_at
            FROM daily_quotes ORDER BY snapshot_date, rank""").fetchall()
        runs = src.execute("""
            SELECT run_id, started_at, finished_at, status, source, rows_extracted, rows_loaded,
                   CAST(issues AS VARCHAR)
            FROM pipeline_runs ORDER BY started_at""").fetchall()
        raw = src.execute("""
            SELECT run_id, extracted_at, source, ticker, CAST(payload AS VARCHAR)
            FROM raw.quotes_raw ORDER BY extracted_at""").fetchall()
    finally:
        src.close()

    by_day: dict[date, list[tuple]] = {}
    for q in quotes:
        by_day.setdefault(q[0], []).append(q)
    for day, day_rows in by_day.items():
        _write_parquet(QUOTE_COLUMNS, day_rows, wh.quote_file(day))

    for run in runs:
        _write_parquet(RUN_COLUMNS, [run], wh.runs_dir / f"{_stamp(run[1])}_{run[0]}.parquet")

    by_run: dict[str, list[tuple]] = {}
    for r in raw:
        by_run.setdefault(r[0], []).append(r)
    for run_id, recs in by_run.items():
        records = [json.loads(r[4]) for r in recs]
        write_raw(wh, run_id, recs[0][1], recs[0][2], records)

    legacy.unlink()
    wal = legacy.with_name(legacy.name + ".wal")
    if wal.exists():
        wal.unlink()
    return len(by_day)
