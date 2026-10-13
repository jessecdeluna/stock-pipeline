"""One-time migration: import the old notebook's StocksDatabase.db into the new warehouse.

The legacy table has no date column, so you supply the trading date it was captured on:

    python scripts/backfill_legacy.py legacy/StocksDatabase.db --as-of 2026-10-09
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, date, datetime

from stock_pipeline.load import connect, load_clean, record_run
from stock_pipeline.transform import QuoteRow


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("legacy_db")
    p.add_argument("--as-of", required=True, type=date.fromisoformat)
    p.add_argument("--db", default="data/stocks.duckdb")
    args = p.parse_args()

    src = sqlite3.connect(args.legacy_db)
    legacy = src.execute("SELECT Ticker, OpenPrice, AvgVolume, PERatio FROM StocksTable").fetchall()
    src.close()

    rows = [QuoteRow(snapshot_date=args.as_of, ticker=t, company=None, open_price=o,
                     avg_volume=v, pe_ratio=pe, volume=None, rank=i)
            for i, (t, o, v, pe) in enumerate(legacy, start=1)]

    con = connect(args.db)
    now = datetime.now(UTC)
    n = load_clean(con, "legacy-backfill", now, rows)
    record_run(con, run_id="legacy-backfill", started_at=now, finished_at=now, status="success",
               source="legacy_sqlite", rows_extracted=len(legacy), rows_loaded=n, issues=[])
    con.close()
    print(f"Backfilled {n} rows for {args.as_of} into {args.db}")


if __name__ == "__main__":
    main()
