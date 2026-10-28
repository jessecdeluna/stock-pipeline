"""One-time migration: import the old notebook's StocksDatabase.db into the new warehouse.

The legacy table has no date column, so you supply the trading date it was captured on:

    python scripts/backfill_legacy.py legacy/StocksDatabase.db --as-of 2026-10-09 --warehouse warehouse
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, date, datetime

from stock_pipeline.load import Warehouse, record_run, write_clean
from stock_pipeline.transform import QuoteRow


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("legacy_db")
    p.add_argument("--as-of", required=True, type=date.fromisoformat)
    p.add_argument("--warehouse", default="data/warehouse")
    args = p.parse_args()

    src = sqlite3.connect(args.legacy_db)
    legacy = src.execute("SELECT Ticker, OpenPrice, AvgVolume, PERatio FROM StocksTable").fetchall()
    src.close()

    rows = [QuoteRow(snapshot_date=args.as_of, ticker=t, company=None, open_price=o,
                     avg_volume=v, pe_ratio=pe, volume=None, rank=i)
            for i, (t, o, v, pe) in enumerate(legacy, start=1)]

    wh = Warehouse(args.warehouse)
    now = datetime.now(UTC)
    n = write_clean(wh, "legacy-backfill", now, rows)
    record_run(wh, run_id="legacy-backfill", started_at=now, finished_at=now, status="success",
               source="legacy_sqlite", rows_extracted=len(legacy), rows_loaded=n, issues=[])
    print(f"Backfilled {n} rows for {args.as_of} into {wh.quote_file(args.as_of)}")


if __name__ == "__main__":
    main()
