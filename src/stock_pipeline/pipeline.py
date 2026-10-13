"""Orchestrates one pipeline run: extract -> raw load -> transform -> validate -> clean load.

Usage:
    python -m stock_pipeline.pipeline             # run once
    python -m stock_pipeline.pipeline --export-csv stocks.txt
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import uuid
from datetime import UTC, datetime

from . import extract as ex
from .config import Settings
from .load import connect, load_clean, load_raw, record_run
from .transform import transform
from .validate import has_errors, validate

log = logging.getLogger("stock_pipeline")


def run(settings: Settings | None = None) -> dict:
    settings = settings or Settings()
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(UTC)
    con = connect(settings.db_path)
    source, raw_count, loaded, issues, status = None, 0, 0, [], "failed"

    try:
        result = ex.extract(settings.screener, settings.top_n,
                            settings.fallback_tickers, settings.max_retries)
        source, raw_count = result.source, len(result.records)
        load_raw(con, run_id, started, source, result.records)

        rows = transform(result.records, started)
        issues = validate(rows, settings.min_rows)
        for issue in issues:
            (log.error if issue.severity == "error" else log.warning)(str(issue))

        if has_errors(issues):
            status = "failed_validation"
        else:
            loaded = load_clean(con, run_id, datetime.now(UTC), rows)
            status = "success"
        return {"run_id": run_id, "status": status, "source": source,
                "rows_extracted": raw_count, "rows_loaded": loaded,
                "issues": [str(i) for i in issues]}
    except Exception:
        log.exception("Pipeline run %s crashed", run_id)
        raise
    finally:
        record_run(con, run_id=run_id, started_at=started, finished_at=datetime.now(UTC),
                   status=status, source=source, rows_extracted=raw_count,
                   rows_loaded=loaded, issues=issues)
        log.info("Run %s finished: status=%s source=%s extracted=%d loaded=%d",
                 run_id, status, source, raw_count, loaded)
        con.close()


def export_csv(db_path: str, path: str) -> None:
    """Write the latest snapshot in the original stocks.txt format."""
    con = connect(db_path)
    rows = con.execute(
        "SELECT ticker, open_price, avg_volume, pe_ratio FROM latest_quotes"
    ).fetchall()
    con.close()
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Ticker", "OpenPrice", "AvgVolume", "PERatio"])
        w.writerows([["" if v is None else v for v in r] for r in rows])
    log.info("Exported %d rows to %s", len(rows), path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Most-active stocks ELT pipeline")
    parser.add_argument("--export-csv", metavar="PATH", help="also export latest snapshot to CSV")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    settings = Settings()
    summary = run(settings)
    if args.export_csv and summary["status"] == "success":
        export_csv(settings.db_path, args.export_csv)
    return 0 if summary["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
