"""Query the warehouse with SQL from the command line.

    stock-query "SELECT * FROM latest_quotes"
    stock-query --summary                  # latest run + snapshot as Markdown
    stock-query --format plain "SELECT max(snapshot_date) FROM daily_quotes"
    stock-query --file sql/analysis.sql     # run every query in a .sql file

Tables: daily_quotes, latest_quotes, pipeline_runs, raw_quotes.
Reads ./warehouse by default (the history committed by the daily job);
use --warehouse data/warehouse for your own local runs.
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb

from .load import connect

SUMMARY_QUERIES = {
    "Latest run": """SELECT run_id, status, source, rows_loaded, issues
                     FROM pipeline_runs ORDER BY started_at DESC LIMIT 1""",
    "Latest snapshot": """SELECT snapshot_date, rank, ticker, open_price, avg_volume, pe_ratio
                          FROM latest_quotes""",
}


def _cell(v: object) -> str:
    return "" if v is None else str(v)


def to_markdown(rel: duckdb.DuckDBPyRelation) -> str:
    lines = ["| " + " | ".join(rel.columns) + " |", "|" + "---|" * len(rel.columns)]
    lines += ["| " + " | ".join(_cell(v) for v in row) + " |" for row in rel.fetchall()]
    return "\n".join(lines)


def show(rel: duckdb.DuckDBPyRelation | None, fmt: str) -> None:
    if rel is None:  # a statement with no result, e.g. CREATE
        return
    if fmt == "markdown":
        print(to_markdown(rel) + "\n")
    elif fmt == "plain":
        for row in rel.fetchall():
            print("\t".join(_cell(v) for v in row))
    else:
        rel.show(max_rows=100, max_width=200)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run SQL against the stock warehouse")
    p.add_argument("sql", nargs="?", help="SQL query (default: SELECT * FROM latest_quotes)")
    p.add_argument("--warehouse", default=os.getenv("STOCKS_WAREHOUSE_DIR", "warehouse"),
                   help="warehouse folder (default: ./warehouse)")
    p.add_argument("--format", choices=["table", "markdown", "plain"], default="table")
    p.add_argument("--summary", action="store_true", help="print the latest run and snapshot as Markdown")
    p.add_argument("--file", metavar="PATH", help="run every statement in a .sql file")
    args = p.parse_args(argv)

    con = connect(args.warehouse)
    try:
        if args.summary:
            for title, sql in SUMMARY_QUERIES.items():
                print(f"### {title}\n\n{to_markdown(con.sql(sql))}\n")
            return 0
        if args.file:
            with open(args.file, encoding="utf-8") as f:
                statements = [s.query for s in con.extract_statements(f.read())]
        else:
            statements = [args.sql or "SELECT * FROM latest_quotes"]
        for sql in statements:
            show(con.sql(sql), args.format)
        return 0
    except duckdb.Error as exc:
        print(f"Query error: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
