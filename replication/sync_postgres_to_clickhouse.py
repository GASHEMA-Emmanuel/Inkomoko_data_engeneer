#!/usr/bin/env python3
"""
sync_postgres_to_clickhouse.py
==============================
Replicate the raw OLTP tables in PostgreSQL into the ClickHouse OLAP layer.

Key behaviours
--------------
* INCREMENTAL: each table has a watermark (raw.sync_watermark.last_ingested_at)
  in Postgres. We only read rows whose ingested_at > watermark, then advance the
  watermark to the max ingested_at we just moved. The first run (watermark =
  1970-01-01) is effectively a full load.
* IDEMPOTENT: ClickHouse targets are ReplacingMergeTree(ingested_at) ordered by
  id, so re-loading the same id replaces the previous version after merges. This
  makes retries safe.
* VALIDATION: after loading, we compare distinct-id counts in Postgres vs
  ClickHouse (using FINAL to account for not-yet-merged duplicates) and log a
  clear PASS/FAIL line per table.

Usage
-----
    python sync_postgres_to_clickhouse.py            # all tables
    python sync_postgres_to_clickhouse.py --table users
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import clickhouse_connect
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | replication | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("replication")

# --------------------------------------------------------------------------- #
# Table registry: source columns to move (ingested_at is always last)
# --------------------------------------------------------------------------- #
TABLES: dict[str, list[str]] = {
    "users": [
        "id", "name", "username", "email", "phone", "website",
        "company_name", "city", "zipcode", "lat", "lng", "ingested_at",
    ],
    "posts": ["id", "user_id", "title", "body", "ingested_at"],
    "comments": ["id", "post_id", "name", "email", "body", "ingested_at"],
}


# --------------------------------------------------------------------------- #
# Connections
# --------------------------------------------------------------------------- #
def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "inkomoko"),
        password=os.getenv("POSTGRES_PASSWORD", "inkomoko_pwd"),
        dbname=os.getenv("POSTGRES_DB", "inkomoko"),
    )


def ch_connect():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "raw"),
    )


# --------------------------------------------------------------------------- #
# Watermark helpers
# --------------------------------------------------------------------------- #
def get_watermark(pg, table: str) -> datetime:
    with pg.cursor() as cur:
        cur.execute(
            "SELECT last_ingested_at FROM raw.sync_watermark WHERE table_name = %s",
            (table,),
        )
        row = cur.fetchone()
    if row and row[0]:
        return row[0]
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def set_watermark(pg, table: str, value: datetime) -> None:
    with pg, pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.sync_watermark (table_name, last_ingested_at, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (table_name)
            DO UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at,
                          updated_at = now()
            """,
            (table, value),
        )


# --------------------------------------------------------------------------- #
# Core sync
# --------------------------------------------------------------------------- #
def read_incremental(pg, table: str, columns: list[str], watermark: datetime):
    col_list = ", ".join(columns)
    sql = (
        f"SELECT {col_list} FROM raw.{table} "
        f"WHERE ingested_at > %s ORDER BY ingested_at ASC"
    )
    with pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (watermark,))
        return cur.fetchall()


def sync_table(pg, ch, table: str) -> dict[str, Any]:
    columns = TABLES[table]
    watermark = get_watermark(pg, table)
    log.info("[%s] watermark = %s", table, watermark.isoformat())

    records = read_incremental(pg, table, columns, watermark)
    if not records:
        log.info("[%s] no new rows since watermark; nothing to replicate", table)
    else:
        # Build a list-of-lists in the exact column order ClickHouse expects.
        # Coerce Decimal (Postgres NUMERIC) -> float for ClickHouse Float64 cols.
        def _coerce(v: Any) -> Any:
            return float(v) if isinstance(v, Decimal) else v

        data = [[_coerce(rec[c]) for c in columns] for rec in records]
        ch.insert(table, data, column_names=columns)
        log.info("[%s] inserted %d rows into ClickHouse", table, len(records))

        max_ts = max(rec["ingested_at"] for rec in records)
        set_watermark(pg, table, max_ts)
        log.info("[%s] advanced watermark -> %s", table, max_ts.isoformat())

    return validate_table(pg, ch, table)


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate_table(pg, ch, table: str) -> dict[str, Any]:
    with pg.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM raw.{table}")
        pg_count = cur.fetchone()[0]

    # FINAL forces ReplacingMergeTree de-duplication at read time so the count
    # reflects logical (post-merge) rows even before background merges run.
    ch_count = ch.query(f"SELECT count() FROM {table} FINAL").result_rows[0][0]

    status = "PASS" if pg_count == ch_count else "FAIL"
    log.info(
        "[%s] VALIDATION %s | postgres=%d clickhouse=%d",
        table, status, pg_count, ch_count,
    )
    return {"table": table, "postgres": pg_count, "clickhouse": ch_count, "status": status}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(description="Replicate Postgres -> ClickHouse")
    parser.add_argument("--table", choices=list(TABLES.keys()), help="Single table")
    args = parser.parse_args()

    targets = [args.table] if args.table else list(TABLES.keys())

    pg = pg_connect()
    ch = ch_connect()
    results = []
    try:
        for table in targets:
            log.info("=== Replicating table: %s ===", table)
            results.append(sync_table(pg, ch, table))
    finally:
        pg.close()
        ch.close()

    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        log.error("Replication finished with validation failures: %s", failures)
        sys.exit(1)
    log.info("Replication complete. All tables validated successfully.")


if __name__ == "__main__":
    main()
