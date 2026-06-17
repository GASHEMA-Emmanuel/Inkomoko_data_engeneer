#!/usr/bin/env python3
"""
extract_load_postgres.py
========================
Extract data from the public JSONPlaceholder REST API and load it into the
PostgreSQL OLTP layer (schema: raw).

Design notes
------------
* One script handles all three entities (users / posts / comments) driven by a
  small ENTITY_CONFIG registry. This avoids three near-identical files and keeps
  the transform logic in one place.
* Network calls are wrapped with tenacity retries (exponential backoff) so a
  transient API/network blip does not fail the whole pipeline.
* Loads are idempotent: we UPSERT on the primary key (ON CONFLICT DO UPDATE),
  so re-running a task never creates duplicates.
* All configuration comes from environment variables (12-factor style).

Usage
-----
    python extract_load_postgres.py --entity users
    python extract_load_postgres.py --entity posts
    python extract_load_postgres.py --entity comments
    python extract_load_postgres.py --entity all
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# psycopg2 is used directly for the fast execute_values UPSERT
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | ingestion | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ingestion")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
API_BASE_URL = os.getenv("API_BASE_URL", "https://jsonplaceholder.typicode.com")
API_TIMEOUT = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))


def _pg_dsn() -> dict[str, Any]:
    return dict(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "inkomoko"),
        password=os.getenv("POSTGRES_PASSWORD", "inkomoko_pwd"),
        dbname=os.getenv("POSTGRES_DB", "inkomoko"),
    )


def get_sqlalchemy_engine() -> Engine:
    """SQLAlchemy engine (handy for ad-hoc reads / pandas)."""
    c = _pg_dsn()
    url = (
        f"postgresql+psycopg2://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['dbname']}"
    )
    return create_engine(url, pool_pre_ping=True)


# --------------------------------------------------------------------------- #
# Transform functions: API JSON record -> flat tuple matching the table columns
# --------------------------------------------------------------------------- #
def _ts() -> datetime:
    return datetime.now(tz=timezone.utc)


def transform_user(r: dict[str, Any]) -> tuple:
    addr = r.get("address", {}) or {}
    geo = addr.get("geo", {}) or {}
    company = r.get("company", {}) or {}
    return (
        int(r["id"]),
        r.get("name"),
        r.get("username"),
        (r.get("email") or "").strip().lower() or None,
        r.get("phone"),
        r.get("website"),
        company.get("name"),
        addr.get("city"),
        addr.get("zipcode"),
        float(geo["lat"]) if geo.get("lat") not in (None, "") else None,
        float(geo["lng"]) if geo.get("lng") not in (None, "") else None,
        _ts(),
    )


def transform_post(r: dict[str, Any]) -> tuple:
    return (
        int(r["id"]),
        int(r["userId"]),
        r.get("title"),
        r.get("body"),
        _ts(),
    )


def transform_comment(r: dict[str, Any]) -> tuple:
    return (
        int(r["id"]),
        int(r["postId"]),
        r.get("name"),
        (r.get("email") or "").strip().lower() or None,
        r.get("body"),
        _ts(),
    )


# --------------------------------------------------------------------------- #
# Entity registry: endpoint, target table, columns, conflict key, transform
# --------------------------------------------------------------------------- #
ENTITY_CONFIG: dict[str, dict[str, Any]] = {
    "users": {
        "endpoint": "/users",
        "table": "raw.users",
        "columns": [
            "id", "name", "username", "email", "phone", "website",
            "company_name", "city", "zipcode", "lat", "lng", "ingested_at",
        ],
        "transform": transform_user,
    },
    "posts": {
        "endpoint": "/posts",
        "table": "raw.posts",
        "columns": ["id", "user_id", "title", "body", "ingested_at"],
        "transform": transform_post,
    },
    "comments": {
        "endpoint": "/comments",
        "table": "raw.comments",
        "columns": ["id", "post_id", "name", "email", "body", "ingested_at"],
        "transform": transform_comment,
    },
}


# --------------------------------------------------------------------------- #
# Extract (with retries)
# --------------------------------------------------------------------------- #
@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(API_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def fetch(endpoint: str) -> list[dict[str, Any]]:
    url = f"{API_BASE_URL.rstrip('/')}{endpoint}"
    log.info("GET %s", url)
    resp = requests.get(url, timeout=API_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array from {url}, got {type(data)}")
    log.info("Fetched %d records from %s", len(data), endpoint)
    return data


# --------------------------------------------------------------------------- #
# Load (idempotent UPSERT)
# --------------------------------------------------------------------------- #
def upsert(table: str, columns: list[str], rows: list[tuple]) -> int:
    if not rows:
        log.warning("No rows to load into %s", table)
        return 0

    col_list = ", ".join(columns)
    update_cols = [c for c in columns if c != "id"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES %s "
        f"ON CONFLICT (id) DO UPDATE SET {set_clause}"
    )

    conn = psycopg2.connect(**_pg_dsn())
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        log.info("Upserted %d rows into %s", len(rows), table)
        return len(rows)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Orchestration helpers
# --------------------------------------------------------------------------- #
def run_entity(entity: str) -> int:
    cfg = ENTITY_CONFIG[entity]
    raw_records = fetch(cfg["endpoint"])
    transform: Callable[[dict[str, Any]], tuple] = cfg["transform"]
    rows = [transform(r) for r in raw_records]
    return upsert(cfg["table"], cfg["columns"], rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest JSONPlaceholder -> Postgres")
    parser.add_argument(
        "--entity",
        required=True,
        choices=[*ENTITY_CONFIG.keys(), "all"],
        help="Which entity to ingest",
    )
    args = parser.parse_args()

    entities = list(ENTITY_CONFIG.keys()) if args.entity == "all" else [args.entity]

    total = 0
    for entity in entities:
        log.info("=== Ingesting entity: %s ===", entity)
        try:
            total += run_entity(entity)
        except Exception:
            log.exception("Ingestion FAILED for entity=%s", entity)
            raise

    log.info("Ingestion complete. Total rows upserted: %d", total)


if __name__ == "__main__":
    main()
