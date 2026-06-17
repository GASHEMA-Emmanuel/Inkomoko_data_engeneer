"""
pipeline_dag.py
===============
Orchestrates the full Inkomoko analytics pipeline:

    ingest_users ─┐
    ingest_posts ─┼─> replicate_to_clickhouse ─> dbt_run ─> dbt_test
    ingest_comments ┘

Airflow best practices applied here:
* default_args with retries + exponential retry_delay
* a failure callback that logs a clear alert line (swap for Slack/email/PagerDuty)
* explicit task dependencies
* catchup disabled, single active run, sensible tags
* idempotent tasks (UPSERT + ReplacingMergeTree) so retries are safe

The scripts and dbt project are mounted into the Airflow containers at
/opt/airflow/project (see docker-compose.yml volumes).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# Paths inside the Airflow container (mounted from the repo)
PROJECT_DIR = "/opt/airflow/project"
INGESTION = f"{PROJECT_DIR}/ingestion/extract_load_postgres.py"
REPLICATION = f"{PROJECT_DIR}/replication/sync_postgres_to_clickhouse.py"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"


def alert_on_failure(context: dict) -> None:
    """Minimal alerting hook. Replace the body with Slack/email/PagerDuty."""
    ti = context.get("task_instance")
    log.error(
        "ALERT: task=%s dag=%s execution_date=%s failed",
        getattr(ti, "task_id", "?"),
        getattr(ti, "dag_id", "?"),
        context.get("logical_date") or context.get("execution_date"),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="inkomoko_pipeline",
    description="JSONPlaceholder -> Postgres -> ClickHouse -> dbt (staging+marts)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",          # auto-runs the latest interval shortly after startup
    catchup=False,               # do not backfill historical intervals
    max_active_runs=1,
    is_paused_upon_creation=False,  # so `docker compose up` -> pipeline runs itself
    tags=["inkomoko", "analytics-engineering", "elt"],
) as dag:

    # --- 1-3. Ingestion (API -> Postgres) ---------------------------------
    ingest_users = BashOperator(
        task_id="ingest_users",
        bash_command=f"python {INGESTION} --entity users",
    )
    ingest_posts = BashOperator(
        task_id="ingest_posts",
        bash_command=f"python {INGESTION} --entity posts",
    )
    ingest_comments = BashOperator(
        task_id="ingest_comments",
        bash_command=f"python {INGESTION} --entity comments",
    )

    # --- 4. Replication (Postgres -> ClickHouse) --------------------------
    replicate_to_clickhouse = BashOperator(
        task_id="replicate_to_clickhouse",
        bash_command=f"python {REPLICATION}",
    )

    # --- 5. dbt run (staging + marts in ClickHouse) -----------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} run "
            f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"
        ),
    )

    # --- 6. dbt test (data quality) ---------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} test "
            f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"
        ),
    )

    # --- Dependencies ------------------------------------------------------
    [ingest_users, ingest_posts, ingest_comments] >> replicate_to_clickhouse
    replicate_to_clickhouse >> dbt_run >> dbt_test
