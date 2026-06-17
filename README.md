# Inkomoko — End-to-End Analytics Engineering Pipeline

An end-to-end ELT pipeline that ingests data from a public REST API, lands it in
**PostgreSQL** (OLTP), replicates it into **ClickHouse** (OLAP), transforms it
with **dbt** (staging → marts with data-quality tests), and orchestrates the
whole flow with **Apache Airflow** — all running under **Docker Compose** and
startable with a single command.

```
JSONPlaceholder API ─▶ Ingestion (Python) ─▶ PostgreSQL ─▶ Replication (Python)
   ─▶ ClickHouse ─▶ dbt staging (views) ─▶ dbt marts (tables) ─▶ dbt tests
                              ▲
                     orchestrated by Airflow
```

See `docs/architecture.png` and `docs/design-report.md` for the full design.

---

## 1. Data source

* **API:** JSONPlaceholder — <https://jsonplaceholder.typicode.com>
* **Authentication:** none. It is a free, fully public fake-REST API. No API
  key, token, or sign-up is required.
* **Endpoints used:** `/users` (10 rows), `/posts` (100 rows), `/comments` (500 rows).

---

## 2. Prerequisites

* Docker Desktop (with Docker Compose v2 — bundled with modern Docker Desktop)
* ~4 GB free RAM for the containers
* Optional, for inspecting data: **DBeaver**
* Optional, for running scripts outside Docker: Python 3.12

Everything else (Python libraries, dbt, Airflow) is installed **inside the
containers** — you do not need to install them on your host.

---

## 3. Quick start (one command)

From the project root (`inkomoko-pipeline/`):

```bash
docker compose up -d --build
```

This builds the Airflow image and starts five services: `postgres`,
`clickhouse`, `airflow-init` (runs once), `airflow-webserver`, and
`airflow-scheduler`.

The DAG `inkomoko_pipeline` is created **unpaused** and on an `@hourly`
schedule, so the scheduler triggers a run automatically within a minute or two
of startup — no manual action required.

**Open the Airflow UI:** <http://localhost:8088> · user `admin` / password
`admin` (configurable in `.env`).

### Trigger a run manually (optional)

```bash
docker compose exec airflow-scheduler airflow dags trigger inkomoko_pipeline
```

or click **Trigger DAG** ▶ in the UI.

---

## 4. What runs, in order

The DAG executes:

1. `ingest_users`, `ingest_posts`, `ingest_comments` — pull from the API and
   UPSERT into PostgreSQL `raw.*` (run in parallel).
2. `replicate_to_clickhouse` — incremental copy PostgreSQL → ClickHouse `raw.*`.
3. `dbt_run` — build staging views and mart tables in ClickHouse `analytics`.
4. `dbt_test` — run not-null / unique / relationship data-quality tests.

All tasks are idempotent (UPSERT + `ReplacingMergeTree`), so retries and reruns
are safe.

---

## 5. Connecting with DBeaver

**PostgreSQL**

| Field    | Value       |
|----------|-------------|
| Host     | `localhost` |
| Port     | `5433`      |
| Database | `inkomoko`  |
| User     | `inkomoko`  |
| Password | `inkomoko_pwd` |

**ClickHouse** (use the HTTP driver)

| Field    | Value            |
|----------|------------------|
| Host     | `localhost`      |
| Port     | `8123`           |
| User     | `default`        |
| Password | `clickhouse_pwd` |
| Databases| `raw`, `analytics`|

---

## 6. Validating each stage

Full query set: `docs/validation_queries.sql`. Quick checks from the terminal:

**PostgreSQL counts** (expect 10 / 100 / 500):

```bash
docker compose exec postgres psql -U inkomoko -d inkomoko -c \
  "SELECT 'users' t, count(*) FROM raw.users
   UNION ALL SELECT 'posts', count(*) FROM raw.posts
   UNION ALL SELECT 'comments', count(*) FROM raw.comments;"
```

**ClickHouse raw counts** (expect 10 / 100 / 500):

```bash
docker compose exec clickhouse clickhouse-client --password clickhouse_pwd -q \
  "SELECT 'users', count() FROM raw.users FINAL
   UNION ALL SELECT 'posts', count() FROM raw.posts FINAL
   UNION ALL SELECT 'comments', count() FROM raw.comments FINAL;"
```

**dbt marts** (expect 10 users, 100 posts):

```bash
docker compose exec clickhouse clickhouse-client --password clickhouse_pwd -q \
  "SELECT count() FROM analytics.mart_user_post_summary;
   SELECT count() FROM analytics.mart_comment_statistics;"
```

---

## 7. Running pieces manually (optional, for debugging)

The ingestion/replication scripts are standalone. Run them inside the Airflow
container so they pick up the right network hostnames:

```bash
docker compose exec airflow-scheduler python /opt/airflow/project/ingestion/extract_load_postgres.py --entity all
docker compose exec airflow-scheduler python /opt/airflow/project/replication/sync_postgres_to_clickhouse.py
docker compose exec airflow-scheduler bash -lc \
  "cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt run  --profiles-dir . --project-dir ."
docker compose exec airflow-scheduler bash -lc \
  "cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt test --profiles-dir . --project-dir ."
```

---

## 8. Shutting down

```bash
docker compose down            # stop containers, keep data volumes
docker compose down -v         # stop AND delete all data (fresh start)
```

---

## 9. Project layout

```
inkomoko-pipeline/
├── docker-compose.yml          # full stack definition
├── .env                        # local credentials/config (not for production)
├── .env.example                # template with secrets redacted
├── README.md
├── docs/
│   ├── architecture.png        # architecture diagram
│   ├── design-report.md        # design, OLTP vs OLAP, DB choices, scaling
│   └── validation_queries.sql  # stage-by-stage validation SQL
├── postgres/
│   └── init.sql                # schema + tables + airflow DB + watermark
├── clickhouse/
│   └── init/01_init.sql         # raw landing tables (ReplacingMergeTree)
├── ingestion/
│   ├── extract_load_postgres.py # API → Postgres (retries, logging, UPSERT)
│   └── requirements.txt
├── replication/
│   ├── sync_postgres_to_clickhouse.py # Postgres → ClickHouse (incremental)
│   └── requirements.txt
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/  (stg_users, stg_posts, stg_comments + sources + tests)
│       └── marts/    (mart_user_post_summary, mart_comment_statistics + tests)
└── airflow/
    ├── Dockerfile              
    └── dags/
        └── pipeline_dag.py
```

---

## 10. Troubleshooting

| Symptom                                    |Cause / Fix                    |
|--------------------------------------------|-------------------------------|
| `docker compose up` fails pulling an image | Your locally-pulled image tags may differ. Edit the tags in `docker-compose.yml` (`postgres:16`, `clickhouse/clickhouse-server:24.8`) and `AIRFLOW_IMAGE_NAME` in `.env` to match `docker images`. |
| Airflow UI not reachable on 8080 | Give it 1–2 minutes after `up`. Check `docker compose logs airflow-webserver`. Ensure port 8080 is free. |
| Tasks fail connecting to DB | Confirm `postgres` and `clickhouse` are **healthy**: `docker compose ps`. The Airflow services wait for health, but a slow first boot can need a retry (the DAG retries automatically). |
| ClickHouse auth error from dbt/replication | Password mismatch. The value must be identical in `.env` (`CLICKHOUSE_PASSWORD`) and however you connect in DBeaver. |
| Want a clean slate | `docker compose down -v` then `docker compose up -d --build`. This wipes volumes and re-runs all init scripts. |
| Port already in use (5432/8123/8080) | Stop the conflicting local service or remap the left-hand port in `docker-compose.yml`, e.g. `"15432:5432"`. |
| dbt `relation raw.* does not exist` | Replication hasn't run yet. Run the full DAG (or the manual replication command in §7) before dbt. |

---

## 11. Notes on credentials

I've intentionally included the .env file in the repository so the project runs with the single command above, with no extra environment setup required. The credentials in it are local development defaults only.
