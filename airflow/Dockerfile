# =============================================================================
# Airflow image for the Inkomoko pipeline
# -----------------------------------------------------------------------------
# * Installs the light Python deps the ingestion/replication scripts need
#   directly into the Airflow environment (versions aligned with Airflow 2.10).
# * Installs dbt-clickhouse into an ISOLATED virtualenv (/home/airflow/dbt-venv)
#   so dbt's dependency pins can never conflict with Airflow's. The DAG invokes
#   dbt via the venv's binary: /home/airflow/dbt-venv/bin/dbt
# =============================================================================
ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.4
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow

# --- Python deps used by ingestion + replication scripts -------------------
# (requests / SQLAlchemy / tenacity already ship with Airflow; we pin the rest)
RUN pip install --no-cache-dir \
        "pandas>=2.0,<2.2" \
        "psycopg2-binary>=2.9" \
        "clickhouse-connect>=0.7,<1.0" \
        "python-dotenv>=1.0"

# --- dbt-clickhouse in an isolated virtualenv ------------------------------
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-clickhouse>=1.8,<1.10"
