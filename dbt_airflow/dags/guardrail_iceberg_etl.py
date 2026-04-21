"""guardrail_iceberg_etl — Airflow wrapper around the scheduler-agnostic
Python ETL script.

Why a thin wrapper (vs pure PythonOperator code):
  The ETL body lives in datapai-platform-be/scripts/etl_guardrail_to_iceberg.py
  so it can be invoked from Control-M / cron / CI with zero Airflow-specific
  deps. This DAG just calls the same script via BashOperator.

Schedule: @hourly. Each run reads incrementally from the watermark stored
in framework_db.sys_common_config — safe under overlap / catch-up.

Emits an Airflow Dataset on success so the downstream `governance_metrics`
Cosmos DAG auto-triggers when fresh data lands.
"""
from datetime import datetime, timedelta

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Dataset the downstream Cosmos DAG listens on.
guardrail_iceberg_dataset = Dataset(
    "s3://datapai-governance/iceberg/fct_ai_guardrail_decision_flat/"
)


@dag(
    dag_id="guardrail_iceberg_etl",
    description="Postgres fct_ai_guardrail_decision → S3 Iceberg flatten-and-load",
    schedule="@hourly",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datapai",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["datapai", "governance", "etl", "iceberg"],
)
def guardrail_iceberg_etl():
    BashOperator(
        task_id="sync_postgres_to_iceberg",
        # Same script Control-M and cron can call.
        bash_command=(
            "cd /usr/local/airflow/datapai-platform-be && "
            "python3 scripts/etl_guardrail_to_iceberg.py"
        ),
        outlets=[guardrail_iceberg_dataset],
    )


guardrail_iceberg_etl()
