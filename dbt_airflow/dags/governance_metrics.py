"""governance_metrics — Cosmos DAG for datapai-dbt-governance.

Runs the dbt governance project (dbt 1.9.1 + MetricFlow + DuckDB default
target) as an Airflow DAG. Each dbt model becomes a task; Cosmos derives
dependencies from the dbt manifest automatically.

Schedule: @daily (aligned with Postgres → S3 Iceberg ETL that lands
fresh fct_ai_guardrail_decision_flat rows).

Expected task graph (auto-derived from ref() calls):

    time_spine ────────────┐
                           ├─▶  stg_guardrail_decision  ─▶ (metrics)
    [source: S3 Iceberg] ──┘

Pre-requisite:
  - datapai-dbt-governance repo copied into dags/dbt/datapai-dbt-governance
    (symlink or git submodule — see README)
  - Python 3.11 dbt_venv_governance built via Dockerfile
  - Airflow connection for DuckDB (or reuse existing Snowflake conn for
    prod target)
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import Dataset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping

DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv_governance/bin/dbt"
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/datapai-dbt-governance")

# DuckDB profile — uses an Airflow connection for any S3 / iceberg creds.
# For the DuckDB target, the "database" is just the path to the .duckdb file
# in the Airflow worker's local storage; extensions are declared in
# profiles.yml in the dbt project itself.
profile_config = ProfileConfig(
    profile_name="datapai_governance",
    target_name="duckdb",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_governance",   # Airflow conn; see README for setup
        profile_args={
            "path": "/usr/local/airflow/data/datapai_governance.duckdb",
        },
    ),
)

governance_metrics = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    operator_args={
        "install_deps": True,          # dbt deps on each run
        "full_refresh": False,
    },
    default_args={
        "owner": "datapai",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    # Auto-trigger when the upstream ETL lands fresh Iceberg data.
    # Falls back to daily cadence as a safety net.
    schedule=[
        Dataset("s3://datapai-governance/iceberg/fct_ai_guardrail_decision_flat/"),
    ],
    start_date=datetime(2026, 4, 21),
    catchup=False,
    tags=["datapai", "governance", "dbt", "cosmos", "metricflow"],
    dag_id="governance_metrics",
)
