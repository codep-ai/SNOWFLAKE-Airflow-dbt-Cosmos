"""ai_framework_refresh — Daily TinyFish auto-refresh of AI governance frameworks.

Pipeline (all in /opt/datapai inside the airflow container, mounted from
/home/ec2-user/git/datapai-platform-be):
  1. run_ai_framework_refresh.py — G3 cadence-driven; HTML / PDF / via pdf_url
  2. run_browserless_ingest.py   — fallback for JS-rendered SPAs (Drupal etc.)

Schedule: daily 03:00 AEDT.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

DEFAULT_ARGS = {
    "owner": "datapai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="ai_framework_refresh",
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *",
    start_date=datetime(2026, 4, 21, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "governance", "tinyfish"],
    max_active_runs=1,
)
def ai_framework_refresh():
    BashOperator(
        task_id="refresh_due_frameworks",
        bash_command=(
            # Use .format() friendly (no  in template) — single-quote heredoc
            "bash -c '"
            "set +u; "
            "mkdir -p /tmp/airflow_home/ai_framework_refresh/.cache; "
            "export HOME=/tmp/airflow_home/ai_framework_refresh; "
            "set -a; [[ -f /opt/datapai/.env.dev ]] && . /opt/datapai/.env.dev || true; set +a; "
            # Override after sourcing — framework_db on 5433
            "export FRAMEWORK_DB_HOST=localhost FRAMEWORK_DB_PORT=5433 "
            "FRAMEWORK_DB_USER=postgres FRAMEWORK_DB_PASSWORD=auth_root_2026 "
            "FRAMEWORK_DB_NAME=datapai_auth_db; "
            "export PGHOST=localhost PGPORT=5433 PGUSER=postgres "
            "PGPASSWORD=auth_root_2026 PGDATABASE=datapai_auth_db; "
            "export AI_SOURCE_S3_BUCKET=codepais3 "
            "AI_SOURCE_S3_PREFIX=ai_governance/source_docs "
            "LANCEDB_URI=s3://codepais3/lancedb_data/; "
            "cd /opt/datapai && "
            "/home/airflow/.local/bin/python3 scripts/run_ai_framework_refresh.py --batch 30 && "
            "/home/airflow/.local/bin/python3 scripts/run_browserless_ingest.py "
            "'"
        ),
    )


ai_framework_refresh()
