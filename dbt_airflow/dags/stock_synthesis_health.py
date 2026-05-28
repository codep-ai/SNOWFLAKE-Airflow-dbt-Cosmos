"""
stock_synthesis_health — nightly health monitor for the synthesis pipeline.

Runs at 23:00 UTC (one hour after US synthesis finishes at 22:00). Reads
tonight's batch from datapai.stock_synthesis, computes 6 health metrics,
and FAILS the task if any threshold is tripped — Airflow then emails per
DEFAULT_ARGS alert config.

PROTECTS AGAINST
────────────────
the exact failure mode that ran silently for 2 months (March–May 2026):
  - AG2 path broken, every row falling to hardcoded HOLD/0.3/LOW default
  - DAG green, /performance dashboard worthless, no human ever pinged

ALERTING
────────
- run_synthesis_health.py exits non-zero on red status → Airflow marks
  task FAILED → email per DEFAULT_ARGS (or configured Slack/Telegram).
- Every run also writes a row to datapai.synthesis_health_runs for
  trend analysis. The "broken-fallback signature" alert would have
  fired on day 1 of the 2-month bug.

Path-safe: uses datapai_bash_task. No host-path imports.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

UTC = pendulum.timezone("UTC")


@dag(
    dag_id="stock_synthesis_health",
    default_args={
        **DEFAULT_ARGS,
        "retries": 0,  # don't retry — we WANT this to surface failures
        "execution_timeout": timedelta(minutes=10),
        # email_on_failure inherited from DEFAULT_ARGS
    },
    schedule="0 23 * * *",          # daily 23:00 UTC (1h after US synthesis)
    start_date=datetime(2026, 5, 28, tzinfo=UTC),
    catchup=False,
    tags=["datapai", "monitoring", "synthesis", "health"],
    max_active_runs=1,
)
def stock_synthesis_health():
    datapai_bash_task(
        task_id="health_check",
        script_name="run_synthesis_health.sh",
        args="--window-hours 24",
    )


stock_synthesis_health()
