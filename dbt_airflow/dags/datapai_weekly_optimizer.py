"""
datapai_weekly_optimizer — Weekly signal optimization (walk-forward validation).
Runs Sunday 14:00 AEDT when both markets are closed.
Grid-searches threshold parameters and saves best config to signal_params.json.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

@dag(
    dag_id="datapai_weekly_optimizer",
    default_args={**DEFAULT_ARGS, "retries": 1},
    schedule="0 14 * * 0",  # Sunday 14:00 AEDT
    start_date=datetime(2026, 3, 22, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "weekly", "optimizer"],
    max_active_runs=1,
)
def datapai_weekly_optimizer():
    optimize_us = datapai_bash_task(
        "optimize_signals_us", "run_optimize_signals.sh",
        args="--exchange US --sample 200",
        execution_timeout=timedelta(hours=2),
    )
    optimize_asx = datapai_bash_task(
        "optimize_signals_asx", "run_optimize_signals.sh",
        args="--exchange ASX --sample 100",
        execution_timeout=timedelta(hours=1),
    )
    # Run US first (larger dataset), then ASX
    optimize_us >> optimize_asx

datapai_weekly_optimizer()
