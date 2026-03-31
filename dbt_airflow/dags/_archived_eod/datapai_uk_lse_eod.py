"""
datapai_uk_lse_eod — LSE End-of-Day pipeline.
LSE closes 16:30 GMT/BST. Trigger 17:00 London time Mon-Fri.
Chain: refresh_daily -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

LONDON_TZ = pendulum.timezone("Europe/London")


@dag(
    dag_id="datapai_uk_lse_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("0 17 * * 1-5", timezone=LONDON_TZ),
    start_date=datetime(2026, 3, 28, tzinfo=LONDON_TZ),
    catchup=False,
    tags=["datapai", "uk", "lse", "eod"],
    max_active_runs=1,
)
def datapai_uk_lse_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_lse", "refresh_ohlcv_daily_lse.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )
    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_lse", "run_compute_fundamental_lite.sh",
        args="--exchange LSE",
        execution_timeout=timedelta(hours=2),
    )
    ta_daily = datapai_bash_task(
        "compute_ta_daily_lse", "run_compute_ta_daily.sh",
        args="--exchange LSE",
    )
    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_lse", "run_compute_ta_weekly.sh",
        args="--exchange LSE",
        execution_timeout=timedelta(minutes=30),
    )
    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_lse", "run_compute_ta_monthly.sh",
        args="--exchange LSE",
        execution_timeout=timedelta(minutes=30),
    )
    screener_metrics = datapai_bash_task(
        "compute_screener_lse", "run_compute_screener_us.sh",
        args="--exchange LSE",
        execution_timeout=timedelta(minutes=10),
    )

    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics


datapai_uk_lse_eod()
