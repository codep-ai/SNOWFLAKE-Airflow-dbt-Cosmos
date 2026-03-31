"""
datapai_set_eod — Thailand End-of-Day pipeline.
SET closes 16:30 ICT. Trigger 17:00 ICT Mon–Fri.
Chain: refresh_daily -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

BKK_TZ = pendulum.timezone("Asia/Bangkok")


@dag(
    dag_id="datapai_set_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("0 17 * * 1-5", timezone=BKK_TZ),
    start_date=datetime(2026, 3, 25, tzinfo=BKK_TZ),
    catchup=False,
    tags=["datapai", "thailand", "eod"],
    max_active_runs=1,
)
def datapai_set_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_set", "refresh_ohlcv_daily_set.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )
    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_set", "run_compute_fundamental_lite.sh",
        args="--exchange SET",
        execution_timeout=timedelta(hours=2),
    )
    ta_daily = datapai_bash_task(
        "compute_ta_daily_set", "run_compute_ta_daily.sh",
        args="--exchange SET",
    )
    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_set", "run_compute_ta_weekly.sh",
        args="--exchange SET",
        execution_timeout=timedelta(minutes=30),
    )
    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_set", "run_compute_ta_monthly.sh",
        args="--exchange SET",
        execution_timeout=timedelta(minutes=30),
    )
    screener_metrics = datapai_bash_task(
        "compute_screener_set", "run_compute_screener_us.sh",
        args="--exchange SET",
        execution_timeout=timedelta(minutes=10),
    )

    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics


datapai_set_eod()
