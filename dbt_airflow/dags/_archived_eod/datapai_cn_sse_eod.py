"""
datapai_cn_sse_eod — Shanghai Stock Exchange End-of-Day pipeline.
SSE closes 15:00 CST. Trigger 15:30 CST Mon-Fri.
Chain: refresh_daily -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

CN_TZ = pendulum.timezone("Asia/Shanghai")


@dag(
    dag_id="datapai_cn_sse_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("30 15 * * 1-5", timezone=CN_TZ),
    start_date=datetime(2026, 3, 28, tzinfo=CN_TZ),
    catchup=False,
    tags=["datapai", "china", "sse", "eod"],
    max_active_runs=1,
)
def datapai_cn_sse_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_sse", "refresh_ohlcv_daily_sse.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )

    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_sse", "run_compute_fundamental_lite.sh",
        args="--exchange SSE",
        execution_timeout=timedelta(hours=2),
    )

    ta_daily = datapai_bash_task(
        "compute_ta_daily_sse", "run_compute_ta_daily.sh",
        args="--exchange SSE",
    )

    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_sse", "run_compute_ta_weekly.sh",
        args="--exchange SSE",
        execution_timeout=timedelta(minutes=30),
    )

    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_sse", "run_compute_ta_monthly.sh",
        args="--exchange SSE",
        execution_timeout=timedelta(minutes=30),
    )

    screener_metrics = datapai_bash_task(
        "compute_screener_sse", "run_compute_screener_us.sh",
        args="--exchange SSE",
        execution_timeout=timedelta(minutes=10),
    )

    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics


datapai_cn_sse_eod()
