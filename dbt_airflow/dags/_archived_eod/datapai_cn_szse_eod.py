"""
datapai_cn_szse_eod — Shenzhen Stock Exchange End-of-Day pipeline.
SZSE closes 15:00 CST. Trigger 15:45 CST Mon-Fri (staggered from SSE).
Chain: refresh_daily -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

CN_TZ = pendulum.timezone("Asia/Shanghai")


@dag(
    dag_id="datapai_cn_szse_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("45 15 * * 1-5", timezone=CN_TZ),
    start_date=datetime(2026, 3, 28, tzinfo=CN_TZ),
    catchup=False,
    tags=["datapai", "china", "szse", "eod"],
    max_active_runs=1,
)
def datapai_cn_szse_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_szse", "refresh_ohlcv_daily_szse.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )

    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_szse", "run_compute_fundamental_lite.sh",
        args="--exchange SZSE",
        execution_timeout=timedelta(hours=2),
    )

    ta_daily = datapai_bash_task(
        "compute_ta_daily_szse", "run_compute_ta_daily.sh",
        args="--exchange SZSE",
    )

    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_szse", "run_compute_ta_weekly.sh",
        args="--exchange SZSE",
        execution_timeout=timedelta(minutes=30),
    )

    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_szse", "run_compute_ta_monthly.sh",
        args="--exchange SZSE",
        execution_timeout=timedelta(minutes=30),
    )

    screener_metrics = datapai_bash_task(
        "compute_screener_szse", "run_compute_screener_us.sh",
        args="--exchange SZSE",
        execution_timeout=timedelta(minutes=10),
    )

    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics


datapai_cn_szse_eod()
