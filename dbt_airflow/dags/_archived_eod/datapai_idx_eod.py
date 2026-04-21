"""
datapai_idx_eod — Indonesia End-of-Day pipeline.
IDX closes 16:15 WIB. Trigger 16:45 WIB Mon–Fri.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

JKT_TZ = pendulum.timezone("Asia/Jakarta")

@dag(
    dag_id="datapai_idx_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("45 16 * * 1-5", timezone=JKT_TZ),
    start_date=datetime(2026, 3, 25, tzinfo=JKT_TZ),
    catchup=False,
    tags=["datapai", "indonesia", "eod"],
    max_active_runs=1,
)
def datapai_idx_eod():
    refresh_daily = datapai_bash_task("refresh_daily_idx", "refresh_ohlcv_daily_idx.sh", execution_timeout=timedelta(hours=2), retries=2)
    fundamental_lite = datapai_bash_task("compute_fundamental_lite_idx", "run_compute_fundamental_lite.sh", args="--exchange IDX", execution_timeout=timedelta(hours=2))
    ta_daily = datapai_bash_task("compute_ta_daily_idx", "run_compute_ta_daily.sh", args="--exchange IDX")
    ta_weekly = datapai_bash_task("compute_ta_weekly_idx", "run_compute_ta_weekly.sh", args="--exchange IDX", execution_timeout=timedelta(minutes=30))
    ta_monthly = datapai_bash_task("compute_ta_monthly_idx", "run_compute_ta_monthly.sh", args="--exchange IDX", execution_timeout=timedelta(minutes=30))
    screener_metrics = datapai_bash_task("compute_screener_idx", "run_compute_screener_us.sh", args="--exchange IDX", execution_timeout=timedelta(minutes=10))
    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics

datapai_idx_eod()
