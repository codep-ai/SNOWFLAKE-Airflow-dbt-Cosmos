"""
datapai_vn_eod — Vietnam End-of-Day pipeline.
HOSE/HNX close 15:00 ICT. Trigger 15:30 ICT Mon–Fri.
Chain: refresh_daily -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task, BASH_ENV_PREAMBLE, SCRIPTS_DIR

SAIGON_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


@dag(
    dag_id="datapai_vn_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("30 15 * * 1-5", timezone=SAIGON_TZ),
    start_date=datetime(2026, 3, 25, tzinfo=SAIGON_TZ),
    catchup=False,
    tags=["datapai", "vietnam", "eod"],
    max_active_runs=1,
)
def datapai_vn_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_vn", "refresh_ohlcv_daily_vn.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )

    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_vn", "run_compute_fundamental_lite.sh",
        args="--exchange HOSE",
        execution_timeout=timedelta(hours=2),
    )

    ta_daily = datapai_bash_task(
        "compute_ta_daily_vn", "run_compute_ta_daily.sh",
        args="--exchange HOSE",
    )

    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_vn", "run_compute_ta_weekly.sh",
        args="--exchange HOSE",
        execution_timeout=timedelta(minutes=30),
    )

    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_vn", "run_compute_ta_monthly.sh",
        args="--exchange HOSE",
        execution_timeout=timedelta(minutes=30),
    )

    screener_metrics = datapai_bash_task(
        "compute_screener_vn", "run_compute_screener_us.sh",
        args="--exchange HOSE",
        execution_timeout=timedelta(minutes=10),
    )

    refresh_daily >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics


datapai_vn_eod()
