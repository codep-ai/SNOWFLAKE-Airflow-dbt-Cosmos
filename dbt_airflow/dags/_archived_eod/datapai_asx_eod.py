"""
datapai_asx_eod — ASX End-of-Day pipeline.
ASX closes 16:00 AEDT. Trigger 16:15 AEDT Mon-Fri.
Chain: daily_refresh -> eod_rollup -> fundamental_lite -> ta_daily -> ta_weekly -> ta_monthly -> screener_metrics -> fundamental -> multi_factor
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task, BASH_ENV_PREAMBLE, SCRIPTS_DIR

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

@dag(
    dag_id="datapai_asx_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("15 16 * * 1-5", timezone=SYDNEY_TZ),
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "asx", "eod"],
    max_active_runs=1,
)
def datapai_asx_eod():
    refresh_daily = datapai_bash_task(
        "refresh_daily_asx", "refresh_ohlcv_daily_asx.sh",
        execution_timeout=timedelta(hours=2), retries=2,
    )
    eod_rollup = datapai_bash_task(
        "eod_rollup_asx", "run_eod_rollup.sh", args="--exchange ASX",
        execution_timeout=timedelta(minutes=30),
    )
    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_asx", "run_compute_fundamental_lite.sh", args="--exchange ASX",
        execution_timeout=timedelta(hours=2),
    )
    ta_daily = datapai_bash_task(
        "compute_ta_daily_asx", "run_compute_ta_daily.sh", args="--exchange ASX",
    )
    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_asx", "run_compute_ta_weekly.sh", args="--exchange ASX",
        execution_timeout=timedelta(minutes=30),
    )
    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_asx", "run_compute_ta_monthly.sh", args="--exchange ASX",
        execution_timeout=timedelta(minutes=30),
    )
    screener_metrics = datapai_bash_task(
        "compute_screener_asx", "run_compute_screener_asx.sh",
        execution_timeout=timedelta(minutes=10),
    )
    fundamental = datapai_bash_task(
        "compute_fundamental_asx", "run_compute_fundamental.sh", args="--exchange ASX",
        execution_timeout=timedelta(hours=2),
    )
    multi_factor = datapai_bash_task(
        "compute_multi_factor_asx", "run_compute_multi_factor.sh", args="--exchange ASX",
        execution_timeout=timedelta(minutes=30),
    )

    refresh_daily >> eod_rollup >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics >> fundamental >> multi_factor

datapai_asx_eod()
