"""
datapai_us_eod — US End-of-Day pipeline.
NYSE closes 16:00 ET. Trigger 16:30 ET Mon-Fri.
Chain: eod_rollup -> 3x parallel refresh -> fundamental_lite -> ta_daily -> ta_weekly + ta_monthly -> screener_metrics -> fundamental -> multi_factor
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.timetables.trigger import CronTriggerTimetable
from datapai_common import DEFAULT_ARGS, datapai_bash_task

NY_TZ = pendulum.timezone("America/New_York")

_NUM_REFRESH_WORKERS = 3


@dag(
    dag_id="datapai_us_eod",
    default_args=DEFAULT_ARGS,
    timetable=CronTriggerTimetable("30 16 * * 1-5", timezone=NY_TZ),
    start_date=datetime(2026, 3, 18, tzinfo=NY_TZ),
    catchup=False,
    tags=["datapai", "us", "eod"],
    max_active_runs=1,
)
def datapai_us_eod():
    eod_rollup = datapai_bash_task(
        "eod_rollup_us", "run_eod_rollup.sh", args="--exchange US",
        execution_timeout=timedelta(minutes=30),
    )

    # 3 parallel refresh workers — each handles ~2200 tickers
    # yfinance batches of 500, ~4-5 min each, 5 sub-batches = ~25 min
    refresh_workers = []
    for i in range(_NUM_REFRESH_WORKERS):
        worker = datapai_bash_task(
            f"refresh_us_batch_{i}",
            "refresh_ohlcv_daily_us.sh",
            args=f"--batch-num {i} --total-batches {_NUM_REFRESH_WORKERS}",
            execution_timeout=timedelta(minutes=45),
            retries=2,
        )
        refresh_workers.append(worker)

    fundamental_lite = datapai_bash_task(
        "compute_fundamental_lite_us", "run_compute_fundamental_lite.sh", args="--exchange US",
        execution_timeout=timedelta(hours=2),
    )
    ta_daily = datapai_bash_task(
        "compute_ta_daily_us", "run_compute_ta_daily.sh", args="--exchange US",
    )
    ta_weekly = datapai_bash_task(
        "compute_ta_weekly_us", "run_compute_ta_weekly.sh", args="--exchange US",
        execution_timeout=timedelta(minutes=30),
    )
    ta_monthly = datapai_bash_task(
        "compute_ta_monthly_us", "run_compute_ta_monthly.sh", args="--exchange US",
        execution_timeout=timedelta(minutes=30),
    )
    screener_metrics = datapai_bash_task(
        "compute_screener_us", "run_compute_screener_us.sh",
        execution_timeout=timedelta(minutes=10),
    )
    fundamental = datapai_bash_task(
        "compute_fundamental_us", "run_compute_fundamental.sh", args="--exchange US",
        execution_timeout=timedelta(hours=2),
    )
    multi_factor = datapai_bash_task(
        "compute_multi_factor_us", "run_compute_multi_factor.sh", args="--exchange US",
        execution_timeout=timedelta(minutes=30),
    )

    eod_rollup >> refresh_workers >> fundamental_lite >> ta_daily >> [ta_weekly, ta_monthly] >> screener_metrics >> fundamental >> multi_factor

datapai_us_eod()
