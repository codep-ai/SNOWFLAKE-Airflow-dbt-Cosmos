"""datapai_weekly_ta — Weekly TA indicators (Sunday 12:00 AEDT)."""
from datetime import datetime
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

@dag(
    dag_id="datapai_weekly_ta",
    default_args=DEFAULT_ARGS,
    schedule="0 12 * * 0",  # Sunday 12:00 AEDT
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "weekly"],
)
def datapai_weekly_ta():
    us = datapai_bash_task("ta_weekly_us", "run_compute_ta_weekly.sh", args="--exchange US")
    asx = datapai_bash_task("ta_weekly_asx", "run_compute_ta_weekly.sh", args="--exchange ASX")
    us >> asx

datapai_weekly_ta()
