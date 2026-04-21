"""datapai_monthly_ta — Monthly TA indicators (1st of month 12:00 AEDT)."""
from datetime import datetime
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

@dag(
    dag_id="datapai_monthly_ta",
    default_args=DEFAULT_ARGS,
    schedule="0 12 1 * *",  # 1st of month 12:00 AEDT
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "monthly"],
)
def datapai_monthly_ta():
    us = datapai_bash_task("ta_monthly_us", "run_compute_ta_monthly.sh", args="--exchange US")
    asx = datapai_bash_task("ta_monthly_asx", "run_compute_ta_monthly.sh", args="--exchange ASX")
    us >> asx

datapai_monthly_ta()
