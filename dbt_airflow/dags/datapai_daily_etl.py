"""
datapai_daily_etl — Postgres -> S3 -> Snowflake (split by exchange).
ASX ETL: 18:30 AEDT Mon-Fri (after ASX EOD at 16:15 + synthesis at 18:00).
US ETL:  18:30 ET Mon-Fri (after US EOD at 16:30 + synthesis at 18:00).
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")
NY_TZ = pendulum.timezone("America/New_York")

@dag(
    dag_id="datapai_etl_asx",
    default_args=DEFAULT_ARGS,
    schedule="30 18 * * 1-5",  # 18:30 AEDT Mon-Fri
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "asx", "etl", "snowflake"],
    max_active_runs=1,
)
def datapai_etl_asx():
    datapai_bash_task(
        "etl_asx",
        "run_daily_stock_etl.sh",
        args="--exchanges ASX --days 3",
        execution_timeout=timedelta(hours=1),
    )

datapai_etl_asx()

@dag(
    dag_id="datapai_etl_us",
    default_args=DEFAULT_ARGS,
    schedule="30 18 * * 1-5",  # 18:30 ET Mon-Fri
    start_date=datetime(2026, 3, 18, tzinfo=NY_TZ),
    catchup=False,
    tags=["datapai", "us", "etl", "snowflake"],
    max_active_runs=1,
)
def datapai_etl_us():
    datapai_bash_task(
        "etl_us",
        "run_daily_stock_etl.sh",
        args="--exchanges US --days 3",
        execution_timeout=timedelta(hours=1),
    )

datapai_etl_us()
