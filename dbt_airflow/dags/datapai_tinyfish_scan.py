"""
datapai_tinyfish_scan — TinyFish IR page scans.
ASX: 17:30 AEDT Mon-Fri (after ASX EOD pipeline).
US: 17:30 ET Mon-Fri (after US EOD pipeline).
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datapai_common import DEFAULT_ARGS

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")
NY_TZ = pendulum.timezone("America/New_York")

@dag(
    dag_id="datapai_tinyfish_scan_asx",
    default_args=DEFAULT_ARGS,
    schedule="30 17 * * 1-5",  # 17:30 AEDT Mon-Fri
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "tinyfish", "asx"],
    max_active_runs=1,
)
def datapai_tinyfish_scan_asx():
    BashOperator(
        task_id="tinyfish_scan_asx",
        bash_command="curl -sf -X POST http://localhost:3085/api/run?exchange=ASX",
        execution_timeout=timedelta(hours=2),
    )

datapai_tinyfish_scan_asx()

@dag(
    dag_id="datapai_tinyfish_scan_us",
    default_args=DEFAULT_ARGS,
    schedule="30 17 * * 1-5",  # 17:30 ET Mon-Fri
    start_date=datetime(2026, 3, 18, tzinfo=NY_TZ),
    catchup=False,
    tags=["datapai", "tinyfish", "us"],
    max_active_runs=1,
)
def datapai_tinyfish_scan_us():
    BashOperator(
        task_id="tinyfish_scan_us",
        bash_command="curl -sf -X POST http://localhost:3085/api/run?exchange=US",
        execution_timeout=timedelta(hours=2),
    )

datapai_tinyfish_scan_us()
