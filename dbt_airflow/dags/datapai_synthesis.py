"""
datapai_synthesis — AG2 multi-agent stock signal synthesis.
Runs each stock as a separate Airflow task for better monitoring and retry.
ASX: 18:00 AEDT Mon-Fri. US: 18:00 ET Mon-Fri.
Max 2 stocks at a time (pool limit) to avoid LLM rate limits.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")
NY_TZ = pendulum.timezone("America/New_York")

# Stock universes — same as lib/universe.ts
ASX_TICKERS = [
    "BHP", "CBA", "CSL", "NAB", "ANZ", "WBC", "WES", "MQG",
    "TLS", "WOW", "RIO", "FMG", "TWE", "GMG", "STO", "ORG", "WDS", "QAN",
]

US_TICKERS = [
    "ACMR", "AEHR", "ATRC", "CRVL", "ERII", "FLNC", "GATO", "HIMS",
    "IIIV", "KTOS", "LBRT", "MARA", "MGNI", "MNDY", "NOVA", "NTST",
    "PHAT", "PRTS", "SHYF", "TMDX",
]


@dag(
    dag_id="datapai_synthesis_asx",
    default_args={
        **DEFAULT_ARGS,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
        "pool": "synthesis_pool",
    },
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 3, 18, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "asx", "ag2", "synthesis"],
    max_active_runs=1,
    max_active_tasks=2,
)
def datapai_synthesis_asx():
    for ticker in ASX_TICKERS:
        datapai_bash_task(
            task_id=f"synth_{ticker.replace('.', '_')}",
            script_name="run_stock_synthesis.sh",
            args=f"--exchange ASX --tickers {ticker}",
        )

datapai_synthesis_asx()


@dag(
    dag_id="datapai_synthesis_us",
    default_args={
        **DEFAULT_ARGS,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
        "pool": "synthesis_pool",
    },
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 3, 18, tzinfo=NY_TZ),
    catchup=False,
    tags=["datapai", "us", "ag2", "synthesis"],
    max_active_runs=1,
    max_active_tasks=2,
)
def datapai_synthesis_us():
    for ticker in US_TICKERS:
        datapai_bash_task(
            task_id=f"synth_{ticker}",
            script_name="run_stock_synthesis.sh",
            args=f"--exchange US --tickers {ticker}",
        )

datapai_synthesis_us()
