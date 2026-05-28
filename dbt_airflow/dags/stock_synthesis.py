"""
stock_synthesis — AG2 multi-agent stock signal synthesis.

DYNAMIC UNIVERSE (2026-05-24, fixed 2026-05-28): resolves the ticker list
at DAG-parse time by querying datapai.market_demo_stocks (landing-page
top-20 per exchange) UNION datapai.watchlist (all user-watchlisted
tickers), filtered to those with fundamental_lite data.

⚠ Lesson learned (2026-05-28): the first version of this DAG tried to
import from /home/ec2-user/git/datapai-stock-be which does not exist
inside the Airflow container's filesystem (the repo is mounted at
/opt/datapai-stock there, but that container path is irrelevant — the
right move is to query the DB directly from inside the DAG). That broken
import caused 6 days of zero-task DAG runs that completed silently.

This version:
  - Queries stock_db directly via 127.0.0.1:5434 (Airflow uses host network)
  - Falls back to a known-good hardcoded list if SQL fails — DAG never
    silently produces zero tasks again.

ASX: 18:00 AEDT Mon-Fri. US: 18:00 ET Mon-Fri. Max 2 tasks at a time.
"""
from datetime import datetime, timedelta
import logging
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")
NY_TZ = pendulum.timezone("America/New_York")

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Hardcoded fallback — used only if the DB query fails at DAG parse time.
# Keep this list a known-good baseline so the DAG never produces a
# zero-task run silently (the failure mode we hit 2026-05-24..28).
# ──────────────────────────────────────────────────────────────────────────
_FALLBACK_ASX = [
    "BHP", "CBA", "CSL", "NAB", "ANZ", "WBC", "WES", "MQG",
    "TLS", "WOW", "RIO", "FMG", "TWE", "GMG", "STO", "ORG", "WDS", "QAN",
]
_FALLBACK_US = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "V", "JNJ", "WMT", "XOM", "UNH", "HD", "MA",
    "BAC", "PFE", "AVGO", "CRM", "KO",
]


def _resolve_universe(exchange: str) -> list[str]:
    """
    Pull the dynamic synthesis universe for `exchange` from stock_db.

    Universe = (top-20 landing-page stocks per exchange) ∪ (all user
    watchlist symbols), filtered to those with fundamental_lite data so
    AG2 has signals to debate. NASDAQ/NYSE rows are coerced to 'US' so
    they match the analysis tables.

    Returns a hardcoded fallback list on any error so the DAG always
    schedules tasks (no more silent zero-task runs).
    """
    try:
        import psycopg2
    except ImportError:
        log.warning("[%s] psycopg2 not in container — using fallback", exchange)
        return _FALLBACK_ASX if exchange == "ASX" else _FALLBACK_US

    sql = """
        WITH wanted AS (
            SELECT ticker,
                   CASE WHEN exchange IN ('NASDAQ','NYSE') THEN 'US' ELSE exchange END AS exchange
            FROM datapai.market_demo_stocks
            WHERE display_order <= 20
            UNION
            SELECT symbol AS ticker,
                   CASE WHEN exchange IN ('NASDAQ','NYSE') THEN 'US' ELSE exchange END AS exchange
            FROM datapai.watchlist
        )
        SELECT DISTINCT w.ticker
        FROM wanted w
        JOIN datapai.fundamental_lite f
          ON f.ticker = w.ticker AND f.exchange = w.exchange
        WHERE w.exchange = %s
        ORDER BY w.ticker
    """
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", port=5434,
            user="postgres", password="postgres",
            dbname="postgres", connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange,))
                tickers = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        log.warning("[%s] universe SQL failed (%s) — using fallback", exchange, exc)
        return _FALLBACK_ASX if exchange == "ASX" else _FALLBACK_US

    if not tickers:
        log.warning("[%s] universe SQL returned 0 tickers — using fallback", exchange)
        return _FALLBACK_ASX if exchange == "ASX" else _FALLBACK_US

    log.info("[%s] universe resolved at DAG parse: %d tickers — %s",
             exchange, len(tickers),
             ", ".join(tickers[:10]) + ("…" if len(tickers) > 10 else ""))
    return tickers


ASX_TICKERS = _resolve_universe("ASX")
US_TICKERS  = _resolve_universe("US")


@dag(
    dag_id="stock_synthesis_asx",
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
def stock_synthesis_asx():
    for ticker in ASX_TICKERS:
        datapai_bash_task(
            task_id=f"synth_{ticker.replace('.', '_')}",
            script_name="run_stock_synthesis.sh",
            args=f"--exchange ASX --tickers {ticker}",
        )

stock_synthesis_asx()


@dag(
    dag_id="stock_synthesis_us",
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
def stock_synthesis_us():
    for ticker in US_TICKERS:
        datapai_bash_task(
            task_id=f"synth_{ticker}",
            script_name="run_stock_synthesis.sh",
            args=f"--exchange US --tickers {ticker}",
        )

stock_synthesis_us()
