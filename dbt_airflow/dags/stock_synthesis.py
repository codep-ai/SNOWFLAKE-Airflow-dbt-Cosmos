"""
stock_synthesis — AG2 multi-agent stock signal synthesis.

DYNAMIC UNIVERSE (2026-05-24): instead of hardcoding the ticker list, the
DAG resolves it at parse time by calling load_synthesis_universe() in
datapai-stock-be/scripts/lib/ticker_loader.py. That function returns the
union of:

  1. Landing-page top-20 per exchange (datapai.market_demo_stocks) — what
     every visitor sees on /us, /asx, /vietnam, /hongkong, etc.
  2. All user watchlist tickers (datapai.watchlist) — what users actually
     care to track.

filtered to tickers we have fundamental_lite data for. Adds/promotions on
the FE or new watchlist entries are automatically synthesized the next
night — no DAG redeploy required.

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
# Resolve the synthesis universe at DAG parse time.
# DAG parsing happens every ~30s by Airflow's scheduler, so the universe
# auto-refreshes from DB without any redeploy. Empty result is treated as
# "no work tonight" — better than crashing the DAG.
# ──────────────────────────────────────────────────────────────────────────
def _resolve_universe(exchange: str) -> list[str]:
    """Return list of tickers for an exchange. Fail-soft → empty list on error."""
    try:
        import sys
        sys.path.insert(0, "/home/ec2-user/git/datapai-stock-be")
        from scripts.lib.ticker_loader import load_synthesis_universe
        pairs = load_synthesis_universe(exchange=exchange)
        tickers = [t for (t, _ex) in pairs]
        log.info("[%s] universe resolved at DAG parse: %d tickers — %s",
                 exchange, len(tickers), ", ".join(tickers[:10]) + ("…" if len(tickers) > 10 else ""))
        return tickers
    except Exception as exc:
        log.warning("[%s] universe resolve failed (%s) — running zero tasks tonight", exchange, exc)
        return []


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
