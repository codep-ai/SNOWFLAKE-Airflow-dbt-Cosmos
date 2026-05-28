"""
stock_reflector — Post-trade reflection across multiple horizons.

Daily at 06:00 UTC, grades past debates that have reached each horizon
(7d / 30d / 90d) and extracts per-persona lessons into sys_agent_memory.
Lessons feed back into the NEXT debate's agent prompts — closing the
self-learning loop.

DESIGN NOTES
------------
Three sequential tasks (one per horizon) — sequential because:
  - They all UPDATE the same sys_agent_debate_log rows (different columns,
    but conflict avoidance is cleaner with serial execution)
  - Sum of runtime is small (most rows aren't yet due at 30d/90d horizon)

Per-horizon thresholds (handled in Reflector code):
  -  7d return ±3%  band ±5%   (noisy, mostly random-walk; lessons tagged
                                horizon:7d, weighted lower in future debates)
  - 30d return ±5%  band ±10%  (medium-term, fundamentals + sector rotate)
  - 90d return ±8%  band ±15%  (long-term, where edge actually lives —
                                lessons tagged horizon:90d, prioritised)

The "Lesson injection" path in agents/stock_synthesis/memory.py defaults
to preferring horizon:90d and horizon:30d tags over horizon:7d, so longer-
horizon lessons dominate the next debate's prompts.

Schedule rationale: 06:00 UTC = after both ASX (08:00 UTC ASX EOD) and US
(22:00 UTC US EOD) overnight runs from previous days, but before any new
synthesis kicks off, so memory is fresh when tonight's debate begins.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

UTC = pendulum.timezone("UTC")


@dag(
    dag_id="stock_reflector",
    default_args={
        **DEFAULT_ARGS,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    schedule="0 6 * * *",          # daily 06:00 UTC, every day (incl. weekends — outcomes don't sleep)
    start_date=datetime(2026, 5, 28, tzinfo=UTC),
    catchup=False,
    tags=["datapai", "reflector", "self-learning", "synthesis"],
    max_active_runs=1,
)
def stock_reflector():
    # Sequential by chaining the bash-task returns.
    # Order: 7d first (most rows due daily) → 30d → 90d.
    reflect_7d = datapai_bash_task(
        task_id="reflect_7d",
        script_name="run_reflector.sh",
        args="--horizon-days 7",
    )
    reflect_30d = datapai_bash_task(
        task_id="reflect_30d",
        script_name="run_reflector.sh",
        args="--horizon-days 30",
    )
    reflect_90d = datapai_bash_task(
        task_id="reflect_90d",
        script_name="run_reflector.sh",
        args="--horizon-days 90",
    )

    reflect_7d >> reflect_30d >> reflect_90d


stock_reflector()
