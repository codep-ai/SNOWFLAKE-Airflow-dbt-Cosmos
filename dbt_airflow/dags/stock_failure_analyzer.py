"""
stock_failure_analyzer — Macro learning loop.

Runs daily at 06:30 UTC, AFTER stock_reflector (06:00 UTC) finishes
grading new debates. For each horizon (7d / 30d / 90d):

  1. Loads all graded losses from sys_agent_debate_log_full
  2. Clusters them by feature signature (direction × conviction ×
     confidence band × thesis_empty × quality_tier × any-gate-fired
     × signals_aligned)
  3. Promotes clusters with >= 5 observations and >= 60% loss rate
  4. Asks Gemini for a 2-3 sentence concrete remediation per cluster
  5. Writes results into datapai.failure_patterns (status='open')

Operators / admins review the patterns later — when a pattern's
suggested fix is applied, mark status='resolved'.

DESIGN NOTE — path safety
─────────────────────────
Uses datapai_bash_task (which maps to /opt/datapai-stock inside the
Airflow container). Does NOT import stock-be Python at DAG-parse time.
This is the same pattern that prevented the May-23 zero-task breakage.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

UTC = pendulum.timezone("UTC")


@dag(
    dag_id="stock_failure_analyzer",
    default_args={
        **DEFAULT_ARGS,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    schedule="30 6 * * *",          # 06:30 UTC, every day (after stock_reflector at 06:00)
    start_date=datetime(2026, 5, 28, tzinfo=UTC),
    catchup=False,
    tags=["datapai", "learning", "failure-analysis", "synthesis"],
    max_active_runs=1,
)
def stock_failure_analyzer():
    # Sequential per-horizon. Each writes its own rows into failure_patterns.
    analyze_7d = datapai_bash_task(
        task_id="analyze_7d",
        script_name="run_failure_analyzer.sh",
        args="--horizon-days 7 --min-cluster 5 --min-loss-rate 40",
    )
    analyze_30d = datapai_bash_task(
        task_id="analyze_30d",
        script_name="run_failure_analyzer.sh",
        args="--horizon-days 30 --min-cluster 5 --min-loss-rate 40",
    )
    analyze_90d = datapai_bash_task(
        task_id="analyze_90d",
        script_name="run_failure_analyzer.sh",
        args="--horizon-days 90 --min-cluster 5 --min-loss-rate 40",
    )
    analyze_7d >> analyze_30d >> analyze_90d


stock_failure_analyzer()
