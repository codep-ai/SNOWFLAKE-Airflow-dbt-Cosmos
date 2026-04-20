"""ai_framework_refresh — Weekly TinyFish auto-refresh of AI governance seeds.

Reads datapai.sys_common_config rows (config_type='ai_framework_refresh'),
filters enabled frameworks whose cadence has elapsed, for each:
  1. TinyFish fetch source_url
  2. AI-extract via extractor_prompt_path
  3. Diff vs current ai_controls_seed.csv
  4. Open PR in codep-ai/dbt-demo (never auto-merge)
  5. Notify #ai-governance Slack channel
  6. Update last_refreshed_at in sys_common_config

Scheduled daily 03:00 AEDT — script filters by cadence_days internally.
"""
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from datapai_common import DEFAULT_ARGS, datapai_bash_task

SYDNEY_TZ = pendulum.timezone("Australia/Sydney")

@dag(
    dag_id="ai_framework_refresh",
    default_args={**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(minutes=60)},
    schedule="0 3 * * *",
    start_date=datetime(2026, 4, 21, tzinfo=SYDNEY_TZ),
    catchup=False,
    tags=["datapai", "governance", "tinyfish", "ai_framework_refresh"],
    max_active_runs=1,
)
def ai_framework_refresh():
    datapai_bash_task(task_id="refresh_due_frameworks",
                      script_name="run_ai_framework_refresh.sh",
                      args="")

ai_framework_refresh()
