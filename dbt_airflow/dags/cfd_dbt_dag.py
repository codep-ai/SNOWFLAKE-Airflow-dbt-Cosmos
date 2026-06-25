"""
CFD dbt pipeline via astronomer-cosmos.

Renders the CFD dbt models (staging views + Iceberg-materialized marts) as a
native Airflow TaskGroup with model-level dependencies visible in the UI.

- Project:  /home/ec2-user/git/dbt-demo
- Profile:  datapai_snowflake_svc (key-pair auth via airbyte_user)
- Target:   prod
- Selector: tag:cfd

Sources are external Iceberg tables in DATAPAI.CFD_LAKE pointing at the
Kafka-Connect-written metadata.json files in s3://datapai-cfd-lake/lake/.
Marts materialize as Snowflake-managed Iceberg back into the same bucket via
EXTERNAL VOLUME CFD_LAKE_VOL + CATALOG CFD_OBJ_CAT.

Schedule: every 30 minutes (matches the CFD producer steady-state cadence).
"""
import os
from datetime import datetime, timedelta
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_ROOT = Path("/home/ec2-user/git/dbt-demo")

# Read .env.dev for SF + AWS creds (mounted at /opt/datapai/.env.dev)
def _load_env(path="/opt/datapai/.env.dev"):
    out = {}
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip("\"").strip("'")
    return out

_env = _load_env()
# Override key path to the mounted location inside the container
_env["SNOWFLAKE_PRIVATE_KEY_PATH"] = "/snowflake-keys/airbyte_user.p8"

profile_config = ProfileConfig(
    profile_name="datapai_snowflake_svc",
    target_name="prod",
    profiles_yml_filepath=Path("/home/ec2-user/.dbt/profiles.yml"),
)

cfd_dbt_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=["tag:cfd"],
        env_vars=_env,
        dbt_deps=False,
    ),
    operator_args={
        "install_deps": False,
        "env": _env,
    },
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2026, 6, 22),
    catchup=False,
    dag_id="cfd_dbt_iceberg_pipeline",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["cfd", "dbt", "iceberg", "snowflake"],
)
