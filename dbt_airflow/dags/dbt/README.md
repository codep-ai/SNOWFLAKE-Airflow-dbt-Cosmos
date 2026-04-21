# Embedded dbt projects

Cosmos needs the dbt project source inside the Airflow container. We keep
two projects here:

| Directory | Source repo | Cosmos DAG |
|---|---|---|
| `cosmosproject/` | demo / existing | `my_cosmos_dag.py` |
| `datapai-dbt-governance/` | `~/git/datapai-dbt-governance` | `governance_metrics.py` |

## Install via symlink (local dev)

```bash
ln -s ~/git/datapai-dbt-governance \
      ~/git/datapai-airflow/dbt_airflow/dags/dbt/datapai-dbt-governance
```

## Install via git subtree (production)

```bash
cd ~/git/datapai-airflow
git subtree add --prefix=dbt_airflow/dags/dbt/datapai-dbt-governance \
    git@github.com:datapai/datapai-dbt-governance.git master --squash
```

Update with `git subtree pull` when the governance repo changes.

## Airflow connections

`governance_metrics.py` expects:

- `duckdb_governance` — Connection type: Generic. Used only as a Cosmos
  profile-mapping binding; the actual DuckDB path is declared in
  `profile_args` in the DAG.

(For the `snowflake` target add `snowflake_governance` connection with
the usual Snowflake auth; swap `DuckDBUserPasswordProfileMapping` →
`SnowflakeUserPasswordProfileMapping` in the DAG.)
