# 2026-09-24 — DAG-folder walk broke on a virtualenv inside a symlinked repo; fixed with .airflowignore

**Symptom.** `airflow dags list` / `airflow tasks list` inside the scheduler container:
`RuntimeError: Detected recursive loop when walking DAG directory /opt/airflow/dags: /home/ec2-user/git/dbt-demo/xengine_stock/.venv_airflow/lib has appeared more than once.`
Scheduled runs kept succeeding (the file processor tolerates it), but the CLI DAG bag could not be built.

**Why.** `dags/dbt-demo` is a symlink to `/home/ec2-user/git/dbt-demo`, so anything created inside that repo is inside the DAGs tree.
On 2026-09-21 a virtualenv (`xengine_stock/.venv_airflow`, dbt 1.9 + adapters for the cross-engine DAG) was created there; its
`lib64 -> lib` symlink makes the directory walk revisit the same path. The root `.airflowignore` was empty.

**Fix.** Appended to `dbt_airflow/dags/.airflowignore` (backup `.airflowignore.bak-20260924T*`; syntax = regex on the relative path):
```
\.venv
node_modules
\.git/
target/
```
Verified: `airflow dags list` returns all 38 DAGs; `airflow tasks list stock_asx_eod` returns its 11 tasks. No restart needed.

**Rule.** Never create virtualenvs, node_modules or build output inside a repo that is symlinked under the DAGs folder — or add the
pattern here first.
