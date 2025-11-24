"""
DAG: btc_elt_dbt_v1 (parse-safe, with dbt path verification + deps)
Purpose:
  - Run dbt (deps → run → test → snapshot) after ETL finishes.
  - Uses Airflow Connection 'snowflake_conn' at runtime OR can fall back to DBT_* envs.
  - Verifies dbt_project.yml & profiles.yml exist before running dbt.
  - DBT_PROJECT_DIR can be overridden via Airflow Variable `DBT_PROJECT_DIR`.
"""

from __future__ import annotations
import os
from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

# Default path inside container
_DEFAULT_DBT_PROJECT_DIR = "/opt/airflow/dbt"

def _project_dir_template():
    # Jinja resolves this Variable at runtime if set; otherwise uses default
    return "{{ var.value.DBT_PROJECT_DIR | default('" + _DEFAULT_DBT_PROJECT_DIR + "') }}"

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {"owner": "airflow"}

with DAG(
    dag_id="btc_elt_dbt_v1",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ELT", "dbt", "analytics"],
    description="ELT: dbt deps/run/test/snapshot to build analytics indicators from RAW",
) as dag:

    @task(task_id="build_dbt_env")
    def build_dbt_env() -> dict:
        """
        Try to fetch Snowflake creds from Airflow connection.
        If not present, fallback to OS env vars DBT_*.
        """
        try:
            conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
            extra = conn.extra_dejson or {}
            env = {
                "DBT_USER": conn.login,
                "DBT_PASSWORD": conn.password,
                "DBT_ACCOUNT": extra.get("account"),
                "DBT_SCHEMA": conn.schema or "analytics",
                "DBT_DATABASE": extra.get("database"),
                "DBT_ROLE": extra.get("role"),
                "DBT_WAREHOUSE": extra.get("warehouse"),
                "DBT_TYPE": "snowflake",
                "DBT_REGION": extra.get("region", ""),
            }
            source = "airflow_connection"
        except AirflowNotFoundException:
            env = {
                "DBT_USER": os.getenv("DBT_USER"),
                "DBT_PASSWORD": os.getenv("DBT_PASSWORD"),
                "DBT_ACCOUNT": os.getenv("DBT_ACCOUNT"),
                "DBT_SCHEMA": os.getenv("DBT_SCHEMA", "analytics"),
                "DBT_DATABASE": os.getenv("DBT_DATABASE"),
                "DBT_ROLE": os.getenv("DBT_ROLE"),
                "DBT_WAREHOUSE": os.getenv("DBT_WAREHOUSE"),
                "DBT_TYPE": "snowflake",
                "DBT_REGION": os.getenv("DBT_REGION", ""),
            }
            source = "os_env"

        required = ["DBT_USER", "DBT_PASSWORD", "DBT_ACCOUNT", "DBT_DATABASE", "DBT_SCHEMA", "DBT_WAREHOUSE"]
        missing = [k for k in required if not env.get(k)]
        if missing:
            raise ValueError(
                f"Missing required Snowflake fields from {source}: {missing}. "
                f"If using Airflow connection, check '{SNOWFLAKE_CONN_ID}'. "
                f"If using env fallback, set those DBT_* variables."
            )

        print(f"[build_dbt_env] Using {source}: user={env['DBT_USER']}, account={env['DBT_ACCOUNT']}, "
              f"db={env['DBT_DATABASE']}, schema={env['DBT_SCHEMA']}, wh={env['DBT_WAREHOUSE']}, role={env['DBT_ROLE']}")
        return env

    def _env_from_xcom(task_id: str):
        return {
            "DBT_USER": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_USER') }}",
            "DBT_PASSWORD": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_PASSWORD') }}",
            "DBT_ACCOUNT": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_ACCOUNT') }}",
            "DBT_SCHEMA": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_SCHEMA') }}",
            "DBT_DATABASE": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_DATABASE') }}",
            "DBT_ROLE": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_ROLE') }}",
            "DBT_WAREHOUSE": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_WAREHOUSE') }}",
            "DBT_TYPE": "snowflake",
            "DBT_REGION": "{{ ti.xcom_pull(task_ids='" + task_id + "').get('DBT_REGION') }}",
        }

    verify_dbt_path = BashOperator(
        task_id="verify_dbt_path",
        bash_command=(
            "set -euo pipefail\n"
            "DBT_PROJECT_DIR=" + _project_dir_template() + "\n"
            'echo "[verify_dbt_path] Checking $DBT_PROJECT_DIR"\n'
            'ls -la "$DBT_PROJECT_DIR" || { echo "Directory not found: $DBT_PROJECT_DIR" >&2; exit 2; }\n'
            '[ -f "$DBT_PROJECT_DIR/dbt_project.yml" ] || { echo "Missing dbt_project.yml in $DBT_PROJECT_DIR" >&2; exit 2; }\n'
            '[ -f "$DBT_PROJECT_DIR/profiles.yml" ] || { echo "Missing profiles.yml in $DBT_PROJECT_DIR" >&2; exit 2; }\n'
            'echo "[verify_dbt_path] OK"\n'
        ),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "/home/airflow/.local/bin/dbt deps "
            f"--profiles-dir {_project_dir_template()} "
            f"--project-dir {_project_dir_template()}"
        ),
        env=_env_from_xcom("build_dbt_env"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "/home/airflow/.local/bin/dbt run "
            f"--profiles-dir {_project_dir_template()} "
            f"--project-dir {_project_dir_template()}"
        ),
        env=_env_from_xcom("build_dbt_env"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "/home/airflow/.local/bin/dbt test "
            f"--profiles-dir {_project_dir_template()} "
            f"--project-dir {_project_dir_template()}"
        ),
        env=_env_from_xcom("build_dbt_env"),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "/home/airflow/.local/bin/dbt snapshot "
            f"--profiles-dir {_project_dir_template()} "
            f"--project-dir {_project_dir_template()}"
        ),
        env=_env_from_xcom("build_dbt_env"),
    )

    build_env = build_dbt_env()
    build_env >> verify_dbt_path >> dbt_deps >> dbt_run >> dbt_test >> dbt_snapshot
