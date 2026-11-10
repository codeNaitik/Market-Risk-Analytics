"""Daily batch pipeline: ingest -> bronze -> silver (Spark) -> gold (dbt) -> risk models.

Schedule: 22:30 America/Toronto on weekdays, after North American markets close and
FRED / Bank of Canada publish the day's rates.

Design choices worth explaining in an interview:
  * Ingestion tasks run in parallel and are independent: one API outage (say SEC) does
    not block prices from loading. Silver waits for all of them, but uses
    trigger_rule=all_done so stale-but-present data still flows; dbt source freshness
    tests then flag anything that is genuinely late.
  * Every task is idempotent: bronze is partitioned by ingest_date (the DAG's logical
    date), silver uses dynamic partition overwrite, dbt rebuilds tables. Re-running a
    day or backfilling a range is safe.
  * Retries with exponential backoff absorb flaky public APIs.
  * Run mode is switched by the environment variable MRA_RUN_MODE (read at parse time,
    so no metadata-DB call on every DAG parse):
      local      -> Spark + dbt run inside the Airflow worker (docker-compose demo)
      databricks -> Spark jobs + dbt are triggered on Azure Databricks Jobs
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup

REPO = "/opt/airflow/repo"
ENV = {"PYTHONPATH": f"{REPO}/src", "LAKE_ROOT": "{{ var.value.get('lake_root', '/opt/airflow/repo/lake') }}"}
INGEST_DATE = "--ingest-date {{ ds }}"

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="market_risk_daily",
    description="Market & macro risk lakehouse: daily batch",
    schedule="30 22 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Toronto"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["finance", "lakehouse", "risk"],
    doc_md=__doc__,
) as dag:

    run_mode = os.getenv("MRA_RUN_MODE", "local")

    with TaskGroup("ingest_bronze") as ingest:
        BashOperator(task_id="prices", bash_command=f"python -m mra.ingestion.prices --days 10 {INGEST_DATE}", env=ENV, append_env=True)
        BashOperator(task_id="sec_edgar", bash_command=f"python -m mra.ingestion.sec_edgar {INGEST_DATE}", env=ENV, append_env=True)
        BashOperator(task_id="fred", bash_command=f"python -m mra.ingestion.fred {INGEST_DATE}", env=ENV, append_env=True)
        BashOperator(task_id="boc_valet", bash_command=f"python -m mra.ingestion.boc_valet {INGEST_DATE}", env=ENV, append_env=True)

    if run_mode == "databricks":
        # Triggers the job defined in databricks/databricks.yml (silver -> dbt -> risk models),
        # deployed with `databricks bundle deploy`.
        from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

        databricks_job = DatabricksRunNowOperator(task_id="databricks_mra_daily", databricks_conn_id="databricks_default",
                                                  job_name="mra_daily", trigger_rule="all_done")
        first = last = databricks_job
    else:
        silver = BashOperator(task_id="silver_spark", bash_command="python -m mra.processing.run_silver",
                              env=ENV, append_env=True, trigger_rule="all_done")
        dbt_cmd = f"cd {REPO}/dbt && dbt {{cmd}} --profiles-dir . --target dev"
        with TaskGroup("gold_dbt") as gold:
            seed = BashOperator(task_id="seed", bash_command=dbt_cmd.format(cmd="seed"), env=ENV, append_env=True)
            freshness = BashOperator(task_id="source_freshness", bash_command=dbt_cmd.format(cmd="source freshness"),
                                     env=ENV, append_env=True)
            build = BashOperator(task_id="build", bash_command=dbt_cmd.format(cmd="build"), env=ENV, append_env=True)
            seed >> freshness >> build
        risk = BashOperator(task_id="risk_models", bash_command="python -m mra.analytics.run_risk", env=ENV, append_env=True)
        export = BashOperator(task_id="export_for_powerbi", bash_command="python -m mra.export_gold", env=ENV, append_env=True)
        silver >> gold >> risk >> export
        first, last = silver, export

    done = EmptyOperator(task_id="done")
    ingest >> first
    last >> done
