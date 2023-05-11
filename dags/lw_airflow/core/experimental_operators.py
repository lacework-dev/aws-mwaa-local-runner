"""
All operators in this file should be treated as "experimental". They are
intended for use in applications that are not business-critical. As a
general guideline, if the failure of your DAG could cause a SEV you
should probably not use the contents of this file.
"""
from datetime import datetime, timedelta
from typing import List, Optional, Union

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.python import PythonSensor


def wait_for_hawkeye_operator(
    task_id: str,
    pipeline_name: str,
    job_name: str,
    env_guid: Optional[Union[str, List[str]]] = None,
    execution_delta: Optional[timedelta] = None,
) -> PythonSensor:
    """
    Queries the Hawkeye stats table for completions matching the specified
    filters.

    Parameters
    ----------
    task_id: str
        The ID that will be given to the PythonSensor
    pipeline_name: str
        The name of the Hawkeye DAG. E.g. "HOURLY_AGENT_PIPELINE"
    job_name: str
        The name of the Hawkeye job. E.g. "hourly_event_report"
    env_guid: Optional[Union[str, List[str]]]
        The env_guid(s) to check for completion. If not specified, all
        Lacework customers from analytics.prod_product.lacework_accounts
        with customer type 'Internal' or 'Current Customer', and customer
        status 'TRIAL' or 'ENABLED' will be used. Note that ANY missing
        task will cause this operator to wait.
    execution_delta: Optional[timedelta]
        If specified, the difference between the current DAG's logical time
        and the Hawkeye DAG's batch_time. Similar to Airflow's
        ExternalTaskOperator, positive values denote dates in the past. E.g.
        to specify yesterday, use timedelta(days=1)
    """
    return PythonSensor(
        task_id=task_id,
        python_callable=_wait_for_hawkeye_callback,
        op_args=[
            "{{ ts }}",
            pipeline_name,
            job_name,
            _get_env_cte(env_guid),
            execution_delta or timedelta(seconds=0),
        ],
    )


def _wait_for_hawkeye_callback(
    ts: str,
    pipeline_name: str,
    job_name: str,
    env_cte: str,
    execution_delta: timedelta,
) -> bool:
    batch_time = datetime.fromisoformat(ts) - execution_delta
    df = pd.read_sql(
        sql=f"""
        WITH
        env_guids AS ({env_cte}),
        completions AS (
            SELECT DISTINCT
                env_guid
            FROM
                prodn_mdb.platform_internal.hawkeye_stats_t
            WHERE
                batch_time = '{batch_time.isoformat()}'
                AND pipeline_name = '{pipeline_name}'
                AND job_name = '{job_name}'
        )
        SELECT
            COUNT_IF(completions.env_guid IS NULL) AS not_complete
        FROM
            env_guids
        LEFT JOIN
            completions
        ON
            env_guids.env_guid = completions.env_guid
        """,
        con=_prod_snowflake_hook().get_conn()
    )
    return df.NOT_COMPLETE.iloc[0] == 0


def _prod_snowflake_hook() -> SnowflakeHook:
    return SnowflakeHook(
        snowflake_conn_id="snowflake_lacework",
        warehouse="DATASCIENCE_AIRFLOW",
        role="internal_portal",
        session_parameters={"TIMEZONE": "UTC"}
    )


def _get_env_cte(env_guid: Optional[Union[str, List[str]]]) -> str:
    if env_guid is None:
        return """
        SELECT DISTINCT
            env_guid
        FROM
            lw_dw.sot.core_lacework_accounts_d
        WHERE
            LOWER(sfdc_customer_type) IN ('internal', 'current customer')
            AND account_status IN ('ENABLED', 'TRIAL')
        """

    if isinstance(env_guid, str):
        env_guid = [env_guid]

    values = ", ".join([f"('{e}')" for e in set(env_guid)])
    return f"SELECT column1 AS env_guid FROM (VALUES {values})"
