from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from airflow import DAG
from airflow.models.dag import DagContext
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

from lw_airflow.core.db import ColumnSpec


def managed_snowflake_task_group(
    group_id: str,
    output_table: str,
    output_columns: List[ColumnSpec],
    select: str,
    replace_where: str = "",
    pre_insert: str = "",
    post_insert: str = "",
    table_type: str = "",
    create_transaction: bool = True,
    cluster_by: Optional[List[str]] = None,
    parent_group: Optional[TaskGroup] = None,
    dag: Optional[DAG] = None,
    **default_args,
) -> TaskGroup:
    """
    Create an idempotent Snowflake operator to handle create->delete->insert task chain.

    Parameters
    ----------
    group_id: str
        Serves as a unique ID for this group of tasks, and a prefix for all the generated tasks.
    output_table: str
        Output table destination. Include the table name only. Warehouse & schema will be inferred
        from the default_args
    output_columns: List[str]
        Specifies the name and type of all columns in the output table.
    select: str
        A Snowflake SQL query to select data from the input datasets.
    replace_where: str
        Optionally specify a Snowflake SQL clause used to delete matching data from `output_table` before replacing it
        with the output from `select`. Items where `replace_where` evaluates to `TRUE` will be replaced by this
        operator.
    pre_insert: str
        Optionally specify statement(s) to be run before invoking the insert/select query
    post_insert: str
        Optionally specify statement(s) to be run after the insert is successful.
    table_type: str
        Optionally specify "temporary" "transient" "global temporary", etc.
    create_transaction: bool (Default True)
        Execute the TaskGroup in an explicit transaction. Set False ONLY if your transaction gets killed by SFMGR
        AND you know what you are doing.
    cluster_by: Optional[List[str]]
        A list of clauses to cluster the table by. If specified, typically a subset of the column list.
    parent_group: Optional[TaskGroup]
        If this task group is part of a parent group, you can specify the parent group here.
    dag: Optional[DAG]
        If you want to pass a DAG explicitly, you can do so here. If not specified, the current context's DAG
        will be used.
    default_args: **kwargs
        Any default arguments that should be passed to the generated SnowflakeOperators. If you want to
        override warehouse, database, schema, or role relative to the rest of your DAG you can do so
        here.

    Returns
    -------
    grp: TaskGroup
        A task group handling the full Snowflake transaction & side effects.
    """
    dag = dag or DagContext().get_current_dag()
    database = default_args.get("database", dag.default_args["database"])
    schema = default_args.get("schema", dag.default_args["schema"])
    full_table_name = f"{database}.{schema}.{output_table}"

    if pre_insert:
        pre_insert = _format_query(pre_insert) + ";"
    if post_insert:
        post_insert = _format_query(post_insert) + ";"

    transaction_start, transaction_end, autocommit_state = _transaction_settings(create_transaction)

    cluster_by_clause = f"CLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
    delete = f"DELETE FROM {full_table_name} WHERE {_format_query(replace_where)};" if replace_where else ""

    with TaskGroup(group_id, parent_group=parent_group, dag=dag, default_args=default_args) as grp:
        columns = ", ".join([c.clause for c in output_columns])
        create = SnowflakeOperator(
            task_id="create",
            sql=f"""
            CREATE {table_type} TABLE IF NOT EXISTS {full_table_name} ({columns})
            {cluster_by_clause};
            """
        )

        col_names = ", ".join([c.name for c in output_columns])
        replace = SnowflakeOperator(
            task_id="replace",
            sql=f"""
            alter session set
                TIMEZONE = 'UTC'
                TIMESTAMP_TYPE_MAPPING = TIMESTAMP_LTZ
                AUTOCOMMIT = {autocommit_state};
            {transaction_start}
            {pre_insert}
            {delete}
            INSERT INTO {full_table_name} ({col_names})
            SELECT {col_names}
            FROM ({_format_query(select)});
            {post_insert}
            {transaction_end}
            alter session unset TIMEZONE, TIMESTAMP_TYPE_MAPPING, AUTOCOMMIT;
            """
        )

        create >> replace

    return grp


def wait_n_hours_operator(task_id: str, n_hours: int) -> PythonSensor:
    """
    This returns an operator that waits for the current time to be greater
    than a set number of hours past the DAGContext's scheduled time. It can
    be used to wait for expected data availability before launching a task
    without changing schedule (and possibly the logical date) of your entire
    DAG. E.g. if you want to wait until 3 AM to process a daily report, you
    can construct an operator like so:

    wait_op = wait_n_hours_operator(
        task_id="wait_til_3_am_tomorrow",
        n_hours=27,
    )

    Note the setting of `n_hours=27` to wait the full UTC day (24 hours) plus
    3 to start at 3 AM. If the target time has already passed, this operator will
    succeed immediately.
    """
    return PythonSensor(
        task_id=task_id,
        python_callable=_wait_n_hours,
        op_args=["{{ ts }}", n_hours],
        mode="reschedule",
        poke_interval=3600,
    )


def _format_query(snow_sql: str) -> str:
    return snow_sql.strip().rstrip(";")


def _transaction_settings(create_transaction: bool) -> Tuple[str, str, str]:
    """ Enable AUTOCOMMIT and remove explicit tx management when create_transaction=False """

    transaction_start = "BEGIN TRANSACTION;" if create_transaction else ""
    transaction_end = "COMMIT;" if create_transaction else ""
    autocommit_state = "FALSE" if create_transaction else "TRUE"

    return transaction_start, transaction_end, autocommit_state


def _wait_n_hours(logical_time_utc: str, n_hours: int) -> bool:
    """
    Return True if the actual time is greater than `n_hours` after this DAG's
    logical scheduled time.
    """
    supplied_time = datetime.fromisoformat(logical_time_utc)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    return now >= supplied_time + timedelta(hours=n_hours)
