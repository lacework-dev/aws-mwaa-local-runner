from airflow import DAG


def wh_size_to_credits(size_clause: str, string_literal: bool = False) -> str:
    if string_literal:
        size_clause = "'" + size_clause.replace("'", "\\'") + "'"
    return f"""
    CASE {size_clause}
        WHEN 'X-Small' THEN 1
        WHEN 'Small' THEN 2
        WHEN 'Medium' THEN 4
        WHEN 'Large' THEN 8
        WHEN 'X-Large' THEN 16
        WHEN '2X-Large' THEN 32
        WHEN '3X-Large' THEN 64
        WHEN '4X-Large' THEN 128
        WHEN '5X-Large' THEN 256
        WHEN '6X-Large' THEN 512
        WHEN '7X-Large' THEN 1024
        ELSE 0
    END
    """


def SnowflakeETLDAG(**kwargs) -> DAG:
    """
    Applies reasonable defaults for Snowflake ETL pipelines.
    DAG required arguments are still required here as keyword arguments
    (e.g. dag_id), and any overrides (e.g. in default_args) will be
    respected.

    Even though this is a function, we use CamelCase to convey to the
    user that they are instantiating a DAG with this callable.

    The instantiated DAG will have the following properties:
    - Default connections via snowflake_conn_id to the DATASCIENCE_AIRFLOW warehouse
    - lw_dw.etl as the default database/schema
    - internal_portal as the snowflake role
    - A custom macro {{ ws_size_to_credits(value) }}

    kwargs are passed directly to DAG(...) with defaults applied.
    """
    if "catchup" not in kwargs:
        kwargs["catchup"] = False  # Be intentional about compute usage

    user_defined_macros = {
        "wh_size_to_credits": wh_size_to_credits
    }
    user_defined_macros.update(kwargs.get("user_defined_macros", {}))
    kwargs["user_defined_macros"] = user_defined_macros

    default_args = {
        "snowflake_conn_id": "snowflake_lacework",
        "session_parameters": {"TIMEZONE": "UTC"},
        "warehouse": "DATASCIENCE_AIRFLOW",
        "database": "LW_DW",
        "schema": "ETL",
        "role": "internal_portal",
    }
    default_args.update(kwargs.get("default_args", {}))
    kwargs["default_args"] = default_args
    return DAG(**kwargs)




