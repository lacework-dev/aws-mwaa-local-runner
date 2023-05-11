import boto3
from airflow.models import Variable

def get_multi_region_info(regions=["us","eu","au"]):
    """
    creates lists that allow us to configure multi-snowflake environment setup in airflow

    parameters
    ----------
    params_both: list
        since the different snowflake environments have different database names for LWBI, the function gets the parameters
        that will ultimately pass to the SQL queries, creates separate dictionaries for each env, adds the corresponding lwbi db name
        and passes back a list of dictionaries

    returns
    ----------
    env_names: list of snowflake environment names
    snowflake_conn_ids: list of snowflake connection ids
    snowflake_warehouse_names: list of warehouses to use with each env
    params_lst: list of dictionaries - each dictionary is specific to each snowflake env
    """
    lst_dict = []


    us = {
        "env_name": "us",
        "snowflake_conn_id":"snowflake_lacework",
        "warehouse":"DATASCIENCE_AIRFLOW",
        "total_cdb_number": Variable.get("total_cdb_number"),
        "lwbi":"prodn_lacework_bi"

    }
    eu = {
        "env_name":"eu",
        "snowflake_conn_id":"snowflake_lacework_eu",
        "warehouse":"DATASCIENCE_AIRFLOW_0",
        "total_cdb_number": Variable.get("total_cdb_number_eu"),
        "lwbi":"euprodn_lacework_bi"
    }
    au = {
        "env_name":"au",
        "snowflake_conn_id":"snowflake_lacework_au",
        "warehouse":"DATASCIENCE_AIRFLOW_0",
        "total_cdb_number":Variable.get("total_cdb_number_au"),
        "lwbi":"auprodn1_lacework_bi"
    }

    if "us" in regions:
        lst_dict.append(us)
    if "eu" in regions:
        lst_dict.append(eu)
    if "au" in regions:
        lst_dict.append(au)

    return lst_dict

def get_config_properties(prod_properties: any, non_prod_properties: any) -> any:
    """
    sets properties based on the environment variables

    parameters
    ----------
    prod_properties: any data types including bool, int, str, float, list, dict, etc.
        has all the prod properties
    non_prod_properties: any data types including bool, int, str, float, list, dict, etc.
        has all the non-prod properties

    returns
    -------
    either prod_properties or non_prod_properties
    """

    env = Variable.get("env")

    if env == "prod":
        return prod_properties
    else:
        return non_prod_properties


def get_secret_from_secretsmanager(secret_id: str, region_name: str = "us-west-2") -> str:

    """
    gets secrets or tokens from AWS secretsmanager

    parameters
    ----------
    secret_id: string
        Equivalent to secret name in the secret details section.
        Here uses secret_id to avoid security issue from guardrails check in Github.
    region_name: string
        The default value is "us-west-2".

    returns
    -------
    secret: string
        A string of either key/value pairs or plain text depending on how the secrets are stored.
    """

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    secret = client.get_secret_value(SecretId=secret_id)['SecretString']

    return secret
