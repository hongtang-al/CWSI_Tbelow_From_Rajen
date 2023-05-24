import datetime as dt
import json
from typing import Union

import pandas as pd
import boto3
import sqlalchemy


def get_user_db_creds(user: str, environment: str):
    """
    Fetch individual user db credentials from AWS Secretes Manager

    :param user: username that corresponds to secret name of the format "{user}_db_creds"
    :param environment: environment for which to fetch db credentials: "alp", "als", or "alt"
    :return db_info: dictionary that includes  user, password, host, port and db name
    """

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=f"{user}_db_creds")
    secret_db_creds = json.loads(response["SecretString"])
    db_info = {
        "user": secret_db_creds[f"user_{environment}"],
        "password": secret_db_creds[f"password_{environment}"],
        "host": secret_db_creds[f"host_{environment}"],
        "db": secret_db_creds[f"db_{environment}"],
        "port": secret_db_creds[f"port_{environment}"],
    }
    return db_info


def connect_to_db(db_info):
    """
    Generate sqlalchemy database engine object

    :param db_info: dictionary containing db name, port, host, user and password in the format, see get_user_db_creds()
    :return db_connection: sqlalchemy database engine object
    """
    engine = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{db_info["user"]}:{db_info["password"]}@{db_info["host"]}/{db_info["db"]}',
        connect_args={"connect_timeout": 5},
    )

    return engine


def query_constructor(
    table,
    device: Union[str, list] = None,
    start_date: dt.datetime = None,
    end_date: dt.datetime = None,
    columns: list = None,
    additional_conditions: str = None,
):
    """
    SQL query constructor for fetching device data

    :param table: db table from which data will be fetched. If this arg has no prefix, "device_data" will be assumed
    :param device: optional, if supplied, the query will request only data from this device, or these devices
    :param start_date: optional, if supplied, the query will request only data after this date (inclusive)
    :param end_date: optional, if supplied, the query will request only data before this date (exclusive)
    :param columns: optional, if supplied, the query will request only columns in this list
    :param additional_conditions: optional, if supplied, the query will be appended with this string
        of additional conditions. known deficiency: user needs to know to start with AND or WHERE
    :return:
    """

    if columns:
        select = f"SELECT {', '.join(columns)} "
    else:
        select = "SELECT * "

    if "." in table:
        select += f"FROM {table} "
    else:
        select += f"FROM device_data.{table} "

    conditions = []
    if start_date and end_date:
        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S+00:00")
        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S+00:00")
        conditions.append(f"time between '{start_str}' and  '{end_str}' ")
    elif start_date:
        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S+00:00")
        conditions.append(f"time >= '{start_str}' ")
    elif end_date:
        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S+00:00")
        conditions.append(f"time < '{end_str}' ")
    if type(device) == str:
        conditions.append(f"device = '{device}' ")
    elif type(device) == list:
        if len(device) == 0:
            raise Exception("no devices provided in list")
        elif len(device) == 1:
            conditions.append(f"device = '{device[0]}' ")
        else:
            conditions.append(f"device IN {tuple(device)} ")
    else:
        raise Exception(
            "Did not recognize datatype provided for device argument. Must be string or list."
        )


    sql_query = f"{select} {''.join([f'WHERE {c}' if i==0 else f'AND {c}' for i, c in enumerate(conditions)])}"

    if additional_conditions:
        sql_query += additional_conditions

    return sql_query


def get_db_data(db_info: dict, sql_query: str, db_connection=None):
    """

    :param db_info: dictionary containing db name, port, host, user and password in the appropriate format,
        see get_user_db_creds()
    :param sql_query: sql query for db
    :param db_connection: (optional) pandas compatible db engine/connection object to use to connect to db
    :return df: pandas df that includes results of db query
    """
    if db_connection is None:
        db_connection = connect_to_db(db_info)
    try:
        df = pd.read_sql(sql=sql_query, con=db_connection)
    except sqlalchemy.exc.OperationalError:
        raise Exception(
            "An operational error occurred. This usually happens when you are not connected to the VPN"
        )
    if "time" in df.columns:
        df.sort_values("time", inplace=True)
    return df


def get_mark_calval_deployments(
    which: str = "current", sites: list = None, include_notes=False
):
    """Fetch pandas df of mark calval deployment data from calval correspondence table
    Args:
        which - determine which deployment data to fetch, 'current', 'historical' or 'all'
        sites - optional. list of sites to restrict returned deployments
        include_notes - if true, includes notes for each deployment
    returns:
        deployment_data - pandas df with deployment data
    """

    url_base = "https://docs.google.com/spreadsheets/d/e"
    doc_id = "2PACX-1vTwPZ7gv05wgGVvaoL0H_IfsLmzXx_nLwUpmWSHwn1Rl5hD0_L8Ur-iuMGueavQitRMMteqLtoFHpqV"
    current_url = f"{url_base}/{doc_id}/pub?gid=0&single=true&output=csv"
    historical_url = f"{url_base}/{doc_id}/pub?gid=1303173661&single=true&output=csv"
    if which == "current":
        urls = [current_url]
    elif which == "historical":
        urls = [historical_url]
    elif which == "all":
        urls = [current_url, historical_url]
    else:
        raise Exception(
            f"Unidentified Data Source: '{which}'. 'which' argument must be set to 'current', 'historical' or 'all' "
        )

    # fetch data
    deployment_data = pd.concat([pd.read_csv(url) for url in urls])

    # format end dates for active devices
    deployment_data["end_date"] = deployment_data["end_date"].apply(
        lambda x: dt.datetime.utcnow() if x == "still active" else x
    )

    # deal with weird datetimes
    deployment_data["start_date"] = pd.to_datetime(
        deployment_data["start_date"], errors="coerce"
    )
    deployment_data["end_date"] = pd.to_datetime(
        deployment_data["end_date"], errors="coerce"
    )

    # filter to data of interest
    cols_of_interest = (
        ["site", "device", "start_date", "end_date", "notes"]
        if include_notes
        else ["site", "device", "start_date", "end_date"]
    )
    deployment_data = deployment_data[cols_of_interest]
    if sites is not None:
        deployment_data = deployment_data[deployment_data["site"].isin(sites)]

    return deployment_data
