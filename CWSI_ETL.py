 
# # Mark2 daily and hourly data ETL for CWSI Calculation and Save to S3


# %conda install psycopg2
# %conda install -c anaconda boto3
# %conda install -y -c anaconda sqlalchemy


import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import boto3
import psycopg2
from datetime import timedelta
from sqlalchemy import create_engine, text
from src.utils import df_from_s3, df_to_s3



# ## Connect to Database
def get_user_db_creds(user: str, environment: str):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=f"{user}_db_creds_1")
    secret_db_creds = json.loads(response["SecretString"])
    db_info = {
        "user": secret_db_creds[f"user_{environment}"],
        "password": secret_db_creds[f"password_{environment}"],
        "host": secret_db_creds[f"host_{environment}"],
        "db": secret_db_creds[f"db_{environment}"],
        "port": secret_db_creds[f"port_{environment}"],
    }
    return db_info

 
def connect_db(dsn: str) -> str:
    cnx = create_engine(dsn)
    return cnx
 
# * CWSI ETL Pipeline

 
def read_daily(cnx, device, column_hourly, begin, end):
    
    schema_raw = 'daily'
    query_template_raw = """    
--may want to change me here
select {column_hourly}
from device_data_alp.hourly as r
where r.device = '{device}' and time  >= '{start}' and time  < '{end}'
"""

    sql_query = query_template_raw.format(schema=schema_raw, device=device, column_hourly=column_hourly, start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

 
# retrieve personal tocken from arable secrete Manager
# --may want to change me here
dsn=get_user_db_creds('hong_tang', 'adse')
sqlalchemy_dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(**dsn)

 
pg_conn = connect_db(sqlalchemy_dsn)
pg_conn

 
# Define start and end dates and device IDs
start_date = '2023-03-25'
end_date = '2023-05-9'
devices = ['D003701', 
'D003705', 
'D003932', 
'D003978', 
'D003898', 
'D003960', 
'D003942', 
'D003943' ]

column_hourly = 'time, device, swdw, tbelow, tair, precip, vpd, ea'

# Read data for each device and save to S3
# res = pd.DataFrame()
# for device in devices:
#     df = read_daily(pg_conn, device, column_daily, column_hourly, start_date, end_date)
#     df['time'] = pd.to_datetime(df['time'])
#     res = pd.concat([res, df])

from tqdm import tqdm
res = pd.DataFrame()
for device in tqdm(devices):
    df = read_daily(pg_conn, device, column_hourly, start_date, end_date)
    df['time'] = pd.to_datetime(df['time'])
    res = pd.concat([res, df])

bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/ET_mark_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( res, path, bucket_name, format ='csv')

