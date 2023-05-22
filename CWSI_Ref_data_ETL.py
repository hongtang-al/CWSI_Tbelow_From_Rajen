 
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
def read_ref_hourly(cnx, site, source, begin, end):
    
    schema_raw = 'hourly'
    query_template_raw = """    
--may want to change me here

select
 	   DATE_TRUNC('hour', ref_time) as ref_time, 
       site_id,
       source,
       avg(ref_tbelow) as ref_tbelow,
        avg(ref_tsensor) as body_temp

FROM   device_data.calval_ref_data
WHERE  site_id = '{site}' --'TWE_GB'
and source= '{source}'
and ref_time  >= '{start}' and ref_time  < '{end}'

group by ref_time, site_id, source
ORDER  BY site_id, source, ref_time  asc

"""

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         site=site, source=source, start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

# %%
# retrieve personal tocken from arable secrete Manager
# --may want to change me here
dsn=get_user_db_creds('hong_tang', 'adse')
sqlalchemy_dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(**dsn)

 
pg_conn = connect_db(sqlalchemy_dsn)
pg_conn

 
# Define start and end dates and device IDs
start_date = '2023-03-25'
end_date = '2023-05-9'
sites = ['TWE_GB', 'TWE_BV2']
sources = ['L1', 'L2', 'H1', 'H2']

from tqdm import tqdm
res = pd.DataFrame()
for site in tqdm(sites):
    for source in tqdm(sources):
        df = read_ref_hourly(pg_conn, site, source, start_date, end_date)
        df['ref_time'] = pd.to_datetime(df['ref_time'])
        res = pd.concat([res, df])


# define site and stress level/source
devmap={ 'B076302':['TWE_GB', 'L1'], 
         'B076523':['TWE_GB', 'L2'], 
         'B076528':['TWE_GB', 'H1'], 
         'B076282':['TWE_GB', 'H2'], 
         'B076526':['TWE_BV2','L1'],
         'B078407':['TWE_BV2','L2'],
         'B076276':['TWE_BV2','H1'],
         'B078419':['TWE_BV2','H2']
      }
# join meta data with pulled reference data
df_devmap=pd.DataFrame(devmap).T
df_devmap.reset_index(inplace=True)
df_devmap.columns=[ 'device', 'site_id', 'source']
joined_df = df_devmap.merge(res, on=['site_id', 'source'])

# uploaded reference data to S3
bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/Joined_ref_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( joined_df, path, bucket_name, format ='csv')
