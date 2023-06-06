# %%
# # Mark2 daily and hourly data ETL for CWSI Calculation and Save to S3


# %conda install psycopg2
# %conda install -c anaconda boto3
# %conda install -y -c anaconda sqlalchemy

import json
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
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
 
# * CWSI ETL Pipeline Integrated reference data and Mark3 data
def read_ref_hourly(cnx, begin, end):
    
    schema_raw = 'hourly'
    query_template_raw = """    
--may want to change me here

-- 1. create meta data table
WITH devmap AS (
  SELECT *
  FROM (VALUES
('D003701', 'TWE_GB', 'L1'),
('D003705', 'TWE_GB', 'L2'),
('D003932', 'TWE_GB', 'H1'),
('D003978', 'TWE_GB', 'H2'),
('D003898', 'TWE_BV2', 'L1'),
('D003960', 'TWE_BV2', 'L2'),
('D003942', 'TWE_BV2', 'H1'),
('D003943', 'TWE_BV2', 'H2')
) AS t(device, site_id, source)
), 
-- 2. read reference data for trial sites
cte as (
select
 	   DATE_TRUNC('hour', ref_time) as ref_time, 
       site_id,
       source,
       avg(ref_tbelow) as ref_tbelow,
       avg(ref_tsensor) as body_temp

FROM   device_data.calval_ref_data
where site_id in ('TWE_GB', 'TWE_BV2')
and ref_time>'{start}' and ref_time  < '{end}'
group by ref_time, site_id, source
ORDER  BY site_id, source, ref_time  
),
-- 3. join meta data table with reference table
cte1 as (
SELECT c.*, d.device FROM devmap d
join cte c
using (site_id, source)
)
--4. join mark and reference/meta table
,
joined as (
SELECT time, tair, tbelow, vpd, ea, precip, lat, long, c.* 
from device_data_alp.hourly d
join cte1 c
on c.device=d.device and c.ref_time =d.time
--
where 
d.device  in (
'D003701', 
'D003705', 
'D003932', 
'D003978', 
'D003898', 
'D003960', 
'D003942', 
'D003943' )
order by time
)
-- 5 join soil data
SELECT  j.*,  moisture_0_mean,  moisture_8_mean,salinity_0_mean,  temp_0_mean
FROM device_data_alp.sentek_hourly s
join joined j
on j.device=s.device and j.time=s.time

"""

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

# %%
def read_ref_additional(cnx, begin, end, devices):
    schema_raw = 'hourly'
    query_template_raw = """
--may want to change me here

SELECT date_trunc('hour', d.time) AS time, d.device, avg(d.swdw) AS average_swdw
FROM device_data_alp.calibrated d 
WHERE device IN ({devices})
AND d.time > '{start}' AND d.time < '{end}'
GROUP BY time, d.device
ORDER BY time;
""".format(devices=devices, start=begin, end=end)

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

# %%
def read_temp3(cnx, begin, end, devices):
    schema_raw = 'hourly'
    query_template_raw = """
--may want to change me here

SELECT date_trunc('hour', r.time) AS time, r.device, avg(r.lw_temp_3) AS average_temp_3
FROM device_data_alp.raw r 
WHERE device IN ({devices})
AND r.time > '{start}' AND r.time < '{end}'
GROUP BY time, r.device
ORDER BY time;
""".format(devices=devices, start=begin, end=end)

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

# %%
# retrieve personal tocken from arable secrete Manager
# --may want to change me here
dsn=get_user_db_creds('hong_tang', 'adse')
sqlalchemy_dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(**dsn)

 
pg_conn = connect_db(sqlalchemy_dsn)
pg_conn

 
# Define start and end dates
start_date = '2023-03-25'
end_date = '2023-05-30'
bucket_name = 'arable-adse-dev'

joined_df = read_ref_hourly(pg_conn, start_date, end_date)
joined_df['ref_time'] = pd.to_datetime(joined_df['ref_time'])
# %%
joined_df 

path = f'Carbon Project/Stress Index/UCD_Almond/Joined_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( joined_df, path, bucket_name, format ='csv')
# %%

devices_list = "'D003701', 'D003705', 'D003932', 'D003978', 'D003898', 'D003960', 'D003942', 'D003943'"
additional_df = read_ref_additional(pg_conn, start_date, end_date, devices_list)
additional_df['time'] = pd.to_datetime(joined_df['time'])
# %%
additional_df
# %%
# uploaded reference data to S3
path = f'Carbon Project/Stress Index/UCD_Almond/swdw_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( additional_df, path, bucket_name, format ='csv')
# %%
temp3_df = read_temp3(pg_conn, start_date, end_date, devices_list)
temp3_df['time'] = pd.to_datetime(joined_df['time'])
# %%
temp3_df
path = f'Carbon Project/Stress Index/UCD_Almond/lw_temp3_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( temp3_df, path, bucket_name, format ='csv')

# %%
#join three dataframes
joined_df.merge(additional_df, on=['time', 'device'])
joined_df.merge(temp3_df, on=['time', 'device'])
joined_df
# %%
# uploaded reference data to S3
bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/Joined_ref_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( joined_df, path, bucket_name, format ='csv')

# %%
joined_df
# %%
