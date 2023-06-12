# %%
# # Mark2 daily and hourly data ETL for CWSI Calculation and Save to S3
# file load joined_df, lw_temp_3 and swdw into a dataframe used for CWSI calculation 
# and shared on quicksight and pwoerbi dashboard

# %conda install psycopg2
# %conda install -c anaconda boto3
# %conda install -y -c anaconda sqlalchemy

import json
import pandas as pd
import numpy as np
import boto3
import psycopg2
from datetime import timedelta
from sqlalchemy import create_engine, text
from src.utils import df_from_s3, df_to_s3

import plotly.graph_objects as go
from plotly.subplots import make_subplots


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

cte as (
select
 	   DATE_TRUNC('hour', ref_time) as ref_time, 
       site_id,
       source,
       avg(ref_tbelow) as ref_tbelow,
       avg(ref_tsensor) as body_temp

FROM   device_data.calval_ref_data
where site_id in ('TWE_GB', 'TWE_BV2')
and DATE_TRUNC('hour', ref_time)>'{start}' and DATE_TRUNC('hour', ref_time)  < '{end}'
group by DATE_TRUNC('hour', ref_time), site_id, source
ORDER  BY site_id, source, DATE_TRUNC('hour', ref_time)
),

-- 3. join meta data table with reference table
cte1 as (
SELECT c.*, d.device FROM devmap d
join cte c
using (site_id, source)
)
,
--4. join mark and reference/meta table
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

SELECT date_trunc('hour', time) AS time, device, avg(swdw) AS swdw
FROM device_data_alp.calibrated 
WHERE device IN ({devices})
AND time > '{start}' AND time < '{end}'
GROUP BY date_trunc('hour', time), device
ORDER BY device, time;
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

SELECT date_trunc('hour', r.time) AS time, r.device, avg(r.lw_temp_3) AS lw_temp_3
FROM device_data_alp.raw r 
WHERE device IN ({devices}) 
AND time > '{start}' AND time < '{end}'
GROUP BY date_trunc('hour', r.time), r.device
ORDER BY r.device, time;
""".format(devices=devices, start=begin, end=end)

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df
# %%
def read_ref_additional(cnx, begin, end, devices):
    schema_raw = 'hourly'
    query_template_raw = """
--may want to change me here

SELECT date_trunc('hour', time) AS time, device, avg(swdw) AS swdw
FROM device_data_alp.calibrated 
WHERE device IN ({devices})
AND time > '{start}' AND time < '{end}'
GROUP BY date_trunc('hour', time), device
ORDER BY device, time;
""".format(devices=devices, start=begin, end=end)

    sql_query = query_template_raw.format(schema=schema_raw, \
                                         start=begin, end=end)

    df = pd.read_sql_query(sql_query, cnx)

    return df

# %%
def read_irrigation(cnx, begin, end, devices):
    schema_raw = 'hourly'
    query_template_raw = """
--may want to change me here

SELECT time,device, duration_seconds
FROM device_data_alp.irrigation_runtime_hourly r 
WHERE device IN ({devices}) 
AND time > '{start}' AND time < '{end}'
ORDER BY r.device, time;
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
end_date = '2023-06-12'
bucket_name = 'arable-adse-dev'
# %%
%%time
joined_df = read_ref_hourly(pg_conn, start_date, end_date)
joined_df['ref_time'] = pd.to_datetime(joined_df['ref_time'])

# %%
joined_df['time'] = pd.to_datetime(joined_df['time'])
joined_df.sort_values(['device', 'time'])
# %%
# joined_df.sort_values(['device', 'time']).tail(30)
# %%
path = f'Carbon Project/Stress Index/UCD_Almond/Joined_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( joined_df, path, bucket_name, format ='csv')
# %%
%%time
devices_list = "'D003701', 'D003705', 'D003932', 'D003978', 'D003898', 'D003960', 'D003942', 'D003943'"
swdw_df = read_ref_additional(pg_conn, start_date, end_date, devices_list)
# %%
swdw_df['time'] = pd.to_datetime(swdw_df['time'])
# %%
swdw_df = swdw_df[swdw_df['time'].notnull()]
swdw_df['device'] = swdw_df['device'].astype('category')
swdw_cleaned_df = swdw_df.groupby(['time', 'device']).agg({'swdw': 'mean'}).reset_index()
swdw_cleaned_df = swdw_cleaned_df.dropna(subset=['swdw'])
# swdw_cleaned_df.sort_values(['device', 'time']).head(30)
# %%
%%time
# uploaded reference data to S3
path = f'Carbon Project/Stress Index/UCD_Almond/swdw_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( swdw_cleaned_df, path, bucket_name, format ='csv')
# %%
%%time

temp3_df = read_temp3(pg_conn, start_date, end_date, devices_list)
temp3_df['time'] = pd.to_datetime(temp3_df['time'])
# %%
#remove NaT from dataframe
temp3_df['time']=temp3_df['time'].replace('NaT', '-999')
temp3_df['device'] = temp3_df['device'].astype('category')
temp3_df.sort_values(['device', 'time']).head(30)
# %%
path = f'Carbon Project/Stress Index/UCD_Almond/lw_temp3_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( temp3_df, path, bucket_name, format ='csv')

# %%
# Irrigation dataframe
# %%time
devices_list = "'D003701', 'D003705', 'D003932', 'D003978', 'D003898', 'D003960', 'D003942', 'D003943'"
irg_df = read_irrigation(pg_conn, start_date, end_date, devices_list)
# %%
irg_df['time'] = pd.to_datetime(irg_df['time'])
# %%
irg_df
# %%
irg_df = irg_df[irg_df['time'].notnull()]
irg_df['device'] = irg_df['device'].astype('category')
# swdw_cleaned_df.sort_values(['device', 'time']).head(30)
# %%
%%time
# uploaded reference data to S3
path = f'Carbon Project/Stress Index/UCD_Almond/irg_df_hourly.csv' 
df_to_s3( irg_df, path, bucket_name, format ='csv')
# %%
path='Carbon Project/Stress Index/UCD_Almond/lw_temp3_df_hourly.csv'
temp3_df=df_from_s3(path,bucket_name)
path='Carbon Project/Stress Index/UCD_Almond/swdw_df_hourly.csv'
swdw_df=df_from_s3(path,bucket_name)
path='Carbon Project/Stress Index/UCD_Almond/Joined_df_hourly.csv'
joined_df=df_from_s3(path,bucket_name)
# %%
temp3_df.shape, swdw_df.shape, joined_df.shape,irg_df.shape
[temp3_df, swdw_df, joined_df, irg_df] = [df.apply(lambda x: pd.to_datetime(x) if x.name == 'time' else x) for df in [temp3_df, swdw_df, joined_df, irg_df]]
[temp3_df, swdw_df, joined_df, irg_df] = [df.apply(lambda x: x.astype('category') if x.name == 'device' else x) for df in [temp3_df, swdw_df, joined_df, irg_df]]


# temp3_df.device.unique(), swdw_df.device.unique(), joined_df.device.unique()

# %%
merged_df = joined_df.merge(swdw_df, on=['time', 'device']).merge(temp3_df, on=['time', 'device'])
# %%
merged_df = merged_df.merge(irg_df, on=['time', 'device'], how='left')
# %%
merged_df['duration_seconds'] = merged_df['duration_seconds'].fillna(0)
merged_df['duration_seconds'].value_counts()
# %%
# uploaded reference data to S3
bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/Joined_ref_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( merged_df, path, bucket_name, format ='csv')
# %%
# merged_df.loc[merged_df.device=='D003978'].sort_values('time')
# %%
## quickplot of merged data

def quicklook_df(merged_df):
    # Sort the merged_df DataFrame by 'time' in ascending order
    merged_df_sorted = merged_df.sort_values('time')

    # Get unique device values
    unique_devices = merged_df_sorted['device'].unique()

    # Create subplots with shared x-axis
    fig = make_subplots(rows=len(unique_devices), cols=1, shared_xaxes=True)

    # Iterate over unique devices
    for i, device in enumerate(unique_devices, start=1):
        filtered_df = merged_df_sorted[merged_df_sorted['device'] == device]

        # Add trace for each device with mode='lines+markers'
        fig.add_trace(
            go.Scatter(x=filtered_df['time'], y=filtered_df['lw_temp_3'], mode='lines+markers', name=f'Average Temp 3'),
            row=i, col=1
        )
        fig.add_trace(
            go.Scatter(x=filtered_df['time'], y=filtered_df['ref_tbelow'], mode='lines+markers', name=f'Ref Tbelow'),
            row=i, col=1
        )
        fig.add_trace(
            go.Scatter(x=filtered_df['time'], y=filtered_df['tbelow'], mode='lines+markers', name=f'Tbelow'),
            row=i, col=1
        )

        # Set y-axis label for each subplot
        fig.update_yaxes(title_text='Value', row=i, col=1)

        # Set title for each subplot with device name
        # fig.update_layout(title_text=f"Device: {device}", row=i, col=1)

    # Set x-axis label for the last subplot
    fig.update_xaxes(title_text='Time', row=len(unique_devices), col=1)

    # for i, device in enumerate(unique_devices, start=1):
    #     fig.update_layout(title=f"Device: {device}", row=i, col=1)

    # Move the legend to the bottom
    fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                    height=400, width=1000)

    # Set the figure size
    fig.update_layout(width=1000, height=80 * len(unique_devices))

    # Show the figure
    fig.show()
# quick check data quality, time alignment, outlier etc.
quicklook_df(merged_df)





# %%
