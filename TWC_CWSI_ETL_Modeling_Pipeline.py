# %% [markdown]
# # Part 1: CWSI ETL

# %%
# %%
# # Mark2 daily and hourly data ETL for CWSI Calculation and Save to S3
# file load joined_df, lw_temp_3 and swdw into a dataframe used for CWSI calculation 
# and shared on quicksight and pwoerbi dashboard

# !pip install scikit-learn
# !pip install --force-reinstall boto3
# !pip install psycopg2-binary

# %%
import json
import pandas as pd
import numpy as np
import boto3
import psycopg2
from datetime import timedelta, datetime
from sqlalchemy import create_engine, text
import io
# from src.utils import df_from_s3, df_to_s3

# %%
# preload packages so only one ipynb used in scheduler

# %%


def df_from_s3(key, bucket, format="csv", **kwargs):
    """read csv from S3 as pandas df
    Arguments:
        key - key of file on S3
        bucket - bucket of file on S3
        **kwargs - additional keyword arguments to pass pd.read_ methods
    Returns:
        df - pandas df
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    if format == "csv":
        csv_string = body.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_string), **kwargs)
    elif format == "parquet":
        bytes_obj = body.read()
        df = pd.read_parquet(io.BytesIO(bytes_obj), **kwargs)
    else:
        raise Exception(f"format '{format}' not recognized")
    return df


def df_to_s3(df, key, bucket, verbose=True, format="csv"):
    if format == "csv":
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
    elif format == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
    else:
        raise Exception(f"format '{format}' not recognized")
    # write stream to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    if verbose:
        print(f"Uploaded file to s3://{bucket}/{key}")


# %%
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
SELECT  j.*,  moisture_0_mean, moisture_1_mean, moisture_2_mean,moisture_3_mean,moisture_4_mean,moisture_5_mean,moisture_6_mean, moisture_7_mean, moisture_8_mean, salinity_0_mean,  temp_0_mean
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
end_date = datetime.today().strftime('%Y-%m-%d')  # Get today's date
bucket_name = 'arable-adse-dev'

# %%
joined_df = read_ref_hourly(pg_conn, start_date, end_date)
joined_df['ref_time'] = pd.to_datetime(joined_df['ref_time'])

joined_df['time'] = pd.to_datetime(joined_df['time'])
joined_df.sort_values(['device', 'time'])

path = f'Carbon Project/Stress Index/UCD_Almond/Joined_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( joined_df, path, bucket_name, format ='csv')

# %%
devices_list = "'D003701', 'D003705', 'D003932', 'D003978', 'D003898', 'D003960', 'D003942', 'D003943'"
swdw_df = read_ref_additional(pg_conn, start_date, end_date, devices_list)
swdw_df['time'] = pd.to_datetime(swdw_df['time'])

swdw_df = swdw_df[swdw_df['time'].notnull()]
swdw_df['device'] = swdw_df['device'].astype('category')
swdw_cleaned_df = swdw_df.groupby(['time', 'device']).agg({'swdw': 'mean'}).reset_index()
swdw_cleaned_df = swdw_cleaned_df.dropna(subset=['swdw'])

# %%
path = f'Carbon Project/Stress Index/UCD_Almond/swdw_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( swdw_cleaned_df, path, bucket_name, format ='csv')

temp3_df = read_temp3(pg_conn, start_date, end_date, devices_list)
temp3_df['time'] = pd.to_datetime(temp3_df['time'])

temp3_df['time']=temp3_df['time'].replace('NaT', '-999')
temp3_df['device'] = temp3_df['device'].astype('category')
# temp3_df.sort_values(['device', 'time']).head(30)

# %%
path = f'Carbon Project/Stress Index/UCD_Almond/lw_temp3_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( temp3_df, path, bucket_name, format ='csv')

# %%
irg_df = read_irrigation(pg_conn, start_date, end_date, devices_list)
irg_df['time'] = pd.to_datetime(irg_df['time'])
irg_df = irg_df[irg_df['time'].notnull()]
irg_df['device'] = irg_df['device'].astype('category')
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


# %%
merged_df = joined_df.merge(swdw_df, on=['time', 'device']).merge(temp3_df, on=['time', 'device'])
merged_df = merged_df.merge(irg_df, on=['time', 'device'], how='left')


# %%
merged_df['duration_seconds'] = merged_df['duration_seconds'].fillna(0)

# %%
bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/Joined_ref_df_hourly.csv' #ET{device}_mark_df_daily.csv
df_to_s3( merged_df, path, bucket_name, format ='csv')

# %% [markdown]
# # Part 2: CWSI Feature Engineering and Model Prediction

# %%
import nbformat
print(nbformat.__version__)

import sys
import pandas as pd
# from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
# import seaborn as sns
import boto3
from botocore.session import Session
import pandas.io.sql as psql
import datetime as dt
import numpy as np
import warnings

warnings.simplefilter(action="ignore")

# %%
# Add the path to calval-etl to sys.path
path_to_calval_etl = '/home/ec2-user/SageMaker/calval-etl'
sys.path.append(path_to_calval_etl)



# %%
# Import specific functions from src.math module
# from src.math import NDVI_, EVI_, crop_kH, SlpInt, lr_vpd_tdif, esat_, upper_limit, lower_limit
# from src.utils import df_from_s3, df_to_s3

# %%
#preload functions

## NDVI and EVI functions 
def NDVI_(df):
    """NDVI
    """
    df['ndvi'] = (df['b6uw'] - df['b4uw']) / (df['b6uw'] + df['b4uw'])
    return df

def EVI_(df):
    """EVI
    """
    df['evi'] = 2.5 *(df['b6uw'] - df['b4uw']) / (df['b6uw'] + (6 * df['b4uw'] - 7.5 * df['b1uw']) +1)
    
    return df

def crop_kH(df):
    """
    Calculates the crop coefficient and hourly actual evapotranspiration for a crop using NDVI and actual ET values.

    Args:
        df: A Pandas DataFrame with columns 'ndvi' and 'et'.

    Returns:
        A new DataFrame with columns 'kcb_hr' and 'new_etc_hr'. 'kcb_hr' represents the crop coefficient and 'new_etc_hr' represents 
        the hourly actual evapotranspiration calculated as the product of 'et' and 'kcb_hr'.
    """
    df['kcb_hr'] =  0.176 + 1.325 * df['ndvi'] - 1.466 * df['ndvi'] ** 2 + 1.146 * df['ndvi'] ** 3
    df['new_etc_hr'] = df['et'] * df ["kcb_hr"] 
    return 

def SlpInt(df):
    df['time'] = pd.to_datetime(df['time'])
    mask = (df['date'] > '2021-06-01') & (df['date'] <= '2021-07-30')
    df = ET75_daily.loc[mask_m]
    df_sub = {'vpd': df['vpd'], 'diff':df['ref_tbelow']- df['tair']}
    df_sub =pd.DataFrame.from_dict(df_sub)
    df_sub = df_sub[df_sub['diff'].notna()]
    x= df_sub.iloc[:,0].values.reshape(-1,1)
    y= df_sub.iloc[:,1].values.reshape(-1,1)
    lr= LinearRegression()
    m1= lr.fit(x, y)
    Y_pred = lr.predict(x)
    plt.scatter(x, y)
    plt.plot(x, Y_pred, color='red')
    plt.title('Apogee data_R ')
    plt.ylabel ('tbelow - tair')
    plt.xlabel ('vpd')
    plt.show()
    b=m1.intercept_
    a=m1.coef_
    print(a,b)
    return

def lr_vpd_tdif(df):
    from sklearn.linear_model import LinearRegression
    ''' 
    Fits a linear regression model to two columns of a Pandas DataFrame.

    Args:
        df: A DataFrame with two columns representing independent variable x and dependent variable y.

    Returns:
        A tuple with four elements: x, Y_pred, coef, and intercept. x is the input independent variable, Y_pred is
        the predicted dependent variable, coef is the slope of the regression line, and intercept is the y-intercept.
        '''
    x = df.iloc[:,0].values.reshape(-1,1)
    y= df.iloc[:,1].values.reshape(-1,1)

    lr = LinearRegression()
    ## ideally from Apogee data to establish regression

    m1= lr.fit(x, y)
    Y_pred = lr.predict(x)
    return x, Y_pred, m1.coef_, m1.intercept_

def esat_(xT):  # pragma: no cover
    """ saturation vapor pressure: kPa
        return 0.611*exp(17.27*xT/(xT+237)) # Monteith formulation
    :param xT:
    :return:
    """
    return (617.4 + 42.22 * xT + 1.675 * xT ** 2 + 0.01408 * xT ** 3 +
            0.0005818 * xT ** 4)/1000

def upper_limit(df):
    cp = 1013 # heat capacity of air
    d = 1.225 #density of air
    zm = 4.5 # height of wind measurement 
    chmax = 4
    dp = 2*(chmax/3)
    zom = 0.123 * chmax
    zoh = 0.1 * zom 
    ra=(math.log((zm-dp)/zom)*math.log((zm-dp)/zoh))/(0.41*0.41*2)
    #ra=14.16
    print (ra)
    ul_m = (ra * (df["swdw"]))/( cp * d )
    return ul_m

def lower_limit(df): 
    chmax = 4
    dp = 2*(chmax/3)
    zom = 0.123 * chmax
    zoh = 0.1 * zom
    print(zoh)
    gamma =  101.325 *(101.3*(((293-0.0065 *36.6)/293)** 5.26))/ (0.622 * (2503 -2.39 * df.tair)* 100)
    #print (gamma)
    delta = (4098 * (0.6108 * np.exp((17.27 * df.tair) / (df.tair + 237.3)) / ((df.tair + 237.3) ** 2)))
    #print (delta)
    es =0.6108 *(np.exp((17.27*df.tair)/(df.tair+237.3)))
    ea = df['ea']
    ll_m = ((df.ul_m * gamma)/delta + zoh) - ((es - ea) / (delta + zoh)) 
    return ll_m

# %%
# Process ET DataFrame by converting columns to appropriate data types and taking the average for each 30-minute interval
def process_ET(df):
    df_p = df.copy()
    df_p['local_time'] = df_p['local_time'].astype('datetime64[ns]').dt.round('30min')
    cols = [i for i in df_p.columns if i not in ['local_time', 'device', 'treatment', 'location', 'update_time', 'create_time']]
    for col in cols:
        df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
    df_p1 = df_p.groupby('local_time').mean().reset_index()
    return df_p1


# %%
def DACT_DANS(df, Tcritical):
    """
    Calculates DACT and DANS based on the provided DataFrame and critical temperature.

    Parameters:
    - df (pandas.DataFrame): DataFrame containing temperature data.
    - Tcritical (float): Critical temperature value (in Celsius).

    Returns:
    - df (pandas.DataFrame): DataFrame with added columns for DACT and DANS.

    If an error occurs during the calculation of DANS, an error message is displayed.

    Note: This function modifies the input DataFrame by adding the 'DACT' and 'DANS' columns.
    # define Tcritical=20C from https://digitalcommons.unl.edu/cgi/viewcontent.cgi?article=2505&context=usdaarsfacpub
    """
    
   
    df['DACT'] = (df['tbelow'] - Tcritical).clip(lower=0)
   
    try:
        if (df['tair'] != 0).all() or df['tair'].notnull().all():
            df['DANS'] = (df['tair'] - df['tbelow']) / df['tair']
        else:
            print('Check if tair is not null or equals to zero')
    except:
        print('An error occurred while calculating DANS. Please check your data.')
    return df

# %% Stress Time above Tcritical
def Stress_Intensity(df):
    '''
    aggregate DACT daily sum to df_daily dataframe
    and aggregate DACT >0 (defined as >0.00001) to get stressed hours
    '''
    df_daily=pd.DataFrame()
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    
    df_daily['device'] = df['device'].groupby(pd.Grouper(freq='D')).first()
    df_daily['site_id'] = df['site_id'].groupby(pd.Grouper(freq='D')).first()
    df_daily['source'] = df['source'].groupby(pd.Grouper(freq='D')).first()
    df_daily['DACT_daily'] = df['DACT'].groupby(pd.Grouper(freq='D')).sum()
    # use to compute daily hours when DACT greater than zero 
    df['DACTgtZero']=np.where(df['DACT']>0.00001, 1, 0)
    df_daily['stressedHours'] = df['DACTgtZero'].groupby(pd.Grouper(freq='D')).sum()

    df_daily.reset_index(inplace=True)
 
    return df_daily

# %%
# Perform preprocessing steps on DataFrame, including calculating various columns based on existing columns
def preprocessing(df, a, b):
    df['LL'] = a + b * df['vpd']  # Define lower limit
    df['VPG'] = (df['es'] * (a + df['tair'])) - df['es'] * df['tair']  # Compute VPG
    df['UL_mod'] = (b - a) * abs(df['VPG'])  # ULmode
    df['UL'] = df['tbelow'] + 5  # Upper limit
    df['diff'] = df['tbelow'] - df['tair']
    df['CWSI'] = (abs(df['diff'] - df['LL']) / (df['UL_mod'] - df['LL']))

    window_size = 3 #moving averate of 3 hours
    #compute pseudo CWSI by computing slopes of diff/vpd
    df['diff_rolling_avg'] = df['diff'].rolling(window=window_size, min_periods=1).mean()
    df['vpd_rolling_avg'] = df['vpd'].rolling(window=window_size, min_periods=1).mean()
    df['pCWSI'] = df.apply(lambda x: x.diff_rolling_avg/x.vpd_rolling_avg, axis=1)

    #calculate DACT and DANS
    DACT_DANS(df, Tcritical)

    df_daily=pd.DataFrame()
    # calculate stress intensity
    df_daily = Stress_Intensity(df)

    return df, df_daily

def get_solar_time(longitude, utc_time):
    # Convert utc_time to pandas Timestamp object
    utc_time = pd.to_datetime(utc_time)
    # Calculate the hour offset based on the longitude
    hour_offset = round(longitude / 15)
    hour_offset = pd.to_timedelta(hour_offset, unit = 'h')
    # Apply the offset to the UTC time to get the solar time
    solar_time = utc_time + hour_offset

    return solar_time


def create_features(field_df):
# Calculate the 'tbelow-tair' column by subtracting 'tair' from 'tbelow'
    field_df['ref_tbelow-tair'] = field_df['ref_tbelow'] - field_df['tair']
    field_df['es'] = esat_(field_df['tair'])
    # field_df['tzconvert_time1'] = field_df['time'].dt.tz_convert('America/Los_Angeles')
    # field_df['fntrans_time'] = solar_noon_(field_df['time'], field_df['long'])
    field_df['solar_time'] = get_solar_time( field_df['long'], field_df['time'])
    field_df['solarnoon'] = (field_df['solar_time'].dt.hour >= 12) & (field_df['solar_time'].dt.hour < 16)
    field_df['solarnoon'] = field_df['solarnoon'].astype(int)
    return field_df

# %%
session = Session()
# define critical temperature for DACT calculation
Tcritical = 20

# %%
# Process ET DataFrame by converting columns to appropriate data types and taking the average for each 30-minute interval
def process_ET(df):
    df_p = df.copy()
    df_p['local_time'] = df_p['local_time'].astype('datetime64[ns]').dt.round('30min')
    cols = [i for i in df_p.columns if i not in ['local_time', 'device', 'treatment', 'location', 'update_time', 'create_time']]
    for col in cols:
        df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
    df_p1 = df_p.groupby('local_time').mean().reset_index()
    return df_p1

# %%
def DACT_DANS(df, Tcritical):
    """
    Calculates DACT and DANS based on the provided DataFrame and critical temperature.

    Parameters:
    - df (pandas.DataFrame): DataFrame containing temperature data.
    - Tcritical (float): Critical temperature value (in Celsius).

    Returns:
    - df (pandas.DataFrame): DataFrame with added columns for DACT and DANS.

    If an error occurs during the calculation of DANS, an error message is displayed.

    Note: This function modifies the input DataFrame by adding the 'DACT' and 'DANS' columns.
    # define Tcritical=20C from https://digitalcommons.unl.edu/cgi/viewcontent.cgi?article=2505&context=usdaarsfacpub
    """
    
   
    df['DACT'] = (df['tbelow'] - Tcritical).clip(lower=0)
   
    try:
        if (df['tair'] != 0).all() or df['tair'].notnull().all():
            df['DANS'] = (df['tair'] - df['tbelow']) / df['tair']
        else:
            print('Check if tair is not null or equals to zero')
    except:
        print('An error occurred while calculating DANS. Please check your data.')
    return df

# %% Stress Time above Tcritical
def Stress_Intensity(df):
    '''
    aggregate DACT daily sum to df_daily dataframe
    and aggregate DACT >0 (defined as >0.00001) to get stressed hours
    '''
    df_daily=pd.DataFrame()
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    
    df_daily['device'] = df['device'].groupby(pd.Grouper(freq='D')).first()
    df_daily['site_id'] = df['site_id'].groupby(pd.Grouper(freq='D')).first()
    df_daily['source'] = df['source'].groupby(pd.Grouper(freq='D')).first()
    df_daily['DACT_daily'] = df['DACT'].groupby(pd.Grouper(freq='D')).sum()
    # use to compute daily hours when DACT greater than zero 
    df['DACTgtZero']=np.where(df['DACT']>0.00001, 1, 0)
    df_daily['stressedHours'] = df['DACTgtZero'].groupby(pd.Grouper(freq='D')).sum()

    df_daily.reset_index(inplace=True)
 
    return df_daily

# %%
# Perform preprocessing steps on DataFrame, including calculating various columns based on existing columns
def preprocessing(df, a, b):
    df['LL'] = a + b * df['vpd']  # Define lower limit
    df['VPG'] = (df['es'] * (a + df['tair'])) - df['es'] * df['tair']  # Compute VPG
    df['UL_mod'] = (b - a) * abs(df['VPG'])  # ULmode
    df['UL'] = df['tbelow'] + 5  # Upper limit
    df['diff'] = df['tbelow'] - df['tair']
    df['CWSI'] = (abs(df['diff'] - df['LL']) / (df['UL_mod'] - df['LL']))

    window_size = 3 #moving averate of 3 hours
    #compute pseudo CWSI by computing slopes of diff/vpd
    df['diff_rolling_avg'] = df['diff'].rolling(window=window_size, min_periods=1).mean()
    df['vpd_rolling_avg'] = df['vpd'].rolling(window=window_size, min_periods=1).mean()
    df['pCWSI'] = df.apply(lambda x: x.diff_rolling_avg/x.vpd_rolling_avg, axis=1)

    #calculate DACT and DANS
    DACT_DANS(df, Tcritical)

    df_daily=pd.DataFrame()
    # calculate stress intensity
    df_daily = Stress_Intensity(df)

    return df, df_daily

def get_solar_time(longitude, utc_time):
    # Convert utc_time to pandas Timestamp object
    utc_time = pd.to_datetime(utc_time)
    # Calculate the hour offset based on the longitude
    hour_offset = round(longitude / 15)
    hour_offset = pd.to_timedelta(hour_offset, unit = 'h')
    # Apply the offset to the UTC time to get the solar time
    solar_time = utc_time + hour_offset

    return solar_time


def create_features(field_df):
# Calculate the 'tbelow-tair' column by subtracting 'tair' from 'tbelow'
    field_df['ref_tbelow-tair'] = field_df['ref_tbelow'] - field_df['tair']
    field_df['es'] = esat_(field_df['tair'])
    # field_df['tzconvert_time1'] = field_df['time'].dt.tz_convert('America/Los_Angeles')
    # field_df['fntrans_time'] = solar_noon_(field_df['time'], field_df['long'])
    field_df['solar_time'] = get_solar_time( field_df['long'], field_df['time'])
    field_df['solarnoon'] = (field_df['solar_time'].dt.hour >= 12) & (field_df['solar_time'].dt.hour < 16)
    field_df['solarnoon'] = field_df['solarnoon'].astype(int)
    return field_df

# %%
# %%
#dataframe is pulled using SQL query directly using: CWSI_Ref_Mark_Joined_data_ETL.py
bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/Joined_ref_df_hourly.csv'

# Load DataFrame from S3 bucket
joined_df = df_from_s3(path, bucket_name, format='csv')

# %%
# change columns names to use following processing program
field_df=joined_df.drop(['ref_time', 'body_temp'], axis=1)

# Create the 'tbelow-tair' and es
create_features(field_df)
# Drop rows with missing values
field_df.dropna(inplace=True) 

# %%
# %%
regline_list=[] # store regression parameters
# export hourly dataframe for CWSI related calculation
res_df = pd.DataFrame()

# create daily sum for DACT as a metric for stress hours and intensity for sites
res_daily= pd.DataFrame()

sites = field_df.site_id.unique().tolist()

#create CWSI
for site in sites:
    #select time window to compute m1_coef_, m1_intercept_
    for source in ['L1','L2', 'H1', 'H2']:
        solarnoon_mask=(field_df.solarnoon==1) & (field_df.time> '2023-06-01') & (field_df.time< '2023-06-29')
        # compute slope and interception for solarnoon on each device
        x, Y_pred, m1_coef_, m1_intercept_ = lr_vpd_tdif(field_df[(field_df['site_id'] == site) \
                                                                & (field_df['source'] == source) \
                                                                & solarnoon_mask][['vpd', 'ref_tbelow-tair']])
        plotmask=(field_df['site_id'] == site) & (field_df['source'] == source)
        _, _df_daily = preprocessing(field_df[plotmask], m1_coef_[0][0], m1_intercept_[0])
        res_df = pd.concat([res_df, _])
        res_daily = pd.concat([res_daily, _df_daily])
        print(site, source, m1_coef_[0][0], m1_intercept_[0], res_df.shape, res_daily.shape)
        regline_list.append([site, source, m1_coef_[0][0], m1_intercept_[0]])

res_df = res_df.drop(['diff', 'diff_rolling_avg', 'vpd_rolling_avg'], axis=1)
# regline_list

# %%
#reset index so the time index will be treated as a column in quicksight
res_df.reset_index(inplace=True)

# %%
# %%
bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/field_cwsi_trial_daily.csv'
# Save res_df to S3 bucket
df_to_s3(res_daily, path, bucket_name, format='csv')


# %%
bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/field_cwsi_trial.csv'
# Save res_df to S3 bucket
df_to_s3(res_df, path, bucket_name, format='csv')



# %%



