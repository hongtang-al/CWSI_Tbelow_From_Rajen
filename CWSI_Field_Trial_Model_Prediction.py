
# %%
import sys
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import seaborn as sns
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

# Import specific functions from src.math module
from src.math import NDVI_, EVI_, crop_kH, SlpInt, lr_vpd_tdif, esat_, upper_limit, lower_limit
from utils import df_from_s3, df_to_s3

session = Session()

# Process ET DataFrame by converting columns to appropriate data types and taking the average for each 30-minute interval
def process_ET(df):
    df_p = df.copy()
    df_p['local_time'] = df_p['local_time'].astype('datetime64[ns]').dt.round('30min')
    cols = [i for i in df_p.columns if i not in ['local_time', 'device', 'treatment', 'location', 'update_time', 'create_time']]
    for col in cols:
        df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
    df_p1 = df_p.groupby('local_time').mean().reset_index()
    return df_p1


# Convert half-hourly DataFrame to hourly DataFrame by rounding to day and taking the average for each day
def halfhour2hourly(df):
    df['local_time'] = pd.to_datetime(df['local_time'].dt.date)
    agg_func = {
        'tair': 'mean',
        'tbelow': 'mean',
        'CanopyTemp': 'mean',
        'ref_tbelow': 'mean',
        'ndvi': 'mean',
        'evi': 'mean',
        'Sapflow(mm/day)': 'mean',
        'Dt': 'mean',
        'Dr': 'mean',
        'Gt': 'mean',
        'Gc': 'mean'
    }
    df['local_time'] = df['local_time'].dt.floor(freq='D')
    df_daily = df.groupby('local_time').agg(agg_func).reset_index()
    return df_daily


# Perform preprocessing steps on DataFrame, including calculating various columns based on existing columns
def preprocessing(df, a, b):
    df['LL'] = a + b * df['vpd']  # Define lower limit
    df['VPG'] = (df['es'] * (a + df['tair'])) - df['es'] * df['tair']  # Compute VPG
    df['UL_mod'] = (b - a) * abs(df['VPG'])  # ULmode
    df['UL'] = df['tbelow'] + 5  # Upper limit
    df['diff'] = df['tbelow'] - df['tair']
    df['CWSI'] = (abs(df['diff'] - df['LL']) / (df['UL_mod'] - df['LL']))
    return df

# %%
bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/ET_mark_df_hourly.csv'

# Load DataFrame from S3 bucket
field_df = df_from_s3(path, bucket_name, format='csv')

# %%
GB_devices=['D003701', 'D003705', 'D003932', 'D003978']
BV_devices=['D003898', 'D003960','D003942', 'D003943']

HighStress =['D003942', 'D003943', 'D003932', 'D003978']
LowStress =['D003701', 'D003705','D003898', 'D003960',]

# Calculate the 'tbelow-tair' column by subtracting 'tair' from 'tbelow'
field_df['tbelow-tair'] = field_df['tbelow'] - field_df['tair']
field_df['es'] = esat_(field_df['tair'])
# Assign site based on the 'device' column
field_df['site'] = np.where(field_df.device.isin(GB_devices), 'GB', 'BV')
# Assign StressLevel based on the 'device' column
field_df['StressLevel'] = np.where(field_df.device.isin(HighStress), 'HS', 'LS')

# Drop rows with missing values
field_df.dropna(inplace=True)
# %%
res_df = pd.DataFrame()
sites = field_df.site.unique().tolist()

for site in sites:
    x, Y_pred, m1_coef_, m1_intercept_ = lr_vpd_tdif(field_df[field_df['site'] == site][['vpd', 'tbelow-tair']])
    _ = preprocessing(field_df[field_df['site'] == site], m1_coef_[0][0], m1_intercept_[0])
    res_df = pd.concat([res_df, _])

res_df = res_df.drop(['diff'], axis=1)

bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/field_cwsi_trial.csv'
# Save res_df to S3 bucket
df_to_s3(res_df, path, bucket_name, format='csv')


# %%
res_df
# %%
