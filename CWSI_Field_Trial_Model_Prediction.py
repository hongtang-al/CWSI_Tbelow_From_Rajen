
# %%
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

# %%
# Perform preprocessing steps on DataFrame, including calculating various columns based on existing columns
def preprocessing(df, a, b):
    df['LL'] = a + b * df['vpd']  # Define lower limit
    df['VPG'] = (df['es'] * (a + df['tair'])) - df['es'] * df['tair']  # Compute VPG
    df['UL_mod'] = (b - a) * abs(df['VPG'])  # ULmode
    df['UL'] = df['tbelow'] + 5  # Upper limit
    df['diff'] = df['tbelow'] - df['tair']
    df['CWSI'] = (abs(df['diff'] - df['LL']) / (df['UL_mod'] - df['LL']))
    return df

def create_features(field_df):
# Calculate the 'tbelow-tair' column by subtracting 'tair' from 'tbelow'
    field_df['ref_tbelow-tair'] = field_df['ref_tbelow'] - field_df['tair']
    field_df['es'] = esat_(field_df['tair'])
    return field_df
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
res_df = pd.DataFrame()
sites = field_df.site_id.unique().tolist()
# %%
for site in sites:
    x, Y_pred, m1_coef_, m1_intercept_ = lr_vpd_tdif(field_df[field_df['site_id'] == site][['vpd', 'ref_tbelow-tair']])
    _ = preprocessing(field_df[field_df['site_id'] == site], m1_coef_[0][0], m1_intercept_[0])
    res_df = pd.concat([res_df, _])

res_df = res_df.drop(['diff'], axis=1)
res_df 
# %%
bucket_name = 'arable-adse-dev'
path = 'Carbon Project/Stress Index/UCD_Almond/field_cwsi_trial.csv'
# Save res_df to S3 bucket
df_to_s3(res_df, path, bucket_name, format='csv')

# %%
res_df
# %%
res_df.to_csv('./data/cwsi_joined.csv')
# %%
