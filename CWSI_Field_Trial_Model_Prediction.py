
import sys
import pandas as pd
path_to_calval_etl = '/home/ec2-user/SageMaker/calval-etl'
sys.path.append(path_to_calval_etl)
from src.math import NDVI_, EVI_, crop_kH, SlpInt, lr_vpd_tdif, esat_, upper_limit, lower_limit

import pandas as pd
from sklearn.datasets import make_regression
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import seaborn as sns


import boto3
from botocore.session import Session
from utils import  df_from_s3, df_to_s3
session = Session()
import json
import pandas.io.sql as psql
import datetime as dt
import numpy as np
import warnings
warnings.simplefilter(action="ignore")


# %%
def process_ET(df):
    df_p = df.copy() ## create a copy of df
    ## convert df into half hour
    df_p['local_time'] = df_p['local_time'].astype('datetime64[ns]').dt.round('30min')

    ## convert some columns into numeric
    cols= [i for i in df_p.columns if i not in ['local_time','device','treatment', 'location', 'update_time', 'create_time']]
    for col in cols:
        df_p[col] = pd.to_numeric(df_p[col], errors ='coerce')

    ## group by to  take average for each 30 min   
    df_p1 = df_p.groupby('local_time').mean().reset_index()
    
    return df_p1


def halfhour2hourly(df):
    ## rounding to day
    df['local_time'] = pd.to_datetime(df['local_time'].dt.date)

    ## use aggregate function to average into daily
    agg_func = {'tair': 'mean',  
                'tbelow': 'mean', 
                'CanopyTemp': 'mean',
                'ref_tbelow' : 'mean', 
                'ndvi': 'mean',
                'evi': 'mean',
                'Sapflow(mm/day)': 'mean', 
                'Dt': 'mean', 
                'Dr': 'mean', 
                'Gt': 'mean', 
                'Gc': 'mean' } 
    df['local_time'] = df['local_time'].dt.floor(freq='D')
    df_daily = df.groupby('local_time').agg(agg_func).reset_index()
   
    return df_daily

def preprocessing(df, a, b):
    df['LL'] = a + b * df['vpd'] #define lower limit

    df['VPG'] = (df['es'] * (a + df['tair'] )) - df['es']  * df['tair'] #compute VPG

    df['UL_mod'] =(b-a) * abs(df['VPG']) #ULmode

    df['UL'] =df['tbelow'] + 5 #upper limit

    df['diff'] = df['tbelow'] - df['tair']

    df['CWSI'] = (abs(df['diff'] - df['LL'])/(df['UL_mod'] - df['LL']))
    
    return df


bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/ET_mark_df_hourly.csv' 
field_df=df_from_s3(path, bucket_name, format ='csv')


GB_devices=['D003701', 'D003705', 'D003932', 'D003978']
BV_devices=['D003898', 'D003960','D003942', 'D003943']

HighStress =['D003942', 'D003943', 'D003932', 'D003978']
LowStress =['D003701', 'D003705','D003898', 'D003960',]


field_df['tbelow-tair']=field_df['tbelow']-field_df['tair']
field_df['es'] =esat_(field_df['tair'] )
field_df['site']= np.where(field_df.device.isin(GB_devices), 'GB', 'BV')
field_df['StressLevel']= np.where(field_df.device.isin(HighStress), 'HS', 'LS')

field_df.dropna(inplace=True)

res_df=pd.DataFrame()
sites=field_df.site.unique().tolist()
for site in sites:
    x, Y_pred, m1_coef_, m1_intercept_= lr_vpd_tdif(field_df[field_df['site']==site][['vpd', 'tbelow-tair']])
    _=preprocessing(field_df[field_df['site']==site], m1_coef_[0][0], m1_intercept_[0]) 
    res_df=pd.concat([res_df, _])
res_df=res_df.drop(['diff'],axis=1)

bucket_name = 'arable-adse-dev'
path = f'Carbon Project/Stress Index/UCD_Almond/field_cwsi_trial.csv' #ET{device}_mark_df_daily.csv
df_to_s3(res_df, path, bucket_name, format ='csv')



