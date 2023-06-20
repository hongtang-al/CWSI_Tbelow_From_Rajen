
# %%
# !pip install plotly
# !pip install nbformat --upgrade
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

# Import specific functions from src.math module
from src.math import NDVI_, EVI_, crop_kH, SlpInt, lr_vpd_tdif, esat_, upper_limit, lower_limit
from utils import df_from_s3, df_to_s3

session = Session()
# define critical temperature for DACT calculation
Tcritical = 20


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
regline_list

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
res_df.to_csv('./data/cwsi_joined.csv')
res_daily.to_csv('./data/cwsi_joined_daily.csv')

# %%
# Visualize the data and trend line
import plotly.express as px
def soloarnoon_vpd_delta(res_df, interception,slope ):
    # Assuming you have a DataFrame called res_df with columns 'vpd' and 'ref_tbelow-tair'
    fig = px.scatter(res_df.loc[res_df.solarnoon==1], x='vpd', y='ref_tbelow-tair', color='source')
    fig.add_trace(px.line(x=[res_df['vpd'].min(), res_df['vpd'].max()],
                        y=[interception + slope * res_df['vpd'].min(),
                            interception + slope * res_df['vpd'].max()]).data[0])
    site_id_title = ' '.join(res_df['site_id'].unique()[:2])
    fig.update_layout(title=f"Scatter Plot - Site IDs: {site_id_title}")
    fig.show()

for item in regline_list:
    print(item)
    df = res_df.loc[(res_df['site_id']==item[0]) & (res_df['source']==item[1])]
    soloarnoon_vpd_delta(df, item[3],item[2] )


# %%
# plot all regression lines in one plot
import plotly.graph_objects as go

# Create an empty figure
fig = go.Figure()

for item in regline_list:
    df = res_df.loc[(res_df['site_id']==item[0]) & (res_df['source']==item[1])]

    # Add a scatter trace for each group of data
    # fig.add_trace(
    #     go.Scatter(
    #         x=df['vpd'],
    #         y=df['ref_tbelow-tair'],
    #         mode='markers',
    #         name=f"site_id: {item[0]}, source: {item[1]}"
    #     )
    # )
    # Add a line trace for the regression line
    fig.add_trace(
        go.Scatter(
            x=[df['vpd'].min(), df['vpd'].max()],
            y=[item[3] + item[2] * df['vpd'].min(),
               item[3] + item[2] * df['vpd'].max()],
            mode='lines',
            name=f"Regression Line: site_id {item[0]}, source: {item[1]}"
        )
    )

fig.update_layout(title="Scatter Plot for all Site IDs")
fig.show()

# %%
# Plot stairstep plot for daily DACT and StressedHours
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_daily(res_daily, devices, feature): 
    # Create subplots with shared x-axis
    fig = make_subplots(rows=1, cols=1, shared_xaxes=True)

    grouped_data = res_daily.groupby('device')

    # Iterate over each device group and add a stair-step plot for DACT_daily
    for device, data in grouped_data:
        if device in devices:
            fig.add_trace(go.Scatter(x=data['time'], y=data[feature], name=device, mode='lines+markers', connectgaps=True, line_shape='hv'))

    # Update the layout
    fig.update_layout(
        showlegend=True,
        legend_title_text='Device',
        width=16*80,  # Convert inches to pixels (1 inch = 80 pixels)
        height=6*80  # Convert inches to pixels (1 inch = 80 pixels)
    )

    # Set x-axis and y-axis labels
    fig.update_xaxes(title_text='Time')
    fig.update_yaxes(title_text=feature)

    # Show the plot
    fig.show()

# plot BV2 sites for quality check

devices=['D003942', 'D003943', 'D003898', 'D003960']
feature='DACT_daily'
plot_daily(res_daily, devices, feature)
# %%
feature='stressedHours'
plot_daily(res_daily, devices, feature)

# %%
