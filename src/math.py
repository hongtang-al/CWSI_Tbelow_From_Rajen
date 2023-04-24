import io
import json
import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import math

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
            0.0005818 * xT ** 4) 

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