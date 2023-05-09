 # Define function to calculate saturation vapor pressure
def esat_(xT):
    return (617.4 + 42.22 * xT + 1.675 * xT ** 2 + 0.01408 * xT ** 3 + 0.0005818 * xT ** 4) / 1000

# Define function to calculate slope and intercept of linear regression model
def SlpInt(df):
    # Convert 'time' column to datetime object
    df['time'] = pd.to_datetime(df['time'])
    # Filter rows with dates between June 1, 2021 and July 30, 2021
    mask = (df['date'] > '2021-06-01') & (df['date'] <= '2021-07-30')
    df = ET75_daily.loc[mask_m]
    # Create new dataframe with columns 'vpd' and 'diff'
    df_sub = {'vpd': df['vpd'], 'diff': df['ref_tbelow'] - df['tair']}
    df_sub = pd.DataFrame.from_dict(df_sub)
    # Remove rows with NaN values in 'diff' column
    df_sub = df_sub[df_sub['diff'].notna()]
    # Split data into X and Y variables
    x = df_sub.iloc[:, 0].values.reshape(-1, 1)
    y = df_sub.iloc[:, 1].values.reshape(-1, 1)
    # Fit linear regression model and calculate slope and intercept
    lr = LinearRegression()
    m1 = lr.fit(x, y)
    b = m1.intercept_
    a = m1.coef_
    return a, b

# define function to calculate esat
def esat_(temp):
    return 0.6108 * np.exp((17.27 * temp) / (temp + 237.3))

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

# ##### calculate lower limit

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

# Define functions to calculate NDVI, EVI, and crop_kH and apply them to the dataframe
def NDVI_(df):
    df['ndvi'] = (df['b6uw'] - df['b4uw']) / (df['b6uw'] + df['b4uw'])
    return df

def EVI_(df):
    df['evi'] = 2.5 *(df['b6uw'] - df['b4uw']) / (df['b6uw'] + (6 * df['b4uw'] - 7.5 * df['b1uw']) +1)
    return df

def  crop_kH(df):
    df['kcb_hr'] =  0.176 + 1.325 * df['ndvi'] - 1.466 * df['ndvi'] ** 2 + 1.146 * df['ndvi'] ** 3
    df['new_etc_hr'] = df['et'] * d_f ["kcb_hr"]
    return df
