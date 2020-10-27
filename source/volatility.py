import pandas as pd
import numpy as np
import math
from datetime import datetime

def volatility(df):
    prices = pd.DataFrame(df).rename(columns={0: 'Close'})
    prices['Close'] = prices['Close'].ffill()
    prices['LagClose'] = prices['Close'].shift(1)
    volatility = ((np.log(prices['LagClose']/prices['Close'])).std() * math.sqrt(1440))
    return volatility


def get_volatility(data, frequency, window=1440):
    have_volatility = [x for x in data.columns if x == 'Volatility']
    if have_volatility:
        print('There is volatility!')
        data_nan = data[window:]
        index = data_nan[data_nan['Volatility'].isnull()].index.tolist()[0]
        print('First NaN value in volatility column is at position: ', index)

        if index:
            df = pd.DataFrame(data['Timestamp'][(index - window):])
            vol = (data['Close'][(index - window):]).rolling(window, min_periods=window).apply(volatility)
            df = pd.concat([df.reset_index(drop=True), vol.reset_index(drop=True)], axis=1)\
                                                                        .rename(columns={'Close': 'Volatility'})

            data_vol = data.merge(df, right_on='Timestamp', left_on='Timestamp', how='left')

            data_vol['Volatility_x'] = data_vol['Volatility_x'].fillna(0)
            data_vol['Volatility_y'] = data_vol['Volatility_y'].fillna(0)
            data_vol['Volat'] = data_vol['Volatility_x'] + data_vol['Volatility_y']

            data_vol['Timestamp'] = data_vol['Timestamp'].map(lambda t: datetime.strptime(str(t), '%Y-%m-%d %H:%M:%S'))
            data_vol = data_vol.set_index('Timestamp')

            return data_vol, data_vol['Volat'].groupby(pd.Grouper(freq=frequency)).mean()

    else:
        data['Volatility'] = data['Close'].rolling(window, min_periods=window).apply(volatility)

        return data.groupby(pd.Grouper(freq=frequency))[data.columns]

vol1 = get_volatility(data, '5min')
vol1.tail()