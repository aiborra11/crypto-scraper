import pandas as pd
from datetime import datetime

from .database import DatabaseUpdator


def datetimeConverter(df):
    df['timestamp'] = df.timestamp.map(lambda t: datetime.strptime(t[:-3], '%Y-%m-%dD%H:%M:%S.%f'))
    # data_indexing = df.set_index('timestamp')
    return df

def data_updator(interval, crypto):
    """Iterates through the dates list collecting the data for the specified cryptocurrency.
    The dates list contains the dates from which we are interested in collecting data.

    Arguments:
        interval {[list]} -- interval of dates we are going to collect.
        crypto {[str]} -- crypto data we are willing to collect.

    Returns:
        [dataset] -- dataset stored in mongo for a specific date
    """


    print('Interval to be scrapped:', interval[:-1])
    for date in interval[:-1]:
        print(f'{date} is being processed...')

        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        crypto_data = dataset[dataset['symbol'] == crypto]
        crypto_data_indexed = datetimeConverter(crypto_data)

        DatabaseUpdator.updateDatabase(str(date), crypto_data_indexed)

