import pandas as pd

from tqdm import tqdm
from datetime import datetime

from .database import DatabaseUpdator


def datetimeConverter(df):
    """
    Converts into datetime format.

    Arguments:
    ----------
        df {[dataframe]} -- Dataframe and column.

    Returns:
    --------
        {[dataframe]}
            Dataframe with the column converted into datetime.

    """

    df['timestamp'] = df.timestamp.map(lambda t: datetime.strptime(t[:-3], '%Y-%m-%dD%H:%M:%S.%f'))
    return df


def data_updator(interval):
    """
    Iterates through a list of dates scraping the data for the specified cryptocurrency.

    Arguments:
    ----------
        interval {[list]} -- Interval of dates we are willing to collect.
        crypto {[str]} -- Cryptocurrency name we are willing to collect.

    Returns:
    --------
        {[dataset]}
            Dataset stored in mongo for a specific date and crypto.

    """


    print('Interval to be scrapped:', interval[:-1])
    cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{interval[-2]}.csv.gz')

    print('\n\nChoose from the list below the cryptocurrency pair you are interested:\n', cryptos['symbol'].unique())
    crypto = str(input()).upper()

    for date in tqdm(interval[:-1]):
        print(f'{date} is being processed...')
        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        crypto_data = dataset[dataset['symbol'] == crypto]
        crypto_data_indexed = datetimeConverter(crypto_data)

        DatabaseUpdator.updateDatabase(str(date), crypto_data_indexed)