import pandas as pd

from .database import Database


def dataUpdator(interval, crypto):
    """Iterates through the dates list collecting the data for the specified cryptocurrency.
    The dates list contains the dates from which we are interested in collecting data.

    Arguments:
        interval {[list]} -- interval of dates we are going to collect.
        crypto {[str]} -- crypto data we are willing to collect.

    Returns:
        [csv] -- gzip file with the data for the desired cryptocurrency.
    """

    Database().initialize('btc_data')

    print('Interval to be scrapped:', interval)
    for date in interval[:-1]:
        print(f'{date} is being processed...')
        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        cryptoData = dataset[dataset['symbol'] == crypto]
        Database().insert(str(date), cryptoData)

