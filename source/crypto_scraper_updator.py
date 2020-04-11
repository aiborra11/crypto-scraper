import pandas as pd

from source.crypto_scraper import csv_creator


def data_obtainer(interval, crypto, name):
    """Iterates through the dates list collecting the data for the specified cryptocurrency.
    The dates list contains the dates from which we are interested in collecting data.
    Arguments:
        interval {[list]} -- interval of dates we are going to collect.
        name {[str]} -- how we want to name our csv file.
        crypto {[str]} -- crypto data we are willing to collect.

    Returns:
        [csv] -- gzip file with the data for the desired cryptocurrency.
    """
    for date in interval:
        print(date)
        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        data_symb = dataset[dataset['symbol'] == crypto]
        return csv_creator(data_symb, crypto, name)

