import pandas as pd

from source.crypto_scraper import csv_creator


def data_obtainer(interval, crypto, name):
    for date in interval:
        print(date)
        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        data_symb = dataset[dataset['symbol'] == crypto]
        return csv_creator(data_symb, crypto, name)

