import pandas as pd

from .crypto_scraper import dates_converter, csv_creator


def data_obtainer(interval, name):
    for date in interval:
        print(date)
        dataset = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz')
        data_symb = dataset[dataset['symbol'] == 'XBTUSD']
        return csv_creator(data_symb, name)

def data_updator (date):
    #charging and cleaning
    print('Updating your data...')
    dates = dates_converter(date)
    data_update = pd.DataFrame(data_obtainer(dates[:], 'data_update'))




    previous_dataset = pd.read_csv('./data/data4gzip_XBTUSD.csv', compression = 'gzip')
    data_updated = pd.concat([previous_dataset, data_update])
    data_updated = data_updated.drop_duplicates('trdMatchID', keep='first')