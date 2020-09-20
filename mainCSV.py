import time
import pandas as pd

from source.crypto_scraper_csv import dates_converter, data_obtainer


def main(day1):
    """
    Pipeline to execute the functions from the source.crypto_scraper script.

    Arguments:
    ----------
        crypto {[str]} -- crypto we are interested in collecting data.
        day1 {[str]} -- date from which we want to start to collect data.

    Returns:
    --------
        {[gzip]}
            File containing the data for the desired crypto and the desired dates.

    """

    cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{day1}.csv.gz')

    print('\n\nChoose from the list below the cryptocurrency pair you are interested:\n', cryptos['symbol'].unique())
    crypto = str(input()).upper()

    print('Preparing your data...')
    dates = dates_converter(day1)
    print('Charging first csv...')
    pd.DataFrame(data_obtainer(dates[:500], crypto, 'data1gzip'))
    time.sleep(15)
    print('Charging second csv...')
    pd.DataFrame(data_obtainer(dates[500:1000], crypto, 'data2gzip'))
    print('Charging third csv...')
    time.sleep(15)
    pd.DataFrame(data_obtainer(dates[1000:1500], crypto, 'data3gzip'))
    print('Charging fourth csv...')
    time.sleep(15)
    pd.DataFrame(data_obtainer(dates[1500:], crypto, 'data4gzip'))