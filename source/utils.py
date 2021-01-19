import pandas as pd

from tqdm import tqdm
from datetime import datetime, timedelta


def interval_to_scrape(day1='20141122', max_date=''):
    """
    Taking the first available date (if anything else is specified when calling the function) and converts it
    into dateformat to add one day and iterate every csv file in the website.

    Arguments:
    ----------
        day1 {[str]} -- date from which we want to start to collect data.

    Returns:
    --------
        {[list]}
            list of dates we will use to collect data.

    """

    dates = []
    date_format = datetime.strptime(str(day1), '%Y%m%d')
    if max_date:
        for day in range(2500):
            next_day = str(date_format + timedelta(days=day))
            next_day_format = next_day.replace('-', '').split()[0]

            if int(next_day_format) <= int(max_date):
                dates.append(next_day_format)

        return dates

    else:
        max_date = int(datetime.today().strftime('%Y%m%d'))
        for day in range(2500):
            next_day = str(date_format + timedelta(days=day))
            next_day_format = next_day.replace('-', '').split()[0]

            if int(next_day_format) <= max_date:
                dates.append(next_day_format)

        return dates

def data_scraper(interval_to_update, crypto=''):
    """
    Iterates through a list of dates scraping the data for the specified cryptocurrency.

    Arguments:
    ----------
        interval_to_update {[list]} -- Interval of dates we are willing to collect.
        crypto {[str]} -- Cryptocurrency name we are willing to collect.

    Returns:
    --------
        {[dataset]}
            Dataset stored in mongo for a specific date and crypto.

    """

    # crypto = crypto.split('_')[1] if len(crypto.split('_')) > 2 else crypto.split('_')[0]
    cryptos_info = crypto.split('_')
    crypto_data = pd.DataFrame()
    for date in tqdm(interval_to_update):
        try:
            dataset = pd.read_csv(
                f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{date}.csv.gz')
            crypto = [crypt for crypt in cryptos_info if crypt in dataset['symbol'].unique()][0]
            crypto_data = pd.concat([crypto_data, dataset[dataset['symbol'] == crypto]])
        except:
            print(f'No available data for {crypto} at this date.')
            # return None
    return crypto_data

