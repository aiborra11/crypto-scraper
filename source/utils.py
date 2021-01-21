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
        max_date {[str]} -- last day we want to have data collected. In case we do not provide amy it will get today's date.

    Returns:
    --------
        {[list]}
            list of dates we will use to collect data.

    """
    dates = []
    date_format = datetime.strptime(str(day1), '%Y%m%d')
    if max_date:
        max_date = max_date
    else:
        max_date = int(datetime.today().strftime('%Y%m%d'))

    for day in range(2500):
        next_day = str(date_format + timedelta(days=day))
        next_day_format = next_day.replace('-', '').split()[0]

        if int(next_day_format) <= int(max_date):
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

    cryptos_info = crypto.split('_')
    crypto_data = pd.DataFrame()
    warnings = []
    for date in tqdm(interval_to_update):
        try:
            # Scraping data
            dataset = pd.read_csv(
                f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{date}.csv.gz')
            # Cleaning the name of the cryptocurrency to use it as a filter
            crypto = [crypt for crypt in cryptos_info if crypt in dataset['symbol'].unique()][0]
            crypto_data = pd.concat([crypto_data, dataset[dataset['symbol'] == crypto]])
        except:
            # Adding dates we cannot get data and return it for warnings
            warnings.append(date)
    return crypto_data, warnings, crypto

