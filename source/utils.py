import pandas as pd
from tqdm import tqdm

# from source.database import DatabaseUpdator


# class DatabasePopulator():
#     def __init__(self):
#         pass

def data_scraper(date, crypto=''):
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

    # for date in tqdm(interval[:-1]):
    #     print(f'{date} is being processed...')
    try:
        dataset = pd.read_csv(
            f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{date}.csv.gz')
        crypto_data = dataset[dataset['symbol'] == crypto]

        return crypto_data
        # print(crypto_data)

        # DatabaseUpdator.update_database(str(date), crypto_data, crypto)
    except:
        print(f'No available data for {crypto} at this date.')
        return None