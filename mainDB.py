from source.crypto_scraper_db import data_updator
from source.crypto_scraper_csv import dates_converter

from source.database import Database




def main(crypto, day_update):
    """Pipeline to execute the functions from the source.crypto_scraper and source.crypto_scraper_updator script.

    Arguments:
        crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
        day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.

    Returns:
        [gzip] -- gzip file containing the data for the desired crypto and the desired dates.
    """
    print('Preparing your data...')
    dates = dates_converter(day_update)
    print('Charging data to update...')
    data_updator(dates, crypto)




if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY THESE STRINGS TO COLLECT THE DESIRED DATA
    
    """

    # raw_data = Database().initialize('btc_data')[-5]
    raw_data = Database().initialize('xbt')
    print('Updating from: ', raw_data[-1])


    main(crypto='XBTUSD', day_update=raw_data[-1])

