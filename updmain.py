from source.crypto_scraper_updator import dataUpdator
from source.crypto_scraper import datesConverter



def main(crypto, day_update):
    """Pipeline to execute the functions from the source.crypto_scraper and source.crypto_scraper_updator script.

    Arguments:
        crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
        day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.

    Returns:
        [gzip] -- gzip file containing the data for the desired crypto and the desired dates.
    """
    print('Preparing your data...')
    dates = datesConverter(day_update)
    print('Charging data to update...')
    dataUpdator(dates, crypto)




if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY THESE STRINGS TO COLLECT THE DESIRED DATA
    
    """

    main(crypto='XBTUSD', day_update='20200625')