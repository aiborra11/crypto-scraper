from source.crypto_scraper_db import data_updator
from source.crypto_scraper_csv import dates_converter

from source.database import Database




def main(crypto, day_update):
    """Pipeline to execute the functions from the source.crypto_scraper_db and source.crypto_scraper_csv script.

    Arguments:
        crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
        day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.

    Returns:
        [gzip] -- gzip file stored in a mongoDB named xbt containing the data for the desired crypto and for the desired dates.
    """


    print('Preparing your data...')
    dates = dates_converter(day_update)

    print('Charging data to update...')
    data_updator(dates, crypto)




if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY 'XBTUSD' TO COLLECT DATA FROM A DIFFERENT CRYPTO
    
    """

    while True:
        print('Enter 1 if you want to update your database:')
        print('Enter 2 if you want to collect raw data:')
        print('Enter 3 if you want to delete a collection:')
        print('Enter 4 to exit:')

        userChoice = int(input())

        if userChoice is 1:
            database = Database()
            update_from = database.showAvailableData()[-1]
            main(crypto='XBTUSD', day_update=update_from)

        elif userChoice is 2:
            database = Database()
            rawData = database.getRawData()
            print(rawData.head())
            print(rawData.tail())
            print(f'Your dataset has {len(rawData)} rows.')

        elif userChoice is 3:
            database = Database()
            deletedColl = database.removeCollection()
            print(f'The collection has been removed successfully from the database.')

        elif userChoice is 4:
            print('Goodbye!')
            quit()

        else:
            print("\n\nSorry, I couldn't understand you. Please select a number between 1-4 to proceed:\n")

