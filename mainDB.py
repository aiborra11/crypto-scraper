from source.crypto_scraper_db import data_updator
from source.crypto_scraper_csv import dates_converter

from source.database import Database
from source.dataframe_creator import processData




def main(crypto, day_update):
    """Pipeline to execute the functions from the source.crypto_scraper_db and source.crypto_scraper_csv script.

    Arguments:
        crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
        day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.

    Returns:
        [gzip] -- gzip file stored in a mongo database named xbt. The file contains the data for the desired crypto and for the desired date.
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
        print('Enter 2 if you want to collect RAW data:')
        print('Enter 3 if you want to collect PROCESSED data:')
        print('Enter 4 if you want to delete a collection:')
        print('Enter 5 to check date warnings:')
        print('Enter 6 to exit:')

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
            rawData = database.getRawData()
            print(f'Your Raw Data has {len(rawData)} rows.')
            print(rawData.head())
            print(rawData.tail())

            processedData = processData(rawData)
            dataFrame = processedData.createDataFrame()

            print(f'Your Processed Data has been stored in your data folder.')

        elif userChoice is 4:
            database = Database()
            deletedColl = database.removeCollection()
            print(f'The collection has been removed successfully from the database.')

        elif userChoice is 5:
            dates = dates_converter('20141122')
            database = Database()
            warnings = database.datesDoubleCheck(dates)
            print('You should double-check the data for these dates: ', warnings)


        elif userChoice is 6:
            print('Goodbye!')
            quit()


        else:
            print("\n\nOops! That is not a valid number. Please try a number between 1-6 to proceed:\n")



