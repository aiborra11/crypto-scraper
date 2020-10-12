from source.crypto_scraper_db import data_updator
from source.crypto_scraper_csv import dates_converter

from source.database import Database
from source.dataframe_creator import get_data


from tqdm import tqdm
import pandas as pd




def main(day_update):
    """
    Pipeline to execute the functions from the source.crypto_scraper_db and source.crypto_scraper_csv script.

    Arguments:
    ----------
        crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
        day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.

    Returns:
    --------
        [gzip]
            File stored in a mongo database containing the data for the desired crypto and the desired date.

    """


    print('Preparing your data...')
    dates = dates_converter(day_update)

    print('Charging data to update...')
    data_updator(dates)




if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY 'XBTUSD' TO COLLECT DATA FROM A DIFFERENT CRYPTO
    
    """

    while True:
        print('\nEnter 1 if you want to update your database:')
        print('Enter 2 if you want to collect RAW data as a csv file:')
        print('Enter 3 if you want to collect PROCESSED data as a csv file:')
        print('Enter 4 if you want to delete a collection from your database:')
        print('Enter 5 if you want to check the current available collections in your database:')
        print('Enter 6 to check date warnings:')
        print('Enter 7 to exit:')

        userChoice = int(input())

        if userChoice == 1:
            database = Database()
            # print(database)
            available_data = database.currentData()
            # update_from = database.showAvailableData()[0]
            main(day_update=available_data[-1])


        elif userChoice == 2:
            database = Database()
            rawData = database.showAvailableData()[0]
            for raw in tqdm(rawData):
                df_raw = pd.DataFrame(Database.DATABASE[raw].find({}))
                print('Preparing your RAW data for: ', raw)
                df_raw.to_csv(f'data.nosync/RAW_{raw}.gz', compression='gzip')


        elif userChoice == 3:
            database = Database()
            rawData = database.showAvailableData()
            print(rawData)
            if rawData[1] == '':
                print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
                frequency = str(input()).replace(' ', '')
            else:
                frequency = rawData[1]

            for raw in tqdm(rawData[0]):
                df_raw = pd.DataFrame(Database.DATABASE[raw].find({}))
                if df_raw.empty:
                    print(f'There is no data for {raw}. Deleting this collection')
                    database.removeCollection(raw)
                else:
                    get_data(df_raw, frequency)

            # csv_file = processedData.create_csv(dataFrame)

        elif userChoice == 4:
            database = Database()
            deletedColl = database.removeCollection()
            print(f'The collection has been removed successfully from the database.')

        elif userChoice == 5:
            database = Database()
            currData = database.currentData()

        elif userChoice == 6:
            dates = dates_converter('20141122')
            database = Database()
            warnings = database.datesDoubleCheck(dates)
            print('You should double-check the data for these dates: ', warnings)


        elif userChoice == 7:
            print('Goodbye!')
            quit()


        else:
            print("\n\nOops! That is not a valid number. Please try a number between 1-6 to proceed:\n")



