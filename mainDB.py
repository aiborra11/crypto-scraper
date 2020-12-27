from source.crypto_scraper_db import data_updator
from source.crypto_scraper_csv import dates_converter

from source.database import Database
from source.dataframe_creator import get_data

from tqdm import tqdm
import pandas as pd

# 20190303


def main(day_update, max_date):
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
    dates_norm = dates_converter(day_update, max_date)
    print('Charging data to update...')
    data_updator(dates_norm)


if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY 'XBTUSD' TO COLLECT DATA FROM A DIFFERENT CRYPTO
    
    """

    while True:
        print('\nEnter 1 if you want to update/create your database:')
        print('Enter 2 if you want to collect RAW data as a csv file:')
        print('Enter 3 if you want to collect PROCESSED data as a csv file:')
        print('Enter 4 if you want to DELETE a collection from your database:')
        print('Enter 5 if you want to check the current available collections in your database:')
        print('Enter 6 to check date warnings:')
        print('Enter 7 to exit:')

        userChoice = int(input())

        if userChoice == 1:
            database = Database()
            print('Write "yes" to update since your last record or write a date in a format "YYMMDD" '
                  'to update since there.')
            print('In case you want to update until a certain date, write until')

            update_since = str(input())
            if update_since == 'yes':
                available_data = database.current_data()
                # update_from = database.showAvailableData()[0]
                main(day_update=available_data[-1], max_date='')

            elif update_since == 'until':
                print('Write the date until the one you would like to update your db (YYMMDD): ')
                update_until = str(input())
                available_data = database.current_data()
                main(day_update=20141128, max_date=int(update_until))

            else:
                main(day_update=update_since, max_date='')

        elif userChoice == 2:
            database = Database()
            rawData = database.show_available_data()[0]
            for raw in tqdm(rawData):
                df_raw = pd.DataFrame(Database.DATABASE[raw].find({}))
                print('Preparing your RAW data for: ', raw)
                df_raw.to_csv(f'data.nosync/RAW_{raw}.gz', compression='gzip')

        elif userChoice == 3:
            database = Database()
            rawData = database.show_available_data()
            if rawData[1] == '':
                print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
                frequency = str(input()).replace(' ', '')
            else:
                frequency = rawData[1]

            df_raw = pd.DataFrame()
            for num, raw in enumerate(tqdm(rawData[0])):
                df_raw = pd.concat([df_raw, pd.DataFrame(Database.DATABASE[raw].find({}))])
                # Grouping collections and generating df by groups to improve performance.
                if (num % 100 == 0) & (num > 0):
                    get_data(df_raw, frequency)
                    df_raw = pd.DataFrame()

                elif raw == rawData[0][-1]:
                    print('collecting data...')
                    get_data(df_raw, frequency)

                else:
                    pass

        elif userChoice == 4:
            database = Database()
            deletedColl = database.remove_collection()
            print(f'The collection has been removed successfully from the database.')

        elif userChoice == 5:
            database = Database()
            currData = database.current_data()

        elif userChoice == 6:
            dates = dates_converter('20141122')
            database = Database()
            warnings = database.dates_double_check(dates)
            print('You should double-check the data for these dates: ', warnings)

        elif userChoice == 7:
            print('Goodbye!')
            quit()

        else:
            print("\n\nOops! That is not a valid number. Please try a number between 1-6 to proceed:\n")
