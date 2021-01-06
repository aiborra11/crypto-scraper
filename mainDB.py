# from source.crypto_scraper_db import data_scraper
# from source.crypto_scraper_csv import interval_to_scrape
from source.database import Database
from source.dataframe_creator import ProcessData

# from database import Database
from source.dataframe_creator import get_data
# from source.csv_generator import CsvGenerator

from tqdm import tqdm
import pandas as pd

# 20190303


# def main(day_update, max_date):
#     """
#     Pipeline to execute the functions from the source.crypto_scraper_db and source.crypto_scraper_csv script.
#
#     Arguments:
#     ----------
#         crypto {[str]} -- crypto we are interested in collecting data. Feel free to modify.
#         day_update {[str]} -- date from which we want to start to collect data. Feel free to modify.
#
#     Returns:
#     --------
#         [gzip]
#             File stored in a mongo database containing the data for the desired crypto and the desired date.
#
#     """
#
#     print('Preparing your data...')
#     dates_norm = interval_to_scrape(day_update, max_date)
#     print('Charging data to update...')
#     data_scraper(dates_norm)


if __name__ == "__main__":
    """
    
    FEEL FREE TO MODIFY 'XBTUSD' TO COLLECT DATA FROM A DIFFERENT CRYPTO
    
    """

    while True:
        print('\nEnter 1 if you want to connect/create your database:')
        print('Enter 2 if you want to collect RAW data as a csv file:')
        print('Enter 3 if you want to collect PROCESSED data as a csv file:')
        print('Enter 4 if you want to DELETE a collection from your database:')
        print('Enter 5 if you want to check the current available collections in your database:')
        print('Enter 6 to check date warnings:')
        print('Enter 7 to exit:')
        available_options = [1, 2, 3, 4, 5, 6, 7]

        try:
            userChoice = int(input())
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue
        if userChoice not in available_options:
            print("Sorry, I didn't understand that. Choose a number between 1 and 7")
            continue
        else:
            break

    if userChoice == 1:
        db = Database()
        selected_collection = db.select_collection()
        data = db.populate_collection(selected_collection)

    elif userChoice == 2:
        db = Database()
        selected_collection = db.select_collection()
        raw_data = db.collect_raw_data(selected_collection)
        raw_data[0].to_csv(f'data.nosync/RAW_{raw_data[1]}.gz', compression='gzip')
        print(f'Your csv file containing data for {raw_data[1]} has been created successfully. '
              f'Check your data.nosync folder!')

    elif userChoice == 3:
        db = Database()
        selected_collection = db.select_collection()
        raw_data = db.collect_raw_data(selected_collection)

        frequency = '1H'
        processed_data = ProcessData(raw_data[0], frequency).create_dataframe()
        # processed_data = processed_data.csv_processed_data()







        # db = Database()
        # rawData = db.populate_collection()
        # if rawData[1] == '':
        #     print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
        #     frequency = str(input()).replace(' ', '')
        # else:
        #     frequency = rawData[1]

        # df_raw = pd.DataFrame()
        # for num, raw in enumerate(tqdm(rawData[0])):
        #     df_raw = pd.concat([df_raw, pd.DataFrame(Database.COLLECTION[raw].find({}))])
        #     # Grouping collections and generating df by groups to improve performance.
        #     if (num % 100 == 0) & (num > 0):
        #         get_data(df_raw, frequency)
        #         df_raw = pd.DataFrame()
        #
        #     elif raw == rawData[0][-1]:
        #         print('collecting data...')
        #         get_data(df_raw, frequency)
        #
        #     else:
        #         pass

    elif userChoice == 4:
        db = Database()
        deletedColl = db.remove_collection()
        print(f'The collection has been removed successfully from the database.')

    elif userChoice == 5:
        db = Database()
        selected_collection = db.select_collection()
        current_data = db.show_stored_dates(selected_collection)
        print(f'You have data for {len(current_data)} days:', current_data)

    elif userChoice == 6:
        db = Database()
        selected_collection = db.select_collection()
        warnings = db.find_missing_data(selected_collection)
        print('You should double-check the data for these dates: ', warnings)

    elif userChoice == 7:
        print('Goodbye!')
        quit()

    else:
        print("\n\nOops! That is not a valid number. Please try a number between 1-7 to proceed:\n")
