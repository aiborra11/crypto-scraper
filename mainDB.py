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


if __name__ == "__main__":
    """
    
    PIPELINE TO EXECUTE THE CRYPTO-SCRAPER    
    
    """

    while True:
        # print('\nEnter 1 if you want to CREATE/UPDATE your database:')
        # print('Enter 2 if you want to collect RAW data as a csv file:')
        print('Enter 1 if you want to COLLECT data:')
        print('Enter 2 if you want to DELETE a database/collection:')
        print('Enter 3 if you want to check your AVAILABLE STORED DATES for a specific crypto:')
        print('Enter 4 to check date WARNINGS:')
        print('Enter 5 to exit:')
        available_options = [1, 2, 3, 4, 5]

        try:
            userChoice = int(input())
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue
        if userChoice not in available_options:
            print("Sorry, I didn't understand that. Choose a number between 1 and 7")
            continue
        elif userChoice in available_options:
            break
        else:
            continue

    # if userChoice == 1:
    #     db = Database()
    #     selected_collection = db.select_collection()
    #     data = db.populate_collection(selected_collection)

    # elif userChoice == 2:
    #     db = Database()
    #     selected_collection = db.select_collection()
    #     raw_data = db.collect_raw_data(selected_collection)
    #     raw_data[0].to_csv(f'data.nosync/RAW_{raw_data[1]}.gz', compression='gzip')
    #     print(f'Your csv file containing data for {raw_data[1]} has been created successfully. '
    #           f'Check your data folder!')

    if userChoice == 1:
        print('What do you need? PROCESSED or RAW data?')
        while True:
            try:
                processed = str(input()).lower()
            except ValueError:
                print("Sorry, I didn't understand that. Please, try again")

            if processed == 'processed':
                break
            elif processed == 'raw':
                break
            else:
                print("Sorry, I didn't understand that. Please, write PROCESSED OR RAW")

        if processed == 'processed':
            initial_data = ProcessData(frequency='', processed=True)
            processed_totals = initial_data.sum_grouper(cols=['size', 'grossValue', 'btcTotal',
                                                              'usdTotal', 'ContractsTraded_size',
                                                              'ContractsTraded_grossValue']).fillna(0)
            processed_transactions = initial_data.counter_grouper(cols=['side']).fillna(0)
            processed_ohcl = initial_data.ohcl()
            processed_data = pd.concat([processed_totals, processed_ohcl], axis=1).reset_index()
            print(processed_data)

        else:
            db = Database(processed=False)
            selected_collection = db.select_collection()
            raw_data = db.collect_raw_data(selected_collection)
            print('--->raw_data', raw_data)
            # raw_data[0].to_csv(f'data.nosync/RAW_{raw_data[1]}.gz', compression='gzip')
            # print(f'Your csv file containing data for {raw_data[1]} has been created successfully. '
            #       f'Check your data folder!')



        # db = Database()
        # selected_collection = db.select_collection()

        # processed = db.select_database()[1]
        # print(processed)





        # Pasar al final del todo
        # print('Do you want to store this dataframe into a DATABASE, into a CSV file or BOTH?')
        # while True:
        #     try:
        #         store_place = str(input()).lower()
        #     except ValueError:
        #         print("Sorry, that is not a valid input. Write CSV, DATABASE or BOTH.")
        #     if store_place == 'csv':
        #         break
        #     elif store_place == 'database':
        #         break
        #     elif store_place == 'both':
        #         break
        #     else:
        #         print("Sorry, I didn't understand that. Please, choose between CSV, DATABASE or BOTH.")
        #         continue
        #
        # if store_place == 'csv':
        #     pass
        # elif store_place == 'database':
        #     pass
        # elif store_place == 'both':
        #     pass
        #




        # if store_place == 'database':
        #     available_data = initial_data.store_processed_data(frequency, processed_data, processed=True)
        #     pass
        # elif store_place == 'csv':
        #     pass
        #
        # elif store_place == 'both':
        #     available_data = initial_data.store_processed_data(frequency, processed_data)
        #     pass
        # else:
        #     print('Something went wrong, please try again!')











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

    elif userChoice == 2:
        db = Database()
        deletedColl = db.remove_collection()
        print(f'The collection has been removed successfully from the database.')


    elif userChoice == 3:
        db = Database()
        selected_collection = db.select_collection()
        current_data = db.show_stored_dates(selected_collection)
        print(f'You have data for {len(current_data)} days:', current_data)

    elif userChoice == 4:
        db = Database()
        selected_collection = db.select_collection()
        warnings = db.find_missing_data(selected_collection)
        print('You should double-check the data for these dates: ', warnings)

    elif userChoice == 5:
        print('Goodbye!')
        quit()

    else:
        print("\n\nOops! That is not a valid number. Please try a number between 1-5 to proceed:\n")
