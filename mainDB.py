import pandas as pd

from source.database import Database
from source.dataframe_creator import ProcessData
from source.utils import csv_converter

# 20190303

if __name__ == "__main__":
    """
    
    PIPELINE TO EXECUTE THE CRYPTO-SCRAPER    
    
    """

    while True:
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
            initial_data = ProcessData(processed=True)
            # processed_totals = initial_data.sum_grouper(cols=['size', 'grossValue', 'btcTotal',
            #                                                   'usdTotal', 'ContractsTraded_size',
            #                                                   'ContractsTraded_grossValue']).fillna(0)
            # processed_transactions = initial_data.counter_grouper(cols=['side']).fillna(0)
            # processed_ohcl, collection_name, frequency = initial_data.ohcl()
            # processed_data = pd.concat([processed_totals, processed_ohcl], axis=1).reset_index()



        #     TODO Hacer push de processed data??

        else:
            db = Database(processed=False)
            selected_collection, crypto, new_raw = db.select_collection(processed=False)
            dates_interval, crypto = db.dates_to_collect(selected_collection)

            print('Do you want to store your RAW data as a CSV file (YES/NO)?')
            while True:
                try:
                    csv_file = str(input()).lower()
                except ValueError:
                    print("Sorry, I didn't understand that. Please, try again")

                if csv_file == 'yes':
                    # csv_file = 'yes'
                    break
                elif csv_file == 'no':
                    break
                else:
                    print("Sorry, I didn't understand that. Please, write 'YES' or 'NO'.")

            dates_interval = [dates_interval[i:i + 3] for i in range(0, len(dates_interval), 3)]
            raw_data = pd.DataFrame()
            for intervals in dates_interval:
                raw_data, collection = db.collect_raw_data(selected_collection, crypto, intervals)
                last_date = intervals[-1]
                csv_converter(raw_data, collection, last_date, frequency='', csv_file=csv_file,
                              processed=False)
                raw_data = pd.DataFrame()



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
        print('What do you want to DELETE? PROCESSED or RAW data?')
        while True:
            try:
                processed = str(input()).upper()
            except ValueError:
                print("Sorry, I didn't understand that. Please, try again")

            if processed == 'PROCESSED':
                break
            elif processed == 'RAW':
                break
            else:
                print("Sorry, I didn't understand that. Please, write PROCESSED or RAW")

        db = Database()
        deletedColl = db.remove_collection(processed)
        print(f'The collection has been removed successfully from the database.')

    elif userChoice == 3:
        print('What do you want to check? PROCESSED or RAW data?')
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
                print("Sorry, I didn't understand that. Please, write PROCESSED or RAW")

        processed = processed
        db = Database()
        selected_collection= db.select_collection(processed='')
        current_data = db.show_stored_dates(processed)

    elif userChoice == 4:
        print('What do you want to check? PROCESSED or RAW data?')
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
                print("Sorry, I didn't understand that. Please, write PROCESSED or RAW")

        processed = processed
        db = Database()
        if processed == 'raw':
            selected_collection = db.select_collection(processed='')
        else:
            selected_collection, crypto, new_raw = db.select_collection(processed=processed)

        warnings = db.find_missing_data(selected_collection, crypto='')
        print('You should double-check the data for these dates: ', warnings)

    elif userChoice == 5:
        print('Goodbye!')
        quit()

    else:
        print("\n\nOops! That is not a valid number. Please try a number between 1-5 to proceed:\n")
