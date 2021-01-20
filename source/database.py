import pandas as pd
import pymongo

from tqdm import tqdm
from os import listdir
from pymongo import MongoClient
from datetime import datetime, timedelta

from source.utils import data_scraper, interval_to_scrape


class Database(object):
    """
    Access to different databases where daily collections are stored containing tick data from the orderbook
    and for a specified cryptocurrency.

    """

    def __init__(self, processed=''):
        """
        Connection to your localhost and database.
        """
        self._client = MongoClient('localhost', 27017)
        self.database_name, self.processed = self.select_database(processed)

    def select_database(self, processed=''):
        """
        Shows a list of your existing databases and connects us to the one we are interested in.
        It also allows us to create a new one in case we need it.

        Arguments:
        ----------
        processed {[bool]} -- Flag indicating if the data is processed or raw.

        Returns:
        --------
            Access to the written database.

        """
        available_dbs = [x for x in self._client.list_database_names() if x not in ['admin', 'config', 'local']]
        print('Write the db you are interested in: ', available_dbs)
        print('You can also write e new one in case you want to create it...')


        db_prov = []
        while True:
            try:
                self.db_name = str(input()).lower()
                db_prov.append(self.db_name)
            except ValueError:
                print("Sorry, I didn't understand that. Please, try again")
                continue
            if self.db_name.lower() == 'yes':
                break
            elif self.db_name in self._client.list_database_names():
                break
            else:
                print(f'Sorry, we do not have any database named: {self.db_name}. '
                      f'\nWrite "yes" if you would like to create a new one. '
                      f'Otherwise, re-write the name of the database you would like create/access.')
                # In case we write yes, we will need to get the previous value to name the db. Otherwise name = yes
                db_prov.append(self.db_name)
                continue

        # Connecting the DB we were looking for.
        if self.db_name in self._client.list_database_names():
            print(f'Connecting to {self.db_name}...')
            return self._client[self.db_name], processed

        # Creating the new DB we were looking for.
        elif self.db_name.lower() == 'yes':
            # Assigning the previous value to the db_name
            self.db_name = str(db_prov[-2])
            print(f'Creating and connecting to {self.db_name}...')
            return self._client[self.db_name], processed

        else:
            print('Something went wrong!')
            quit()

    def select_collection(self):
        """
        Shows a list of your existing collections and connects us to the one we are interested in.
        It also allows us to create a new one in case we need it.

        Arguments:
        ----------
        ()

        Returns:
        --------
            Access to the written collection and to its data.

        """

        available_data = self.show_available_collections()
        interval = (str(datetime.today() - timedelta(days=1))).split(' ')[0].replace('-', '')
        cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/'
                              f'{interval}.csv.gz')

        # Filtering available PROCESSED/RAW collections
        if self.processed:
            available_data = [x for x in available_data if x.split('_')[-1].lower() == 'processed']
            if available_data:
                print(f'These are your current stored PROCESSED collections inside your "{self.db_name}" database:',
                      sorted(available_data))
                print("\nSelect the one you are willing to connect or write a new one if you want to create it",
                      sorted(cryptos['symbol'].unique()))
            else:
                print('You do not have any PROCESSED data collection yet. \n\nChoose from the list below and create'
                      'a new one:\n', sorted(cryptos['symbol'].unique()))
        # RAW collections
        else:
            available_data = [x for x in available_data if x.split('_')[-1].lower() == 'raw']
            if available_data:
                print(f'These are your current stored RAW collections inside your "{self.db_name}" database:',
                      sorted(available_data))
                print("\nSelect the one you are willing to connect or write a new one if you want to create it",
                      sorted(cryptos['symbol'].unique()))
            else:
                print('You do not have any RAW data collection yet. \n\nChoose from the list below and create'
                      'a new one:\n', sorted(cryptos['symbol'].unique()))

        while True:
            try:
                self.collection_name = str(input()).upper()
            except ValueError:
                print("Sorry, I didn't understand that.")
                continue

            if self.collection_name in available_data:
                break
            elif self.collection_name in cryptos['symbol'].unique():
                break
            elif self.collection_name not in cryptos['symbol'].unique():
                print(f"Sorry, this crypto '{self.collection_name}' does not exist. Try again!")
                continue
            else:
                print('Something went wrong! Please, try again.')

        # If we had the collection already (either processed or not) in our db
        if self.collection_name in available_data:
            if self.processed:
                self.frequency = self.collection_name.split('_')[0]
            else:
                pass
            print(f"You've been connected into your {self.db_name} database and logged into {self.collection_name} "
                  f"data.")
            return self.database_name[self.collection_name]

        # To generate a new collection storing PROCESSED data for a new crypto
        elif self.collection_name not in available_data and self.processed:
            # We need a timeframe to get processed data
            print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
            while True:
                try:
                    self.frequency = str(input()).replace(' ', '').upper()
                except ValueError:
                    print("Sorry, I didn't understand that.")
                    continue
                else:
                    break

            self.database_name.create_collection(f'{self.frequency}_{self.collection_name}_PROCESSED')
            print(f"You've been connected into your {self.db_name} database and logged into {self.collection_name} "
                  f"data")
            self.collection_name = f'{self.frequency}_{self.collection_name}_PROCESSED'
            return self.database_name[self.collection_name]

        # To generate a new collection storing RAW data for a new crypto
        elif self.collection_name not in available_data:
            print(f"You've been connected into your {self.db_name} database and logged into {self.collection_name} "
                  f"data")
            return self.database_name[self.collection_name]

    def remove_collection(self, collection=''):
        """
        Shows the available collections and asks to delete any collections we are no longer interested in storing
        into our database.

        Arguments:
        ----------
        ()

        Returns:
        --------
        ()

        """
        if collection:
            self.database_name[collection].drop()
            print(f'The database {collection} has been deleted successfully!')

        else:
            print(f'Available collections for this database: {self.show_available_collections()}')
            print('Please, select the one you are willing to drop: or write "all" if you want to drop them all')
            collections = str(input())

            if collections == 'all':
                print('We are preparing all available collections: ', collections)
                for collection in tqdm(self.show_available_collections()):
                    self.database_name[collection].drop()

            elif len(collections) > 1:
                collections_date = collections.replace(',', '').replace("'", '').split(' ')
                print("We are deleting the collections you've selected: ", collections_date)
                for collection in tqdm(sorted(collections_date)):
                    self.database_name[collection].drop()

            else:
                print("We are deleting the collection you've selected: ", collections)
                self.database_name[collections].drop()

    def show_available_collections(self):
        """
         Shows a list of available collections we can find inside the selected database

         Arguments:
         ----------
         ()
         Returns:
         --------
        Our stored collections in the db we have connected
        """
        available_data = sorted(self.database_name.list_collection_names())
        return available_data

    def populate_collection(self, selected_collection):
        """
        Populates our collection according to our desired format. We can choose between:
            a) Origin: Collect all data since the very beginning
            b) Update: Checks our last available record and updates from there till yesterday.
            c) Interval: Collects data for a specific interval of time.
            d) Concrete: Collects data for a specific day.

        Arguments:
        ----------
        selected_collection {[str]} --  client connected to our db and collection

        Returns:
        --------
            Populated collection with the data we have specified.

        """

        print("\nIf you'd like to store all the available data into your DB, write: 'ORIGIN'.")
        print("If you'd like to update your DB, write: 'UPDATE'.")
        print("To store data for a specific interval write: 'INTERVAL'.")
        print("To store ONLY a CONCRETE period, write: CONCRETE.")

        available_options = ['origin', 'update', 'interval', 'concrete']

        while True:
            try:
                interval = str(input()).lower()
            except ValueError:
                print("Sorry, I didn't understand that. Please re-write it in the correct format 'YYYYMMDD'")
                continue
            if interval not in available_options:
                print("Sorry, I didn't understand that. Please, try again!")
                continue
            else:
                break

        if interval == 'origin':
            print(f'You have chosen ORIGIN, so we are going to collect data since the very beginning: 20141122.')

            # Generating a list containing the dates we are going to scrape
            interval_to_update = sorted(interval_to_scrape(day1='20141121', max_date=''))
            print('Updating interval: ', interval_to_update)

        elif interval == 'update':
            print(f'You have chosen UPDATE, so we are going to collect data since your last day recorded.')

            # Finding out last recorded value inside our collection
            last_record = selected_collection.find().sort('timestamp', pymongo.DESCENDING).limit(1)
            last_date = [str(d['timestamp']).split('D')[0].replace('-', '') for d in last_record][0]
            print(f'Your last recorded value was on: {last_date}.')

            # Generating a list containing the dates we are going to scrape
            interval_to_update = sorted(interval_to_scrape(day1=last_date, max_date=''))
            print('Updating interval: ', interval_to_update)

        elif interval == 'interval':
            print(f'You have chosen INTERVAL, so we are going to collect data between two dates.'
                  f'\nWrite your starting day in the following format: "YYYYMMDD"')

            while True:
                try:
                    day1 = int(input())
                    print('Now, write your last date following the same format: ')
                    max_date = int(input())
                except ValueError:
                    print('Sorry, something went wrong. Please, try again! Write your starting date:')
                    continue
                if len(str(day1)) != 8 or len(str(max_date)) != 8 or day1 >= max_date:
                    print('Please, write again your starting and last date.')
                    continue
                else:
                    break

            interval_to_update = sorted(interval_to_scrape(day1=day1, max_date=max_date))
            print('Updating interval: ', interval_to_update)

        elif interval == 'concrete':
            print(f'You have chosen CONCRETE, so we are going to collect data for that specific date'
                  f'\nWrite your date following format: "YYYYMMDD"')

            while True:
                try:
                    day1 = int(input())
                except ValueError:
                    print('Sorry, something went wrong. Please, try again! Write your starting date:')
                    continue
                if len(str(day1)) != 8:
                    print('Please, write it again. Remember to follow this format: "YYYYMMDD"')
                    continue
                else:
                    break

            interval_to_update = sorted(interval_to_scrape(day1=day1, max_date=day1))
            print('Updating interval: ', interval_to_update)

        # Scraping data from bitmex and inserting it into our collection
        data = data_scraper(interval_to_update, self.collection_name)[0]
        if data[1]:
            print(f'You should double-check {data[1]} since we could not find any data.')
        else:
            pass
        if self.processed:
            return self.push_data_into_db(data, selected_collection, processed=True)
        else:
            return self.push_data_into_db(data, selected_collection, processed=False)


    @staticmethod
    def push_data_into_db(available_data, db_collection, processed=False, date=''):
        """
        Inserts scraped data into our selected collection and database.

        Arguments:
        ----------
        date {[str]} -- Day the data belongs to.
        available_data {[Dataframe]} -- Scraped data we will convert into a dict to store into our mongo database.
        db_collection {[str]} -- Client connected to our db and collection
        processed {[bool]} -- Flag indicating if the data is processed or raw.

        Returns:
        --------
            {[Collection]}
                Including new data for the selected cryptocurrency
        """

        if processed:
            try:
                print(f'Pushing processed data into your collection')
                available_data = available_data.to_dict(orient='records')
                db_collection.insert_many(available_data)
            except:
                print(f'There is no available data for the date: ', date)

        else:
            # Converting scraped data into a format required for inserting data into mongodb
            available_data = available_data[[col for col in available_data.columns if col != 'symbol']]
            available_data = available_data.to_dict(orient='records')
            try:
                db_collection.insert_many(available_data)
            except:
                print(f'There is no available data for the date: ', date)

    def find_missing_data(self, selected_collection):
        """
        Checks missing dates into our collection

        Arguments:
        ----------
        selected_collection {[str]} --  client connected to our db and collection

        Returns:
        --------
            {[list]}
                With dates we should double check.

        """

        print('Checking if you have missing data...')

        # Finding out first available record in our collection...
        first_record = selected_collection.find().sort('timestamp', pymongo.ASCENDING).limit(1)
        first_date = [str(d['timestamp']).split('D')[0].replace('-', '') for d in first_record][0]
        print('Your first record is from:', first_date)

        # Filtering by timestamp is the only datapoint we actually need
        actual_dates = self.show_stored_dates(selected_collection)

        # Comparing real datapoints we should currently have from our first record until today's date
        datapoints = sorted(set(interval_to_scrape(day1=first_date, max_date='')))
        missing_dates = set(datapoints) - set(actual_dates)
        print(f'You have {len(missing_dates)} warnings.')
        return sorted(list(missing_dates))

    def show_stored_dates(self, selected_collection):
        """
        Brings all unique timestamp data for the specified cryptocurrencies.

        Arguments:
        ----------
        selected_collection {[str]} --  client connected to our db and collection

        Returns:
        --------
            Dates with available data

        """
        # Filtering by timestamp is the only datapoint we actually need
        print('Checking your current stored data...')
        dates = selected_collection.find({}, {'_id': 0, 'timestamp': 1})
        actual_dates = sorted(set([str(d['timestamp']).split('D')[0].replace('-', '') for d in dates]))
        return actual_dates

    def collect_raw_data(self, selected_collection):
        """
        Generates a csv file (gzip compressed) containing raw data for the required crypto. In case any data is found
        within the db we can also populate it and then pull the data.

        Arguments:
        ----------
        selected_collection {[str]} --  client connected to our db and collection

        Returns:
        --------
            csv file in our data.nosync folder

        """
        raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
        print(raw_df)
        if raw_df.empty:
            interval = (str(datetime.today() - timedelta(days=1))).split(' ')[0].replace('-', '')
            cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/'
                                  f'{interval}.csv.gz')

            print('We could not retrieve any data since you do not have any for this cryptocurrency. '
                  f'\nWrite "yes" to store data for {self.collection_name} in your db'
                  f'\nOtherwise, write the correct name')
            coll_prov = [self.collection_name]
            while True:
                try:
                    self.collection_name = str(input()).upper()
                    coll_prov.append(self.collection_name)
                except ValueError:
                    print("Sorry, I didn't understand that. Please, try again")
                    continue
                if self.collection_name.lower() == 'yes':
                    break
                elif self.collection_name not in cryptos['symbol'].unique():
                    print(f"Sorry, this crypto '{self.collection_name}' does not exist. Try again!")
                    continue
                elif self.collection_name in self.show_available_collections():
                    coll_prov.append(self.collection_name)
                    break
                else:
                    coll_prov.append(self.collection_name)
                    continue

            # # In case we already have data stored into our db
            # if self.collection_name in self.show_available_collections():
            #     print(f'Collecting your raw dataset for {self.collection_name}...')
            #     selected_collection = self.database_name[str(self.collection_name)]
            #     raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
            #     return raw_df, self.collection_name


            # Estamos almacenando RAW data en la processed DB
            # Populating db before retrieving raw data
            if self.collection_name.lower() == 'yes':
                # # Switching into raw_data db. We do not want raw data into the processed_cryptos db.
                # if self.processed:
                #     print(f'Populating your db with raw data for {str(coll_prov[-2])} into your "raw_cryptos" db...')
                #     selected_collection = self._client['raw_cryptos'][str(coll_prov[-2])]
                #
                # else:
                #     print(f'Populating your db with raw data for {str(coll_prov[-2])}...')
                #     selected_collection = self.database_name[str(coll_prov[-2])]

                self.collection_name = str(coll_prov[-2])
                self.populate_collection(selected_collection)
                raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
                return raw_df, self.collection_name

            else:
                print('something went wrong!')
                quit()

        else:
            print(f'Collecting your raw dataset for {self.collection_name}...')
            return raw_df, self.collection_name

    # def store_processed_data(self, processed_data):
    #     """
    #     Reconnects to the mongoDB to check if we have a DB to store processed data, in case we don't, it will be created,
    #     and push new processed data into our processed db.
    #
    #     Arguments:
    #     ----------
    #     frequency {[str]} -- Timeframe we are using to process the data.
    #     processed_data {[Dataframe]} -- Processed data.
    #
    #     Returns:
    #     --------
    #         ()
    #     """
    #
    #     # Reconnecting to the database to store processed data
    #     self._client = MongoClient('localhost', 27017)
    #     self.db_name = 'processed_cryptos'
    #     existing_processed_db = self._client.list_database_names()
    #
    #     # Checking if we already have a db to store processed data
    #     if self.db_name in existing_processed_db:
    #         self.database_name = self._client[self.db_name]
    #         existing_processed_data = self.show_available_collections()
    #
    #         if f'{self.frequency}_{self.collection_name.upper()}_PROCESSED' in existing_processed_data:
    #             # Update collection
    #             new_collection = self.database_name[f'{self.frequency}_{self.collection_name.upper()}_PROCESSED']
    #
    #             print(f'You have existing processed data for {self.collection_name.upper()} and {self.frequency} timeframe. '
    #                   f'We are going to update it.')
    #             self.push_data_into_db(processed_data, new_collection, processed=True)
    #             pass
    #         else:
    #             # Create a new collection
    #             self.database_name.create_collection(f'{self.frequency}_{self.collection_name.upper()}_PROCESSED')
    #             pass
    #
    #     # Creating a new db to store processed data.
    #     else:
    #         self.database_name = self._client[self.db_name]
    #         new_collection = self.database_name.create_collection(f'{self.frequency}_{self.collection_name.upper()}_PROCESSED')
    #         self.push_data_into_db(processed_data, new_collection, processed=True)





#
# class DatabaseUpdator(Database):
#     """
#     Class inherits Database class to properly work when updating a new collection of data.
#
#     """
#
#     # def __init__(self):
#     #     pass
#
#     def update_db(self):
#         """
#         Shows a list of available collections we can find inside the selected database and asks if we are interested
#         in updating our database with new data.
#
#         Arguments:
#         ----------
#         ()
#         Returns:
#         --------
#             Collections we are willing to include into our database.
#
#         """
#
#         Database.COLLECTION = self._client[self.databaseName]
#         available_data = sorted(Database.COLLECTION.list_collection_names())
#         print('Available data: ', available_data)
#
#         print(f'\nThere are {len(available_data)} available collections in your database.')
#
#         print("\nIf you'd like to collect all the available data, write: 'ORIGIN'.")
#         print("If you'd like to update your general csv file, write: 'UPDATE'.")
#         print("If you'd like to update since the last available record in the database, write 'LAST'.")
#         print("To update from a specific period, write the date in this format: 'YYYYMMDD'.")
#         print("To update ONLY a CONCRETE period, write: CONCRETE.")
#
#         interval = str(input()).lower()
#         if interval == 'last':
#             last_val = available_data[-1]
#             print('You have chosen LAST, so we will update your general csv file since:', last_val)
#             to_update = [x for x in available_data if x >= last_val]
#             return sorted(to_update), ''
#
#         elif interval == 'origin':
#             print(f'You have chosen ORIGIN, so we are going to collect data since the very beginning.')
#             to_update = [x for x in available_data if x >= '20141121']
#             return sorted(to_update), ''
#
#         elif interval == 'concrete':
#             print('Write the period you want to collect in the following format: "YYYYMMDD" ')
#             interval = str(input())
#             if interval in available_data:
#                 print(f'The interval: {interval} is available')
#                 return [interval], ''
#             else:
#                 print('Sorry but we do not have this collection in our database.')
#
#         elif interval in available_data:
#             print(f'The interval: {interval} is available')
#             to_update = [x for x in available_data if x >= interval]
#             return sorted(to_update), ''
#
#         elif interval == 'update':
#             return available_data[-1], ''
#
#         else:
#             print('There is no data for this database')
