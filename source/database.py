import pandas as pd
import pymongo

from collections import defaultdict
from tqdm import tqdm
from os import listdir
from pymongo import MongoClient
from datetime import datetime, timedelta

from source.utils import data_scraper, interval_to_scrape


class Database(object):
    """
    Access to different databases where collections are stored containing tick data from the orderbook
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
        # Filtering dbs we do not need
        available_dbs = [x for x in self._client.list_database_names() if x not in ['admin', 'config', 'local']]
        print('Write the db you are interested in: ', available_dbs)

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

    def select_collection(self, processed):
        """
        Shows a list of your existing collections and connects us to the one we are interested in.
        It also allows us to create a new one in case we need it.

        Arguments:
        ----------
        processed {[bool]} -- Flag indicating if the data is processed or raw.

        Returns:
        --------
            Access to the written collection and to its data.

        """
        # Updating flag for processed data
        self.processed = processed
        available_data = self.show_available_collections()
        interval = (str(datetime.today() - timedelta(days=1))).split(' ')[0].replace('-', '')
        cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/'
                              f'{interval}.csv.gz')

        # print("Select the one you are willing to connect or write a new one if you want to create it:\n",
        #       sorted(cryptos['symbol'].unique()))        # Filtering available PROCESSED/RAW collections
        if self.processed == '':
            return print(f'These are your current stored collections inside your "{self.db_name}" database:\n', available_data)

        # PROCESSED collections
        elif self.processed:
            # available_data = [x for x in available_data if x.split('_')[-1].lower() == 'processed']
            available_data = dict(self.show_stored_dates(processed='processed').items())

            if available_data:
                if list(available_data.values())[0]==None:
                    self.collection_name = list(available_data.keys())[0].split('_')[1]
                    self.frequency = list(available_data.keys())[0].split('_')[0]

                else:
                    print(sorted(available_data))
                    # print(f'These are your current stored PROCESSED collections inside your "{self.db_name}" database:',
                    #       sorted(available_data))
                    print("Select the one you are willing to connect or write a new one if you want to create it:\n",
                          sorted(cryptos['symbol'].unique()))
            else:
                print('Choose from the list below and create a new one:\n', sorted(cryptos['symbol'].unique()))

        # RAW collections
        else:
            # available_data = [x for x in available_data if x.split('_')[-1].lower() == 'raw']
            available_data = dict(self.show_stored_dates(processed='raw').items())
            if available_data:
                # print(f'These are your current stored RAW collections inside your "{self.db_name}" database:\n',
                #       sorted(available_data))
                # sorted(available_data)
                print("\nSelect the one you are willing to connect or write a new one if you want to create it",
                      sorted(cryptos['symbol'].unique()))

            else:
                print('You do not have any RAW data collection yet. \n\nChoose from the list below and create'
                      'a new one:\n', sorted(cryptos['symbol'].unique()))

        while True:
            try:
                if self.collection_name in available_data:
                    break
                elif self.collection_name in cryptos['symbol'].unique():
                    break
            except:
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

        self.new_raw = False
        # If we had the collection already (either processed or raw) in our db
        if self.processed:
            self.collection_name = f'{self.collection_name}_PROCESSED' if len(self.collection_name.split('_')) == 1 \
                else self.collection_name
        else:
            self.collection_name = f'{self.collection_name}_RAW' if len(self.collection_name.split('_')) == 1 else self.collection_name
        if self.collection_name in available_data:
            if self.processed:
                self.frequency = self.collection_name.split('_')[0]
            else:
                crypto_dict = available_data[f'{self.collection_name}']
                print(f'Available dates for {self.collection_name}:', crypto_dict)
                self.new_raw = True

            return self.database_name[self.collection_name], self.collection_name, self.new_raw

        # To generate a new collection storing PROCESSED data for a new crypto
        elif self.collection_name not in available_data and self.processed:
            # We need a timeframe to get processed data
            if self.frequency:
                pass
            else:
                print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
                while True:
                    try:
                        self.frequency = str(input()).replace(' ', '').upper()
                    except ValueError:
                        print("Sorry, I didn't understand that.")
                        continue
                    else:
                        break
            try:
                self.database_name.create_collection(f'{self.frequency}_{self.collection_name}')
            except:
                pass
            print(f"You've been connected into your {self.db_name} database and logged into {self.collection_name} "
                  f"data")
            self.collection_name = f'{self.frequency}_{self.collection_name}'
            return self.database_name[self.collection_name], self.collection_name, self.new_raw

        # To generate a new collection storing RAW data for a new crypto
        elif self.collection_name not in available_data:
            selected_collection = self.collection_name.replace('_RAW', '').replace('_PROCESSED', '')
            if len(selected_collection.split('_')) > 1:
                print(f"You do not have previous data for {self.collection_name}, so we are going to generate a new "
                      f"collection per day storing raw data in the following format:\n"
                      f"'YYYYMMDD_{self.collection_name}_RAW'")
                self.new_raw = True
            else:
                # crypto_dict = available_data[f'{self.collection_name}_RAW']
                # print(f'Available dates for {self.collection_name}_RAW:',  crypto_dict)
                self.new_raw = True

            return self.database_name, self.collection_name, self.new_raw

    def dates_to_collect(self, selected_collection):
        """
        Generates a interval of dates we are willing to collect. We can choose between:
            a) Origin: Collect all data since the very beginning
            b) Update: Checks our last available record and updates from there till yesterday.
            c) Interval: Collects data for a specific interval of time.
            d) Concrete: Collects data for a specific day.

        Arguments:
        ----------
        selected_collection {[str]} --  client connected to our db and collection

        Returns:
        --------
            List containing the interval of dates and name of the crypto we are willing to collect
        """

        print("\nIf you'd like to collect all the available data in the AWS server, write: 'ORIGIN'.")
        print("If you'd like to update your DB since your last recorded date, write: 'UPDATE'.")
        print("To get data for a specific interval, write: 'INTERVAL'.")
        print("To get a concrete date, write: CONCRETE.")

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

        # Generating the interval of dates we are going to scrape
        if interval == 'origin':
            print(f'You have chosen ORIGIN, so we are going to collect data since the very beginning: 20141122.')

            # Generating a list containing the dates we are going to scrape
            interval_to_update = sorted(interval_to_scrape(day1='20141121', max_date=''))
            print('Updating interval: ', interval_to_update)

        elif interval == 'update':
            print(f'You have chosen UPDATE, so we are going to collect data since your last day recorded.')

            if self.processed:
                # Finding out last recorded value inside our PROCESSED data collection
                last_record = selected_collection.find().sort('timestamp', pymongo.DESCENDING).limit(1)
                if last_record:
                    last_date = [str(d['timestamp']).split('D')[0].replace('-', '') for d in last_record][0]
                    print(f'Your last recorded value was on: {last_date}.')
                else:
                    print(f'You do not have stored PROCESSED data for {self.collection_name}')

            else:
                # Finding out last recorded value inside our RAW data collection
                crypto_dict = dict(self.show_stored_dates(processed='raw').items())
                if crypto_dict[f'{self.collection_name}']:
                    last_date = crypto_dict[f'{self.collection_name}'][-1]
                    print(f'Your last recorded value was on: {last_date}.')
                else:
                    print(f'You do not have stored RAW data for {self.collection_name}')

            # TODO In case we deleted data from our db and we want to update from our csv files. Compare db vs csv and take the smallest date?

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
        cryptos = self.collection_name.split('_')
        dataset = pd.read_csv(
            f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{interval_to_update[-1]}.csv.gz')
        # Cleaning the name of the cryptocurrency to use it as a filter
        crypto = [crypt for crypt in cryptos if crypt in dataset['symbol'].unique()][0]
        return interval_to_update, crypto

    def collect_raw_data(self, selected_collection, crypto, dates_interval=''):
        """
        Checks if we have previous raw data for our desired crypto. In case we do, it retrieves the data,
        in case we don't it scrapes it and generates a new collections where pushing the new raw data.

        Arguments:
        ----------
        dates_interval {[list]} -- List of dates we are willing to collect.
        crypto {[str]} -- Cryptocurrency we are interested in.

        Returns:
        --------
            {[raw_df]}
                Dataframe containing raw data for the desired cryptocurrency.
            {[self.collection_name]}
                Name of the crypto
        """

        # To generate new collections storing RAW data
        if self.new_raw:
            print('Creating new collections to store the raw data...')
            db = self.database_name
            self.populate_collection(dates_interval, crypto, db)

            raw_df = pd.DataFrame()
            for date in dates_interval:
                raw_df = pd.concat([raw_df, pd.DataFrame(self.database_name[f'{date}_{crypto}_RAW'].find({}, {'_id': 0}))])

            return raw_df, self.collection_name

        else:
            if self.processed:
                crypto_dict = dict(self.show_stored_dates(processed='raw').items())
                db = self.database_name
                crypto = crypto.split('_')[1]
                if crypto_dict[f'{crypto}_RAW']:
                    available_dates = crypto_dict[f'{crypto}_RAW']

                    # Finding out dates we already have and just need to query vs the ones we need to scrape from AWS.
                    available_dates = [date for date in available_dates if date in dates_interval[0]]
                    print(f'\nWe found existing raw data for {crypto}: ', available_dates)
                    missing_dates = [date for date in dates_interval[0] if date not in available_dates]
                    print(f'We will need to collect from AWS the following dates: ', missing_dates)

                    # Pushing new scraped data into our RAW db
                    self.populate_collection(missing_dates, crypto, db)

                    # Collecting all data
                    all_dates = available_dates + missing_dates
                    raw_df = pd.DataFrame()
                    for date in all_dates:
                        raw_df = pd.concat(
                            [raw_df, pd.DataFrame(self.database_name[f'{date}_{crypto}_RAW'].find({}, {'_id': 0}))])
                    return raw_df, self.collection_name

                else:
                    print(f'You do not have stored RAW data for {self.collection_name}')

            else:
                pass
            print(selected_collection)
            raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
            # In case we do not have any previous raw data for this crypto
            print('Checking if we have stored RAW data for your selected dates...')

            if raw_df.empty:
                print('')
                interval = (str(datetime.today() - timedelta(days=1))).split(' ')[0].replace('-', '')
                cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/'
                                      f'{interval}.csv.gz')
                print('We could not find any RAW data for the specified dates...'
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

                # Populating db before retrieving raw data
                if self.collection_name.lower() == 'yes':
                    self.collection_name = str(coll_prov[-2])

                    interval_to_update, crypto = self.dates_to_collect(selected_collection)
                    self.populate_collection(interval_to_update, crypto)
                    # TODO BLOWS HERE BECAUSE .find() REQUIRES TOO MUCH RAM TO BRING THE WHOLE DATA

                    raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
                    return raw_df, self.collection_name

                else:
                    print('something went wrong!')
                    quit()
            # We have previous raw data for this crypto
            else:
                print(f'Collecting your raw dataset for {self.collection_name}...')
                return raw_df, self.collection_name

    def populate_collection(self, interval_to_update, crypto, db=''):
        """
        Push data into the database. In case we do not have any it will create the drypto_RAW collection
        and push data into it.

        Arguments:
        ----------
        interval_to_update {[list]} --  Interval of dates we are going to scrape.
        crypto {[str]} --  Name of the cryptocurrency we are willing to scrape.

        Returns:
        --------
        Data pushed into our db and missing dates we could not retrieve any data.

        """
        # Storing the already collected raw data into a new collection
        warnings = []
        db_collection = db
        for date in tqdm(interval_to_update):
            data, warning, crypto = data_scraper(date, crypto)
            if warning: warnings.append(warning), print(f'You have {len(warnings)} missing dates in {crypto}_RAW. '
                                                        f'You should double-check them!\n', warnings)
            self.push_data_into_db(data, db_collection, date, crypto, processed=False)

        return db_collection

    @staticmethod
    def push_data_into_db(available_data, db_collection, date, crypto, processed=False):
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
                # self.database_name[f'{frequency}_{crypto}_PROCESSED'].insert_many(available_data)
            except:
                pass
                # print(f'There is no available data for the date: ', date)

        else:

            # Converting scraped data into a format required for inserting data into mongodb
            available_data = available_data[[col for col in available_data.columns if col != 'symbol']]
            available_data = available_data.to_dict(orient='records')
            try:
                db_collection[f'{date}_{crypto}_RAW'].insert_many(available_data)
                # self.database_name[f'{date}_{crypto}_RAW'].insert_many(available_data)

            except:
                pass
                # print(f'There is no available data for the date: ', date)

    def remove_collection(self, processed, collection=''):
        """
        Shows the available collections and asks to delete any collections we are no longer interested in storing
        into our database.

        Arguments:
        ----------
        collection {[str]} -- In case we know the collection we want to delete

        Returns:
        --------
            Drops a stored collection

        """
        if collection:
            self.database_name[collection].drop()
            print(f'The database {collection} has been deleted successfully!')

        else:
            print(f'Available collections for this database: {self.show_available_collections()}')
            print('Please, select the one you are willing to drop: or write "all" if you want to drop them all')
            collections = str(input()).strip().split(',')

            if collections[0] == 'all':
                print('We are preparing all available collections: ', collections)
                available_collections = sorted(self.show_available_collections())
                cryptos = list(set([cryp.split('_')[1] for cryp in available_collections]))
                crypto_dict = dict(self.show_stored_dates(processed='raw').items())

                for crypt in cryptos:
                    available_dates = crypto_dict[f'{crypt}_{processed}']
                    print(f'Deleting data for {crypt}')
                    for date in tqdm(sorted(available_dates[:-1])):
                        # Avoid deleting the last collection so we can use 'UPDATE' functionality
                        self.database_name[f'{date}_{crypt}_{processed}'].drop()

            elif len(collections) > 1:
                print("We are deleting the collections you've selected: ", collections)
                for collection in tqdm(collections):
                    # TODO need to improve this by using nltk/spacy
                    collection = collection.replace(' ', '').replace("'", '').replace('[', '').replace(']', '')
                    self.database_name[collection].drop()
            else:
                print("We are deleting the collection you've selected: ", collections)
                collection = collections[0].replace(',', '').replace("'", '').replace('[', '').replace(']', '')
                print(collection)
                self.database_name[collection].drop()

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

    def find_missing_data(self, selected_collection, crypto=''):
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

        # If processed we will need to query the collection.
        if self.processed:
            # Finding out first available record in our collection...
            actual_dates = selected_collection.find().sort('timestamp', pymongo.ASCENDING)
            first_record = actual_dates[0]
            print(first_record)
            print(f'Your first record for {self.collection_name} is from:')
            first_date = [str(d['timestamp']).split('D')[0].replace('-', '') for d in first_record][0]
            print(f'Your first record for {self.collection_name} is from:', first_date)

        # If RAW data we only need to check the names of the collections.
        else:
            # Bringing crypto dictionary containing key=crypto, value=list of available dates
            crypto_dict = self.show_stored_dates(processed='raw')
            available_data = sorted(list(crypto_dict.keys()))
            print(f'\nChoose the crypto you want to check for warnings:')
            while True:
                try:
                    crypto = str(input()).upper()
                except ValueError:
                    print("Sorry, I didn't understand that. Please, try again")

                if crypto in available_data:
                    break
                else:
                    print("Sorry, I didn't understand that. Please, copy and paste from the above list the name of the "
                          "crypto data you want to check:")

            actual_dates = list(crypto_dict[crypto])
            first_date = sorted(list(crypto_dict[crypto]))[0]
            print('first_value = ', sorted(list(crypto_dict[crypto]))[0])

        # Generating dates we should have data according to our first record
        datapoints = sorted(set(interval_to_scrape(day1=first_date, max_date='')))
        # Comparing actual dates vs theoretical
        missing_dates = set(datapoints) - set(actual_dates)
        print(f'You have {len(missing_dates)} warnings.')
        return sorted(list(missing_dates))

    def show_stored_dates(self, processed):
        """
        Basing on the name of the collections we have stored in our DB, it
        shows our available data per currency.
        It also counts how many days of data we have stored in our db.

        Arguments:
        ----------
        processed {[str]} --  Flag to filter by processed, raw or all data

        Returns:
        --------
        Dictionary where key=crypto, value= list of dates.
        """

        crypto_dict = {}
        available_data = self.show_available_collections()

        if processed == 'raw':
            available_data = [x for x in available_data if x.split('_')[-1].lower() == processed]
            crypt_data = [(x.split('_')[1] + '_' + x.split('_')[2], x.split('_')[0]) for x in available_data]
            if available_data:
                crypto_dict = defaultdict(list)
                for crypt, date in crypt_data:
                    crypto_dict[crypt].append(date)

                print(f'\nYou have {processed.upper()} data for:', sorted(list(crypto_dict.keys())))
                for i in range(len(sorted(crypto_dict))):
                    print('\n', list(crypto_dict.items())[i])
                    print(f'You have data for {len(list(crypto_dict.items())[i][1])} days.')
            else:
                print(f'You do not have any {processed.upper()} data collection yet.')

        elif processed == 'processed':
            available_data = [x for x in available_data if x.split('_')[-1].lower() == processed]
            if available_data:
                print('\nSelect the PROCESSED collection you want to check from the following list:', available_data)

                while True:
                    try:
                        collection_name = str(input()).upper()
                    except ValueError:
                        print("Sorry, I didn't understand that.")
                        continue

                    if collection_name in available_data:
                        break
                    else:
                        print('Sorry, something went wrong! Please, try again.')

                selected_collection = self.database_name[collection_name]
                available_dates = list(selected_collection.find().sort('timestamp', pymongo.ASCENDING))
                if available_dates:
                    print(f'Your collection {collection_name} available dates are on:, ', available_dates)
                    crypto_dict[selected_collection]=available_dates
                else:
                    print(f'Sorry, your collection {collection_name} seems to be empty.')
                    return {f"{collection_name.split('_')[0]}_{collection_name.split('_')[1]}": None}
            else:
                print(f'You do not have any {processed.upper()} data collection yet.')
        else:
            print('Sorry, something went wrong!')

        return crypto_dict

