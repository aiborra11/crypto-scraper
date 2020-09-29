import pandas as pd
from tqdm import tqdm
from os import listdir
from pymongo import MongoClient


class Database(object):
    """
    Access to different databases where daily collections containing raw data from the orderbook for a specified
    cryptocurrency (tick level) are stored.

    # Execute in your terminal: brew services restart mongodb-community

    """

    DATABASE = None

    def __init__(self):
        """
        Connection to your localhost database and first function execution.

        """

        self.client = MongoClient('localhost', 27017)
        self.databaseName = str(self.selectDatabase())
        self.availability = self.showAvailableData()


    def selectDatabase(self):
        """
        Shows a list of databases you can try to connect and asks to write the one you are interested.

        Arguments:
        ----------
        ()

        Returns:
        --------
            Access to the written database.

        """

        print(f'Available databases: {sorted(self.client.list_database_names())}.')
        print('Please, write the one you are interested in:')
        self.databaseName = str(input())
        if self.databaseName in self.client.list_database_names():
            return self.databaseName
        else:
            print(f'Sorry, we do not have a database named: {self.databaseName}. Restart the DB and try again.', )
            quit()


    def getRawData(self):
        """
        Shows a list of available collections we can find inside the selected database and asks to select the ones
        you are willing to collect.

        Arguments:
        ----------
        query {[str]} -- Any filter you are willing to apply. By default it brings all the data inside the collection.

        Returns:
        --------
            {[DataFrame]}
                Containing all the scraped and stored data for the selected dates and cryptocurrency.

        """

        Database.DATABASE = self.client[self.databaseName]
        return self.availability


    def removeCollection(self, collection=''):
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
            Database.DATABASE[collection].drop()
            print(f'The database {collection} has been deleted successfully!')

        else:
            Database.DATABASE = self.client[self.databaseName]
            print(f'Available collections for this database: {sorted(Database.DATABASE.list_collection_names())}')
            print('Please, select the one you are willing to drop: or write "all" if you want to drop them all')
            collections = str(input())

            if collections == 'all':
                print('We are preparing all available collections: ', collections)
                for collection in tqdm(sorted(Database.DATABASE.list_collection_names())):
                    Database.DATABASE[collection].drop()

            elif len(collections) > 1:
                collections_date = collections.replace(',', '').replace("'", '').split(' ')
                print("We are deleting the collections you've selected: ", collections_date)
                for collection in tqdm(sorted(collections_date)):
                    Database.DATABASE[collection].drop()

            else:
                print("We are deleting the collection you've selected: ", collections)
                Database.DATABASE[collections].drop()



    def showAvailableData(self):
        """
        Shows a list of available collections we can find inside the selected database and asks if we are interested
        in updating our database with new data.

        Arguments:
        ----------
        ()
        Returns:
        --------
            Collections we are willing to include into our database.

        """

        Database.DATABASE = self.client[self.databaseName]
        available_data = sorted(Database.DATABASE.list_collection_names())
        print('Available data: ', available_data)

        print(f'\nThere are {len(available_data)} available collections in your database.')
        print("\nIf you'd like to collect all the available data, write: 'ORIGIN'.")
        print("If you'd like to update since the last recorded datapoint in your general csv file, write: 'UPDATE'.")
        print("If you'd like to update since the last available record, write 'LAST'.")
        print("To update from a specific period, write the date in this format: 'YYYYMMDD'.")

        interval = str(input()).lower()
        if interval == 'last':
            last_val = available_data[-1]
            print('You have chosen LAST, so we will update since:', last_val)
            to_update = [x for x in available_data if x >= last_val]
            return sorted(to_update), ''

        elif interval == 'origin':
            print(f'You have chosen ORIGIN, so we are going to collect data since the very beginning.')
            to_update = [x for x in available_data if x >= '20141121']
            return sorted(to_update), ''

        elif interval in available_data:
            print(f'The interval: {interval} is available')
            return [interval], ''

        elif interval == 'update':
            file = listdir('data/')
            print('These are your existing csv files: ', file)
            print("Which is the timeframe you'd like to update [XMin, XH, D, W, M...]")
            frequency = str(input()).replace(' ', '')
            all_data = pd.read_csv(f'data/{frequency}_general.csv').Timestamp.iloc[-1].split(' ')[0].replace('-', '')
            print('You have chosen UPDATE, so we will update since:', all_data)
            to_update = [x for x in available_data if x >= all_data]
            return sorted(to_update), frequency

        else:
            print('There is no data for this date')


    @staticmethod
    def updateDatabase(date, available_data):
        """
        Updates our database with new scraped data.

        Arguments:
        ----------
        date {[str]} -- Date to include in our database.
        available_data -- Scraped data we will convert into a dict to store into our mongo database.

        Returns:
        --------
            {[Collection]}
                A new collection into our database for the specified date and currency.

        """

        available_data = available_data.to_dict(orient='records')
        try:
            Database.DATABASE[date].insert_many(available_data)
            print(f'Your new collection {date}, has been created successfully')
        except:
            print(f'There is no available data for the date: ', date)



    def datesDoubleCheck (self, scraped_interval):
        """
        Checks for empty collections.

        Arguments:
        ----------
        scraped_interval {[list]} -- Dates we are going to check.

        Returns:
        --------
            {[list]}
                With collection names we should double check.

        """

        print('We are collecting empty datasets, wait a moment....')
        Database.DATABASE = self.client[self.databaseName]
        double_check = [date for date in scraped_interval if date not in Database.DATABASE.list_collection_names()]
        print('\nSince there is no stored data, you should double check these collections: \n\n', double_check)
        print(f'\nThere are {len(double_check)} collections you should check.')
        return double_check



class DatabaseUpdator(Database):
    """
    Class inherits Database class to properly work when updating a new collection of data.

    """

    def __init__(self):
        pass


