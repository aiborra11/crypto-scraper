import pandas as pd
from pymongo import MongoClient


class Database(object):
    # brew services start mongodb-community
    DATABASE = None

    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.databaseName = str(self.selectDatabase())

    def selectDatabase(self):
        print(f'Available databases: {sorted(self.client.list_database_names())}.')
        print('Please, write the one you are interested in:')
        self.databaseName = input()
        return self.databaseName

    def getRawData(self, query={}):
        Database.DATABASE = self.client[self.databaseName]
        print(f'Available collections for this database: {sorted(Database.DATABASE.list_collection_names())}')
        print('Select the ones you are interested in, or write "all" if you want them all')
        collections = str(input())

        if collections == 'all':
            print('We are preparing all available collections: ', collections)
            crypto_data = pd.DataFrame()
            for collection in sorted(Database.DATABASE.list_collection_names()):
                data = pd.DataFrame(Database.DATABASE[collection].find(query))
                crypto_data = pd.concat([crypto_data, data])
            return crypto_data.set_index('timestamp')

        elif len(collections) > 1:
            collections_date = collections.replace(',', '').replace("'", '').split(' ')
            print('We are preparing collections for: ', collections_date)
            crypto_data = pd.DataFrame()
            for collection in sorted(collections_date):
                data = pd.DataFrame(Database.DATABASE[collection].find(query))
                crypto_data = pd.concat([crypto_data, data])
            return crypto_data.set_index('timestamp')

        else:
            print('We are preparing your collection for: ', collections)
            return pd.DataFrame(Database.DATABASE[collections].find(query))

    def removeCollection(self):
        Database.DATABASE = self.client[self.databaseName]
        print(f'Available collections for this database: {sorted(Database.DATABASE.list_collection_names())}')
        print('Please, select the one you are willing to drop:')
        collections = input()
        return Database.DATABASE[collections].drop()


    def showAvailableData(self):
        Database.DATABASE = self.client[self.databaseName]
        available_data = sorted(Database.DATABASE.list_collection_names())
        print('Available data: ', available_data)
        print(f"If you'd like to update since last record, write 'last'. Else, write the date in this format: YYYYMMDD")
        interval = str(input())
        if interval == 'last':
            print('You have chosen last, so we will update since:', available_data[-1])
            return [available_data][-1]

        elif interval in available_data:
            print(f'The interval: {interval} is available')
            return [interval]

        else:
            print('There is no data for this date')


    @staticmethod
    def updateDatabase(date, available_data):
        available_data = available_data.to_dict(orient='records')
        Database.DATABASE[date].insert_many(available_data)
        print(f'Your new collection {date}, has been created successfully')



class DatabaseUpdator(Database):
    def __init__(self):
        pass


