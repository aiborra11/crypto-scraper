import pandas as pd
from pymongo import MongoClient


class Database(object):
    # brew services start mongodb-community
    DATABASE = None

    def __init__(self):
        self.__client = MongoClient('localhost', 27017)
        self.databaseName = str(self.selectDatabase())

    def selectDatabase(self):
        print(f'Available databases: {sorted(self.__client.list_database_names())}. Choose the one you are interested in:')
        self.databaseName = input()
        return self.databaseName

    def getRawData(self, query={}):
        Database.DATABASE = self.__client[self.databaseName]
        print(f'Available collections for this database: {sorted(Database.DATABASE.list_collection_names())}. Select the ones you are interested in, or write "all" if you want them all')
        collections = input()
        return pd.DataFrame(Database.DATABASE[collections].find(query))

    def removeCollection(self):
        Database.DATABASE = self.__client[self.databaseName]
        print(f'Available collections for this database: {sorted(Database.DATABASE.list_collection_names())}. Select the ones you are willing to drop:')
        collections = input()
        return Database.DATABASE[collections].drop()


    def showAvailableData(self):
        Database.DATABASE = self.__client[self.databaseName]
        available_data = sorted(Database.DATABASE.list_collection_names())
        print('available data', available_data)
        print(f'If you would like to update since last record, write "last". Else, write the date in this format: YYYYMMDD')
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


