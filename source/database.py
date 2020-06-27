import pandas as pd
from pymongo import MongoClient


class Database(object):
    # brew services start mongodb-community
    DATABASE = None

    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        # print(f'These are your available databases: {self.client.list_database_names()}')

    def initialize(self, databaseName):
        Database.DATABASE = self.client[databaseName]
        print(f'These are your available collections inside {databaseName}: {Database.DATABASE.list_collection_names()}')
        return sorted(Database.DATABASE.list_collection_names())

    @staticmethod
    def insert(collection, data):
        data = data.to_dict(orient='records')
        Database.DATABASE[collection].insert_many(data)
        print(f'Your new collection {collection} has been created successfully')

    @staticmethod
    def removeCollection(collection):
        Database.DATABASE[collection].drop()
        print(f'Your collection {collection} has been removed')

    @staticmethod
    def find(collection, query={}):
        return pd.DataFrame(Database.DATABASE[collection].find(query))