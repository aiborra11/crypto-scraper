import pandas as pd

from datetime import datetime, timedelta

from source.database import Database


class CsvGenerator(Database):
    """

    """
    def __init__(self):
        """
        Connection to your localhost database and first function execution.
        """
        super().__init__()

    def retrieve_raw_data(self, selected_collection):
        raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))

        if raw_df.empty:
            interval = (str(datetime.today() - timedelta(days=1))).split(' ')[0].replace('-', '')
            cryptos = pd.read_csv(f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/'
                                  f'{interval}.csv.gz')

            print('We could not retrieve any data since you do not have any for this cryptocurrency. '
                  f'\nWrite "yes" if you want to store data for {self.collection_name} in your db?'
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

            # In case we already have data stored into our db
            if self.collection_name in self.show_available_collections():
                # LO MISMO QUE METEMOS AQUI, excepto el raw_df, LO METEMOS EN EL ELSE DE BAJO    TODO
                print(f'Charging your raw dataset for {self.collection_name}...')
                selected_collection = self.databaseName[str(self.collection_name)]
                raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
                print(raw_df)
                return raw_df

            # Populating db before retrieving raw data
            elif self.collection_name.lower() == 'yes':
                selected_collection = self.databaseName[str(coll_prov[-2])]
                self.collection_name = str(coll_prov[-2])
                self.populate_collection(selected_collection)
                raw_df = pd.DataFrame(list(selected_collection.find({}, {'_id': 0})))
                return raw_df

            else:
                print('something went wrong!')
                quit()

        else:
            print(f'Charging your raw dataset for {self.collection_name}...')
            print(raw_df)
            return raw_df






