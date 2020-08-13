import pandas as pd
import numpy as np


class processData():
    def __init__(self, df):
        self.df = df
        self.noDuplicates = self.duplicatesRemover()
        self.dataClean = self.dataCleaner(['', 'symbol', 'trdMatchID'])
        self.battle = self.bullsVsBears(['size', 'grossValue'])
        self.frequency = self.askFrequency()

        self.dataTotals = self.sumGrouper(cols=['size', 'grossValue', 'btcTotal', 'usdTotal', 'ContractsTraded_size', 'ContractsTraded_grossValue'])
        self.dataTransact = self.counterGrouper(cols=['side'])
        self.smoothedPx = self.emaSmoother(cols=['price'])
        self.highLow = self.wicksFinder(col='price')
        self.logReturns = self.percentageChange(col='price')


    def duplicatesRemover(self):
        self.noDuplicates = self.df.drop_duplicates(keep='first')
        print(f'{len(self.df) - len(self.noDuplicates)} rows have been deleted since they were duplicated')
        return self.noDuplicates


    def dataCleaner(self, columnsList):
        columns_delete = columnsList
        self.dataClean = self.df[[col for col in self.df.columns if col not in columns_delete]] \
            .rename(columns={'foreignNotional': 'usdTotal',
                                'homeNotional': 'btcTotal'})
        return self.dataClean


    def bullsVsBears(self, cols):
        filter_sell = self.dataClean['side'] == 'Sell'
        for col in cols:
            self.dataClean.loc[filter_sell, f'ContractsTraded_{col}'] = - self.dataClean.loc[filter_sell, col]
            self.dataClean.loc[~filter_sell, f'ContractsTraded_{col}'] = self.dataClean.loc[~filter_sell, col]
        return self.dataClean


    def askFrequency(self):
        print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
        self.frequency = str(input())
        return self.frequency


    def sumGrouper(self, cols):
        self.dataSum = pd.DataFrame(self.battle.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                    .sum()).shift(1, freq=self.frequency)
        return self.dataSum


    def counterGrouper(self, cols):
        transactions = pd.DataFrame(self.battle.groupby([pd.Grouper(freq=self.frequency),
                                                         self.battle['side'] == 'Buy'])[cols]
                                                                        .count()).unstack('side')
        transactions.columns = transactions.columns.droplevel()
        self.dataTransact = transactions.rename(columns={0: 'bearTransact', 1: 'bullTransact'}) \
                                                                            .shift(1, freq=self.frequency)
        self.dataTransact['warTransact'] = self.dataTransact.bullTransact - self.dataTransact.bearTransact
        self.dataTransact['totalTransact'] = self.dataTransact.bullTransact + self.dataTransact.bearTransact
        return self.dataTransact


    def emaSmoother(self, cols):
        for i in self.dataTransact['totalTransact'].values:
            exp_mov_avg = self.battle.ewm(span=i, adjust=False).mean()

        self.smoothedPx = pd.DataFrame(exp_mov_avg.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                .mean()).shift(1, freq=self.frequency)
        return self.smoothedPx


    def percentageChange(self, col):
        self.smoothedPx['pctChg'] = pd.DataFrame((np.log(1 + self.smoothedPx[col].pct_change()))).fillna(0)
        return self.smoothedPx



    def wicksFinder(self, col):
        maxList = []
        minList = []
        for val in self.df[col].groupby(pd.Grouper(freq=self.frequency)):
            maxList.append(max(val[1]))
            minList.append(min(val[1]))

        self.highLow = pd.concat([pd.DataFrame(maxList).rename(columns={0: 'High'}),
                                  pd.DataFrame(minList).rename(columns={0: 'Low'})], axis=1)

        return self.highLow.reset_index()



    def createDataFrame(self):
        dataset = pd.concat([self.dataTotals, self.dataTransact, self.logReturns], axis=1)
        wicks = self.highLow
        dataset = pd.concat([dataset.reset_index(),
                                wicks.reset_index(drop=True)], axis=1).set_index('timestamp').drop(columns='index')

        print(dataset)
        return dataset.to_csv(f'data/{self.frequency}_{str(dataset.index[0]).split(" ")[0]}to{str(dataset.index[-1]).split(" ")[0]}.csv')



