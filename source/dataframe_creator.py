import pandas as pd
import numpy as np


class processData():
    """
    Converts raw data into a determined frequency and creates new features as columns.

    """
    def __init__(self, df):
        """
        Allowing different functions to inherit from each other.

        """
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
        """
        Checks for duplicates and removes them.

        Arguments:
        ----------
        ()

        Returns:
        --------
            Dataframe with no duplicated transactions.

        """

        self.noDuplicates = self.df.drop_duplicates(keep='first')
        print(f'{len(self.df) - len(self.noDuplicates)} rows have been deleted since they were duplicated')
        return self.noDuplicates


    def dataCleaner(self, columnsList):
        """
        Deletes columns we no longer need and renames foreigNotional : usdTotal and homenotional : btcTotal

        Arguments:
        ----------
        columnsList {[list]} -- Name of the columns we are willing to drop. By default: ['', 'symbol', 'trdMatchID']

        Returns:
        --------
            {[DataFrame]}
                Containing all the scraped and stored data for the selected dates and cryptocurrency.

        """

        columns_delete = columnsList
        self.dataClean = self.df[[col for col in self.df.columns if col not in columns_delete]] \
            .rename(columns={'foreignNotional': 'usdTotal',
                                'homeNotional': 'btcTotal'})
        return self.dataClean


    def bullsVsBears(self, cols):
        """
        Converts short transactions into negative and longs into positive to find out the type and amount of
        transactions.

        Arguments:
        ----------
        columnsList {[list]} -- Name of the columns we are willing evaluate. By default: ['size', 'grossValue']

        Returns:
        --------
            {[DataFrame]}
                Containing all the scraped and stored data for the selected dates and cryptocurrency + the sign
                modified to evaluate if the transaction was a short or a long.

        """

        filter_sell = self.dataClean['side'] == 'Sell'
        for col in cols:
            self.dataClean.loc[filter_sell, f'ContractsTraded_{col}'] = - self.dataClean.loc[filter_sell, col]
            self.dataClean.loc[~filter_sell, f'ContractsTraded_{col}'] = self.dataClean.loc[~filter_sell, col]
        return self.dataClean


    def askFrequency(self):
        """
        Frequency we would like to receive the data.

        Arguments:
        ----------
        ()

        Returns:
        --------
            Dataframe with the data grouped after the selected frequency.

        """
        print("Which is the timeframe you'd like to receive the data [XMin, XH, D, W, M...]")
        self.frequency = str(input())
        return self.frequency


    def sumGrouper(self, cols):
        """
        Sums the desired columns to group them after the desired frequency.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are willing sum.

                         By default = ['size', 'grossValue', 'btcTotal', 'usdTotal',
                                       'ContractsTraded_size', 'ContractsTraded_grossValue']

        Returns:
        --------
            Dataframe with the sum of the desired columns based on the selected frequency.

        """

        self.dataSum = pd.DataFrame(self.battle.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                    .sum()).shift(1, freq=self.frequency)
        return self.dataSum


    def counterGrouper(self, cols):
        """
        Counts the number of shorts and longs that have been performed during the selected frequency.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are willing count. By default = ['side']

        Returns:
        --------
            Dataframe with the number of transactions performed during the selected frequency.
                bearTransact: number of bearish transactions.
                bullTransact: number of bullish transactions.
                warTransact: bull transactions - bear transactions
                totalTransact: bull transactions + bear transactions

        """

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
        """
        Smooths the column weighting it after an exponential moving average of i periods.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are willing smooth after an EMA. By default = ['price']

        Returns:
        --------
        Dataframe with the cryptocurrency smoothed price.


        """

        for t in self.dataTransact['totalTransact'].values:
            exp_mov_avg = self.battle.ewm(span=t, adjust=False).mean()

        self.smoothedPx = pd.DataFrame(exp_mov_avg.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                .mean()).shift(1, freq=self.frequency)
        return self.smoothedPx


    def percentageChange(self, col):
        """
        Logarithmic returns for the selected column

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are interested in obtaining the log returns. By default = ['price']

        Returns:
        --------
        Dataframe with the logarithmic returns for the selected column

        """

        self.smoothedPx['pctChg'] = pd.DataFrame((np.log(1 + self.smoothedPx[col].pct_change()))).fillna(0)
        return self.smoothedPx



    def wicksFinder(self, col):
        """
        Finds the max and lows for the selected frequency.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are interested in obtaining the log returns. By default = ['price']

        Returns:
        --------
        Dataframe with the max and min wicks during the selected period of time.

        """

        maxList = []
        minList = []
        for val in self.df[col].groupby(pd.Grouper(freq=self.frequency)):
            maxList.append(max(val[1]))
            minList.append(min(val[1]))

        self.highLow = pd.concat([pd.DataFrame(maxList).rename(columns={0: 'High'}),
                                  pd.DataFrame(minList).rename(columns={0: 'Low'})], axis=1)

        return self.highLow.reset_index()



    def createDataFrame(self):
        """
        Creates a dataframe with all the previos functions executed and added as a column.

        Arguments:
        ----------
        ()

        Returns:
        --------
        Dataframe with features to work on.

        """

        dataset = pd.concat([self.dataTotals, self.dataTransact, self.logReturns], axis=1)
        wicks = self.highLow
        dataset = pd.concat([dataset.reset_index(),
                                wicks.reset_index(drop=True)], axis=1).set_index('timestamp').drop(columns='index')

        print(dataset)
        return dataset.to_csv(f'data/{self.frequency}_{str(dataset.index[0]).split(" ")[0]}to{str(dataset.index[-1]).split(" ")[0]}.csv')



