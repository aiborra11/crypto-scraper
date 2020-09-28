import pandas as pd
import numpy as np
from datetime import datetime
from os import listdir


class processData(object):
    """
    Converts raw data into a determined frequency and creates new features as columns.

    """
    def __init__(self, df, frequency):
        """
        Allowing different functions to inherit from each other.

        """
        self.df = df
        self.noDuplicates = self.duplicatesRemover()
        self.dataClean = self.dataCleaner(['', 'symbol', 'trdMatchID'])
        self.battle = self.bullsVsBears(['size', 'grossValue'])

        self.frequency = frequency

        self.dataTotals = self.sumGrouper(cols=['size', 'grossValue', 'btcTotal', 'usdTotal', 'ContractsTraded_size', 'ContractsTraded_grossValue'])
        self.dataTransact = self.counterGrouper(cols=['side'])
        self.smoothedPx = self.emaSmoother(cols=['price'])
        self.px = self.ohcl(cols='price')
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
        self.df = self.df[self.df['timestamp'].notna()]
        self.noDuplicates = self.df.drop_duplicates(subset='trdMatchID', keep='first')
        # print(f'{len(self.df) - len(self.noDuplicates)} rows have been deleted since they were duplicated')
        # self.noDuplicates = self.battle.reset_index()
        self.noDuplicates['timestamp'] = self.noDuplicates['timestamp'].map(
            lambda t: datetime.strptime(str(t)[:19].replace('D', ' '), '%Y-%m-%d %H:%M:%S'))
        # self.battle.set_index('timestamp', inplace=True)
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
        # print('Cleaning columns we no longer need...')
        columns_delete = columnsList
        self.dataClean = self.noDuplicates[[col for col in self.noDuplicates.columns if col not in columns_delete]] \
            .rename(columns={'foreignNotional': 'usdTotal',
                                'homeNotional': 'btcTotal'})
        return self.dataClean.set_index('timestamp')


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
        # print('Converting shorts into negative and longs into positives...')

        filter_sell = self.dataClean['side'] == 'Sell'
        for col in cols:
            self.dataClean.loc[filter_sell, f'ContractsTraded_{col}'] = - self.dataClean.loc[filter_sell, col]
            self.dataClean.loc[~filter_sell, f'ContractsTraded_{col}'] = self.dataClean.loc[~filter_sell, col]
        return self.dataClean


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
        {[DataFrame]}
            Dataframe with the sum of the desired columns based on the selected frequency.

        """
        # try:
        self.dataSum = pd.DataFrame(self.battle.groupby([pd.Grouper(freq=self.frequency)])[cols]
                                                                .sum()).shift(1, freq=self.frequency)
        return self.dataSum
        # except:
        #     self.battle = self.battle.reset_index()
        #     self.battle['timestamp'] = self.battle['timestamp'].map(lambda t: datetime.strptime(str(t)[:19].replace('D', ' '), '%Y-%m-%d %H:%M:%S'))
        #     self.battle.set_index('timestamp', inplace=True)
        # #     self.battle = self.battle.reset_index()
        # #     for num, val in enumerate(self.battle['timestamp']):
        # #         self.battle['timestamp'] = str(val).replace(' ', 'D')
        # #
        # # # self.battle = self.battle.reset_index()
        # # print(self.battle)
        # # self.battle = self.battle.drop_duplicates(keep='first')
        # # self.battle['timestamp'] = self.battle['timestamp'].map(lambda t: datetime.strptime(str(t)[:-3], '%Y-%m-%dD%H:%M:%S.%f')).set_index('timestamp')
        # #
        #     self.dataSum = pd.DataFrame(self.battle.groupby([pd.Grouper(freq=self.frequency)])[cols]
        #                                 .sum()).shift(1, freq=self.frequency)
        #
        #     print(self.dataSum)
        #     return self.dataSum

    def counterGrouper(self, cols):
        """
        Counts the number of shorts and longs that have been performed during the selected frequency.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are willing count. By default = ['side']

        Returns:
        --------
        {[DataFrame]}
            Dataframe with the number of transactions performed during the selected frequency.
                bearTransact: number of bearish transactions.
                bullTransact: number of bullish transactions.
                warTransact: bull transactions - bear transactions
                totalTransact: bull transactions + bear transactions

        """

        # print('Calculating the total number of transactions...')
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
        {[DataFrame]}
            Dataframe with the cryptocurrency smoothed price.


        """

        # print('Calculating the smoothed price...')
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
        {[DataFrame]}
            Dataframe with the logarithmic returns for the selected column

        """

        # print('Calculating log returns...')
        self.smoothedPx['pctChg'] = pd.DataFrame((np.log(1 + self.smoothedPx[col].pct_change()))).fillna(0)
        return self.smoothedPx



    def ohcl(self, cols):
        """
        Finds the max and lows for the selected frequency.

        Arguments:
        ----------
        cols {[list]} -- Name of the columns we are interested in obtaining the log returns. By default = ['price']

        Returns:
        --------
        {[DataFrame]}
            Dataframe with the max and min wicks during the selected period of time.

        """

        # print('Calculating open, high, low and close values...')

        self.smoothedPx['High'] = pd.DataFrame(self.dataClean.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                            .max()).shift(1, freq=self.frequency)

        self.smoothedPx['Low'] = pd.DataFrame(self.dataClean.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                            .min()).shift(1, freq=self.frequency)

        self.smoothedPx['Open'] = pd.DataFrame(self.dataClean.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                            .first()).shift(1, freq=self.frequency)

        self.smoothedPx['Close'] = pd.DataFrame(self.dataClean.groupby(pd.Grouper(freq=self.frequency))[cols]
                                                                            .nth(-1)).shift(1, freq=self.frequency)
        return self.smoothedPx


    def createDataFrame(self):
        """
        Creates a dataframe with all the previos functions executed and added as a column.

        Arguments:
        ----------
        ()

        Returns:
        --------
        {[csv]}
            Dataframe stored in your data folder with features to work on.

        """

        # print('Creating your dataframe...')
        dataset = pd.concat([self.dataTotals, self.dataTransact, self.logReturns], axis=1)
        dataset.columns = ['Size', 'GrossValue', 'Total_BTC', 'Total_USD', 'ContractsTraded_Size',
                            'ContractsTraded_GrossValue', 'BearTransacts', 'BullTransacts', 'WarTransacts',
                            'TotalTransacts', 'Price_exp', 'High', 'Low', 'Open', 'Close', 'LogReturns']

        dataset_csv = dataset.to_csv(f'data/{self.frequency}_{str(dataset.index[0]).split(" ")[0]}.csv')

        files = sorted(listdir('./data'))
        all_data = pd.DataFrame()
        for f in files:
            all_data = pd.concat([all_data, pd.read_csv(f'./data/{f}')])

        return all_data.to_csv(f'data/{self.frequency}_general.csv')