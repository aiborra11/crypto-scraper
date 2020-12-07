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
        self.noDuplicates['timestamp'] = self.noDuplicates['timestamp'].map(
            lambda t: datetime.strptime(str(t)[:19].replace('D', ' '), '%Y-%m-%d %H:%M:%S'))

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
        self.dataSum = pd.DataFrame(self.battle.groupby([pd.Grouper(freq=self.frequency)])[cols]
                                                                            .sum())
        return self.dataSum


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
        self.dataTransact = transactions.rename(columns={0: 'bearTransact', 1: 'bullTransact'})
        self.dataTransact[['bearTransact', 'bullTransact']] = self.dataTransact[['bearTransact',
                                                                                 'bullTransact']].fillna(0)
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

        self.dataPx = pd.DataFrame(exp_mov_avg.groupby(pd.Grouper(freq=self.frequency))[cols].mean())

        return self.dataPx


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
        self.log_returns['pctChg'] = pd.DataFrame((np.log(1 + self.dataPx[col].pct_change()))).fillna(0)

        return self.log_returns



    def ohcl(self):
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
        self.dataPx = self.dataTransact
        self.dataPx[['Open', 'High', 'Low', 'Close']] = self.dataClean.groupby(pd.Grouper(freq=self.frequency))['price'].agg(
                                                                                                Open="first",
                                                                                                High="max",
                                                                                                Low="min",
                                                                                                Close="last"
                                                                                        )
        return self.dataPx.shift(1, freq=self.frequency)


    def createDataFrame(self, dataset):
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

        print('Creating your dataframe...')
        files = sorted(listdir('data.nosync/'))
        exist_df = [f for f in files if f.startswith(f'{self.frequency}_')]

        if exist_df:
            print('Continuing from previous df')

            all_data = pd.read_csv(f'data.nosync/{self.frequency}_general.csv')

            final = pd.concat([all_data, dataset])
            final['Close'] = final['Close'].ffill()
            final['LogReturns'] = pd.DataFrame((np.log(1 + final['Close'].pct_change()))).fillna(0)
            # final[['High', 'Low', 'Open']] = final[['High', 'Low', 'Open']].fillna(final.Close)
            print(f'Your dataframe has {len(all_data)} rows in total.')

            return final.to_csv(f'data.nosync/{self.frequency}_general.csv', index=False)


        else:
            print(f'Starting new df for {self.frequency} data.')
            all_data = pd.DataFrame()
            all_data = pd.concat([all_data, dataset])
            all_data['Close'] = all_data['Close'].ffill()
            all_data['LogReturns'] = pd.DataFrame((np.log(1 + all_data['Close'].pct_change()))).fillna(0)
            # all_data[['High', 'Low', 'Open']] = all_data[['High', 'Low', 'Open']].fillna(all_data.Close)
            print(f'Your dataframe has {len(all_data)} rows in total.')

            return all_data.to_csv(f'data.nosync/{self.frequency}_general.csv', index=False)





def get_data(df_raw, frequency):
    """
    Main to execute all previous functions

    Arguments:
    ----------
        df_raw {[DataFrame]} -- Initial dataframe containing the raw data.
        frequency {[str]} -- Desired frequency you'd like to receive the final data.

    Returns:
    --------
    {[csv]}
        Dataframe stored in your data folder with features to work on.

    """
    processedData = processData(df_raw, frequency)

    dataTotals = processedData.sumGrouper(cols=['size', 'grossValue', 'btcTotal', 'usdTotal', 'ContractsTraded_size',
                                                'ContractsTraded_grossValue']).fillna(0)
    processedData.counterGrouper(cols=['side']).fillna(0)
    dataPx = processedData.ohcl()

    dataset = pd.concat([dataTotals, dataPx], axis=1).reset_index()
    dataset.columns = ['Timestamp', 'Size', 'GrossValue', 'Total_BTC', 'Total_USD', 'ContractsTraded_Size',
                       'ContractsTraded_GrossValue', 'BearTransacts', 'BullTransacts', 'WarTransacts',
                       'TotalTransacts', 'High', 'Low', 'Open', 'Close']

    return processedData.createDataFrame(dataset)


