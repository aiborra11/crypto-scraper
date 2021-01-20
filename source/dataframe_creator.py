import pandas as pd
import numpy as np

from datetime import datetime
from os import listdir
from .database import Database


class ProcessData(Database):
    """
    Inherits Database class to bring raw data and transform it using its own methods.

    """

    def __init__(self, processed=True):
        """
        Executing essential functions: connecting to mongo, selecting db and collection, and bringing raw data to
        transform.

        """
        # Bringing raw data from the mongoDB
        super().__init__(processed)
        selected_collection = self.select_collection(processed)
        self.df = self.collect_raw_data(selected_collection)[0]

        # Essential data preprocessing (cleaning and summarizing features into less columns)
        self.noDuplicates = self.duplicates_remover()
        self.dataClean = self.data_cleaner(['', 'symbol', 'trdMatchID'])
        self.battle = self.bulls_vs_bears(['size', 'grossValue'])


    def duplicates_remover(self):
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
        self.noDuplicates = self.df.drop_duplicates(subset=None, keep='first')
        self.noDuplicates['timestamp'] = self.noDuplicates['timestamp'].map(
            lambda t: datetime.strptime(str(t)[:19].replace('D', ' '), '%Y-%m-%d %H:%M:%S'))
        print(f'{len(self.df) - len(self.noDuplicates)} duplicates have been removed from your dataframe.')
        return self.noDuplicates

    def data_cleaner(self, columns_list):
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
        columns_delete = columns_list
        self.dataClean = self.noDuplicates[[col for col in self.noDuplicates.columns if col not in columns_delete]] \
            .rename(columns={'foreignNotional': 'usdTotal', 'homeNotional': 'btcTotal'})
        return self.dataClean.set_index('timestamp')

    def bulls_vs_bears(self, cols):
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

    def sum_grouper(self, cols):
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
                                    .sum()).shift(1, freq=self.frequency)
        return self.dataSum

    def counter_grouper(self, cols):
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

        transactions = pd.DataFrame(self.battle.groupby([pd.Grouper(freq=self.frequency), self.battle['side'] == 'Buy'])
                                    [cols].count()).unstack('side').shift(1, freq=self.frequency)
        transactions.columns = transactions.columns.droplevel()
        self.dataTransact = transactions.rename(columns={0: 'bearTransact', 1: 'bullTransact'})
        self.dataTransact[['bearTransact', 'bullTransact']] = self.dataTransact[['bearTransact',
                                                                                 'bullTransact']].fillna(0)
        self.dataTransact['warTransact'] = self.dataTransact.bullTransact - self.dataTransact.bearTransact
        self.dataTransact['totalTransact'] = self.dataTransact.bullTransact + self.dataTransact.bearTransact
        return self.dataTransact

    def ema_smoother(self, cols):
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
        for t in self.dataTransact['totalTransact'].values:
            exp_mov_avg = self.battle.ewm(span=t, adjust=False).mean()

        self.dataPx = pd.DataFrame(exp_mov_avg.groupby(pd.Grouper(freq=self.frequency))[cols].mean())\
                        .shift(1, freq=self.frequency)
        return self.dataPx

    # def percentage_change(self, col):
    #     """
    #     Logarithmic returns for the selected column
    #
    #     Arguments:
    #     ----------
    #     cols {[list]} -- Name of the columns we are interested in obtaining the log returns. By default = ['price']
    #
    #     Returns:
    #     --------
    #     {[DataFrame]}
    #         Dataframe with the logarithmic returns for the selected column
    #
    #     """
    #
    #     # print('Calculating log returns...')
    #     self.log_returns['pctChg'] = pd.DataFrame((np.log(1 + self.dataPx[col].pct_change()))).fillna(0)
    #
    #     return self.log_returns

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
        self.dataPx[['Open', 'High', 'Low', 'Close']] = self.dataClean.groupby(pd.Grouper(
                                                        freq=self.frequency))['price'].agg(
                                                                                Open="first",
                                                                                High="max",
                                                                                Low="min",
                                                                                Close="last"
                                                                        ).shift(1, freq=self.frequency)
        return self.dataPx

    def create_dataframe(self, dataset):
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
            final['High'] = final['High'].fillna(final.Close)
            final['Low'] = final['Low'].fillna(final.Close)
            final['Open'] = final['Open'].fillna(final.Close)
            final = final.fillna(0)
            print(f'Your dataframe has {len(all_data)} rows in total.')
            return final.to_csv(f'data.nosync/{self.frequency}_general.csv', index=False)

        else:
            print(f'Starting new df for {self.frequency} data.')
            all_data = pd.DataFrame()
            all_data = pd.concat([all_data, dataset])
            all_data['Close'] = all_data['Close'].ffill()
            all_data['LogReturns'] = pd.DataFrame((np.log(1 + all_data['Close'].pct_change()))).fillna(0)
            all_data['High'] = all_data['High'].fillna(all_data.Close)
            all_data['Low'] = all_data['Low'].fillna(all_data.Close)
            all_data['Open'] = all_data['Open'].fillna(all_data.Close)
            all_data = all_data.fillna(0)
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

    processed_data = ProcessData(df_raw, frequency)

    data_totals = processed_data.sum_grouper(cols=['size', 'grossValue', 'btcTotal', 'usdTotal', 'ContractsTraded_size',
                                                   'ContractsTraded_grossValue']).fillna(0)
    processed_data.counter_grouper(cols=['side']).fillna(0)
    data_px = processed_data.ohcl()

    dataset = pd.concat([data_totals, data_px], axis=1).reset_index()
    dataset.columns = ['Timestamp', 'Size', 'GrossValue', 'Total_BTC', 'Total_USD', 'ContractsTraded_Size',
                       'ContractsTraded_GrossValue', 'BearTransacts', 'BullTransacts', 'WarTransacts',
                       'TotalTransacts', 'High', 'Low', 'Open', 'Close']
    return processed_data.create_dataframe(dataset)

# 17699    2018-10-26D23:59:59.810926000
