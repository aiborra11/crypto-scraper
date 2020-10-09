import pandas as pd
from datetime import datetime, timedelta


def dates_converter(day1='20141122'):               # 20141122 is the first available date provided in the website
    """
    Taking the first available date (if anything else is specified when calling the function) and converts it
    into dateformat to add one day and iterate every csv file in the website.

    Arguments:
    ----------
        day1 {[str]} -- date from which we want to start to collect data.

    Returns:
    --------
        {[list]}
            list of dates we will use to collect data.

    """

    dates = []
    date_format = datetime.strptime(day1, '%Y%m%d')
    max_date = int(datetime.today().strftime('%Y%m%d'))

    for day in range(2500):
        next_day = str(date_format + timedelta(days=day))
        next_day_format = next_day.replace('-', '').split()[0]

        if int(next_day_format) <= max_date:
            dates.append(next_day_format)

    return dates


def csv_creator(df, crypto, name):
    """
    Creates a csv file compressed as gzip

    Arguments:
    ----------
        df {[dataframe]} -- dataframe containing scrapped data.
        name {[str]} -- how we want to name our csv file.
        crypto {[str]} -- crypto data we collected.

    Returns:
    --------
        {[csv]}
           gzip file with the data for the desired cryptocurrency.

    """

    return df.to_csv(f'./data.nosync/{crypto}_{name}.gz', index=True, compression='gzip')


def data_obtainer(interval, crypto, name):
    """
    Iterates through the dates list collecting the data for the specified cryptocurrency. Since we are
    working with a huge amount of data we need to limit the number of dates to collect. Otherwise the computer
    might run out of memory not allowing the code to finish. Hence, we will create different csv to process and
    concatenate at a later stage.

    Arguments:
    ----------
        interval {[list]} -- interval of dates we are going to collect.
        name {[str]} -- how we want to name our csv file.
        crypto {[str]} -- crypto data we are willing to collect.

    Returns:
    --------
        {[csv]}
           gzip file with the data for the desired cryptocurrency.

    """

    crypto_data = pd.DataFrame()
    num_of_dates = 500
    no_data_found = []

    for num, date in enumerate(interval):  # Limit the amount of iterations. Computer's memory might collapse.
        if num % 10 == 0:
            print(f'Progress: {num / num_of_dates * 100:.2f}%')

        url = f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{date}.csv.gz'
        try:                                                    # Data is updated everyday at 05:40am
            data = pd.read_csv(url)
            data_symb = data[data['symbol'] == crypto]
            crypto_data = pd.concat([crypto_data, data_symb])

        except:
            no_data_found.append(date)
            print(f'No data available for {date}.gz')

    print(f'Check data for dates: [{no_data_found}]')

    return csv_creator(crypto_data, name, crypto)


