import pandas as pd
from datetime import datetime, timedelta


def dates_converter(day1='20141122'):               #20141122 is the first date provided in the website
    """Taking the first available date (if anything else is specified when calling the function) and converts it
    into dateformat to add one day and iterate every csv file in the website.

    Arguments:
        day1 {[str]} -- date from which we want to start to collect data.

    Returns:
        [list] -- list of dates we will use to collect data.
    """
    dates = []
    date_format = datetime.strptime(day1, '%Y%m%d')
    last_date = int(datetime.today().strftime('%Y%m%d'))

    for day in range(2500):
        next_day = str(date_format + timedelta(days=day))
        iterator = next_day.replace('-', '').split()[0]

        if int(iterator) <= last_date:
            dates.append(iterator)

    return dates



def csv_creator(df, crypto, name):
    return df.to_csv(f'../data/{crypto}_{name}.csv', index=True)



def data_obtainer(interval, name, crypto):
    xbt_btc = pd.DataFrame()
    num_of_dates = 500
    no_data_found = []
    for i, date in enumerate(interval):  # limit the amount of iterations. Otherwise, the computer's memory reach a point where it stops working. Remember to modify the value to iterate until the last value.
        if i % 10 == 0:
            print(f'Progress: {i / num_of_dates * 100:.2f}%')

        url = f'https://s3-eu-west-1.amazonaws.com/public-testnet.bitmex.com/data/trade/{date}.csv.gz'      #Data updated everyday at 05:40am

        try:
            data = pd.read_csv(url)
            data_xbt = data[data['symbol'] == crypto]
            xbt_btc = pd.concat([xbt_btc, data_xbt])
        except:
            no_data_found.append(date)
            print(f'No data available in {date}.csv')

    print(f'Check data from:{no_data_found}')
    return csv_creator(xbt_btc, name)