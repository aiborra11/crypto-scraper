# import pandas as pd
from datetime import datetime, timedelta


def dates_converter(day1='20141122'):
    """
    Taking the first available date (if anything else is specified when calling the function) and converts it
    into dateformat to add one day and iterate every csv file in the website.

    Arguments:
        day1 {[str]} -- date from which we want to start to collect data.

    Returns:
        [list] -- list of dates we will use to collect data.
    """
    dates = []
    date_format = datetime.strptime(day1, '%Y%m%d')
    for i in range(2500):
        next_day = str(date_format + timedelta(days=i))
        iterator = next_day.replace('-', '').split()[0]
        dates.append(iterator)
    return dates

# dates_converter()
print(dates_converter())

