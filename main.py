import time

from source.crypto_scraper import *


def main(crypto):
    print('Preparing your data...')
    dates = dates_converter(day1='20141122')
    print('Charging first csv...')
    data1 = pd.DataFrame(data_obtainer(dates[:500], crypto, 'data1gzip'))
    time.sleep(15)
    print('Charging second csv...')
    data2 = pd.DataFrame(data_obtainer(dates[500:1000], crypto, 'data2gzip'))
    print('Charging third csv...')
    time.sleep(15)
    data3 = pd.DataFrame(data_obtainer(dates[1000:1500], crypto, 'data3gzip'))
    print('Charging fourth csv...')
    time.sleep(15)
    data4 = pd.DataFrame(data_obtainer(dates[1500:], crypto, 'data4gzip'))

if __name__ == "__main__":                #Feel free to replace 'XBTUSD' by the desired crypto
    main(crypto='XBTUSD')
