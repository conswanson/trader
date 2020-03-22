import pandas as pd
import numpy as np
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
import time
import datetime
from functools import reduce
import sys
from io import StringIO
from trader_utils import *


def get_input_params():
    tickers = sys.argv[1]
    if tickers == 'all':
        tickers = ['V', 'AAPL', 'FB']
    else:
        tickers = tickers.split(',')
    return tickers


def collect_data(ticker: str, interval: str, key):
    """ Collect data from alpha vantage. Currently collects current stock price
        and 5 technical indicators """

    print(f'Collecting data for {ticker}')
    ts = TimeSeries(key)
    ti = TechIndicators(key)

    # get stock price
    price, meta = ts.get_intraday(symbol=ticker,
                                  interval=interval, outputsize='full')
    price = parse_ts(price).reset_index().rename(columns={'index': 'ts'})
    price['ts'] = pd.to_datetime(price['ts'])
    print(price.head())
    # pause to avoid API throttling limits
    time.sleep(45)

    # get technical indicators
    print('Collecting Technical Indicators')
    rsi = parse_ts(ti.get_rsi(symbol=ticker,
                              interval=interval)[0]).reset_index().rename(columns={'index': 'ts'})
    rsi['ts'] = pd.to_datetime(rsi['ts'])
    print(rsi.head())
    macd = parse_ts(ti.get_macd(symbol=ticker,
                                interval=interval)[0]).reset_index().rename(columns={'index': 'ts'})
    macd['ts'] = pd.to_datetime(macd['ts'])
    print(macd.head())
    sar = parse_ts(ti.get_sar(symbol=ticker,
                              interval=interval)[0]).reset_index().rename(columns={'index': 'ts'})
    sar['ts'] = pd.to_datetime(sar['ts'])
    print(sar.head())
    time.sleep(45)
    adx = parse_ts(ti.get_adx(symbol=ticker,
                              interval=interval)[0]).reset_index().rename(columns={'index': 'ts'})
    adx['ts'] = pd.to_datetime(adx['ts'])
    print(adx.head())
    stoch = parse_ts(ti.get_stoch(symbol=ticker,
                                  interval=interval)[0]).reset_index().rename(columns={'index': 'ts'})
    stoch['ts'] = pd.to_datetime(stoch['ts'])
    print(stoch.head())

    # merge into single data set
    dfs = [price, rsi, macd, sar, adx, stoch]
    df_final = reduce(lambda left, right: pd.merge(left, right, on='ts', how='outer'), dfs)

    return df_final.sort_values('ts').dropna()


def main(ticker, k):
    df = collect_data(ticker=ticker, interval='15min', key=k)
    print(len(df))
    print(df.head())

    today = datetime.datetime.now().date()
    print(today)

    # write to s3
    df_to_s3(df=df, bucket='trader-con', key=f'data/{ticker}/{ticker}_{today}.csv')


if __name__ == '__main__':
    tickers = get_input_params()
    k = s3_read(bucket='trader-con', key='secrets/alpha_vantage_secrets.txt')
    for ticker in tickers:
        main(ticker=ticker, k=k)
    print('Process complete')