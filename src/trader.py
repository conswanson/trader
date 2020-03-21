import pandas as pd
import numpy as np
import boto3
import json
import time
import datetime
import sys
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from trader_utils import *

def get_input_params():
    tickers = sys.argv[1]
    if tickers == 'all':
        tickers = ['V', 'AAPL', 'BA', 'MSFT', 'FB', 'NFLX', 'EA']
    else:
        tickers = tickers.split(',')
    return tickers

def main(k, ticker):
    # pull data from API
    stock = get_stock_price(key=k, ticker=ticker)
    rsi = get_rsi(key=k, ticker=ticker)


    # merge together
    df = pd.merge(stock, rsi, how='inner',on='ts').sort_values('ts').reset_index().drop(columns='index')
    df.tail(3)

    ########################## BUY #######################
    if df.iloc[-3]['buy_point'] == 1:
    #if 1==1:
        print('Making Purchase!')
        # time to buy
        # opening portfolio
        with open('Data/portfolio.json') as json_file:
            portfolio = json.load(json_file)

        print('Current Portfolio: ')
        print(portfolio)

        cash = portfolio['cash']
        price = df.iloc[-1]['closing_price']

        shares = np.floor((cash*0.05) / price)
        if shares < 1:
            print('Low Cash')
        else:
            trade(t='buy',ticker=ticker, shares=shares, price=price, portfolio=portfolio)

    ########################## SELL #######################
    elif df.iloc[-3]['sell_point'] == 1:
    #elif 1==1:
        print('Making Sale!')
        # time to sell

        # opening portfolio
        with open('Data/portfolio.json') as json_file:
            portfolio = json.load(json_file)

        print('Current Portfolio: ')
        print(portfolio)
        cash = portfolio['cash']
        price = df.iloc[-1]['closing_price']

        if ticker in portfolio['stock'].keys():
            shares = portfolio['stock'][ticker]
            if shares < 1:
                print('No shares to sell')
            else:
                trade(t='sell', ticker=ticker, shares=shares, price=price, portfolio=portfolio)
        else:
            print('No shares to sell')
    else:
        print('No buy or sell indicators')


if __name__ == '__main__':
    tickers = get_input_params()
    k = s3_read(bucket='trader-con', key='secrets/alpha_vantage_secrets.txt')

    for ticker in tickers:
        print(f'Checking market for {ticker}')
        main(k=k, ticker=ticker)
        time.sleep(30)

    print('Complete')


