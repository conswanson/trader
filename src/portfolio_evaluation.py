import pandas as pd
import json
from alpha_vantage.timeseries import TimeSeries
import boto3
from trader_utils import *


def main():

    # get API keys
    key = s3_read(bucket='trader-con', key='secrets/alpha_vantage_secrets.txt')

    # read in portfolio (TO DO: switch to s3 location
    portfolio = get_portfolio()

    # get cash value
    cash = portfolio['cash']
    print(cash)

    # get stock value
    ts = TimeSeries(key)
    stock_value = 0
    for ticker in portfolio['stock'].keys():
        print(ticker)
        # get number of shares
        shares = portfolio['stock'][ticker]

        # get stock price
        prices, meta = ts.get_quote_endpoint(symbol=ticker)
        price = float(prices['05. price'])
        print(f'Shares: {shares}, Share Price: {price} \n')

        stock_value += 1 * (shares * price)

    # compute total
    total = cash + stock_value
    print(f'Total Portfolio Value: {total}')
    return total



if __name__ == '__main__':
    main()