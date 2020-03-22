import pandas as pd
import numpy as np
import boto3
import json 
import time
import datetime
from io import StringIO
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators


def s3_read(bucket, key):
    session = boto3.Session(profile_name='trader')
    s3 = session.client('s3')
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    body = s3_object['Body']
    return body.read().decode('utf-8')

def df_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    session = boto3.Session(profile_name='trader')
    s3_resource = session.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return None

def df_from_s3(bucket, key):
    session = boto3.Session(profile_name='trader')
    client = session.client('s3')
    csv_obj = client.get_object(Bucket=bucket, Key=key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    return pd.read_csv(StringIO(csv_string))


def json_to_s3(bucket:str, key:str, data:dict):
    session = boto3.Session(profile_name='trader')
    s3 = session.resource('s3')
    s3object = s3.Object(bucket, key)

    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )


def json_from_s3(bucket, key):
    session = boto3.Session(profile_name='trader')
    s3 = session.resource('s3')
    content_object = s3.Object(bucket, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def parse_ts(ts:dict):
    """Given time series data in a dictionary format, return results in a pandas df"""
    return pd.DataFrame.from_dict(ts, orient='index')


def get_local_minmax(rsi, min_max='min'):
    is_loc = []
    data = rsi['RSI'].values

    for i, p in enumerate(data):
        if i in [0, 1, len(data)-1, len(data)-2]:
            ind = 0
        elif min_max == 'min' and p < data[i+1] and p < data[i-1] and p < data[i+2] and p < data[i-2]:
            ind = 1
        elif min_max == 'max' and p > data[i+1] and p > data[i-1] and p > data[i+2] and p > data[i-2]:
            ind = 1
        else:
            ind = 0
        is_loc.append(ind)
    return is_loc


def get_portfolio():
    portfolio = json_from_s3(bucket='trader-con', key='portfolios/portfolio.json')

    return portfolio


def get_stock_price(key, ticker):
    # Get raw timeseries data
    ts = TimeSeries(key)
    price, meta = ts.get_intraday(symbol=ticker, interval='60min', outputsize='full')
    price = parse_ts(price).reset_index().rename(columns={'index':'ts',
                                                               '4. close':'closing_price'})[['ts','closing_price']]
    price['ts'] = pd.to_datetime(price['ts'])
    price['closing_price'] = price['closing_price'].astype(float)
    return price


def get_rsi(key, ticker):
    # get RSI values
    ti = TechIndicators(key)
    rsi = parse_ts(ti.get_rsi(symbol=ticker, interval='60min')[0]).reset_index().rename(columns={'index':'ts'})
    # format RSI data 
    rsi['is_loc_min'] = get_local_minmax(rsi=rsi, min_max='min')
    rsi['is_loc_max'] = get_local_minmax(rsi=rsi, min_max='max')
    rsi['ts'] = pd.to_datetime(rsi['ts'])
    rsi['RSI'] = rsi['RSI'].astype(float)
    rsi['low_value'] = (rsi['RSI'] < 25).astype(int)
    rsi['high_value'] = (rsi['RSI'] > 75).astype(int)
    rsi = rsi.rename(columns={'index':'ts'})
    
    # find buy points
    poss_b = rsi[(rsi.is_loc_min == 1) & (rsi.low_value == 1)].reset_index().rename(columns={'index':'ind'})
    poss_b['ind_delta'] = poss_b.ind.diff()
    poss_b['rsi_delta'] = poss_b.RSI.diff()
    poss_b['buy_point'] = [1 if i <= 10 and r > 0 else 0 for i, r in zip(poss_b['ind_delta'], poss_b['rsi_delta'])] 
    poss_b['buy_point'] = poss_b['buy_point'].fillna(0)
    
    # find sell points
    poss_s = rsi[(rsi.is_loc_max == 1) & (rsi.high_value == 1)].reset_index().rename(columns={'index':'ind'})
    poss_s['ind_delta'] = poss_s.ind.diff()
    poss_s['rsi_delta'] = poss_s.RSI.diff()
    poss_s['sell_point'] = [1 if i <= 10 and r < 0 else 0 for i, r in zip(poss_s['ind_delta'], poss_s['rsi_delta'])] 
    poss_s['sell_point'] = poss_s['sell_point'].fillna(0)
    
    # merge together
    poss = pd.merge(poss_b, poss_s, how='outer',on='ts')
    
    return pd.merge(rsi, poss[['ts','sell_point','buy_point']], how='left', on='ts')


def trade(t:str, ticker:str, shares:int, price:float, portfolio:dict):
    """Buy or sell shares of a given stock and document the changes"""
    total = price*shares
    trade_details = {'type':t,
                     'datetime':str(datetime.datetime.now()),
                    'ticker':ticker,
                    'shares':shares,
                    'price':price,
                    'total':total}
    
    # Update portfolio
    if t == 'buy':
        portfolio['cash'] -= total
        if ticker not in portfolio.keys():
            portfolio['stock'][ticker] = shares
        else:
            portfolio['stock'][ticker] += shares
    elif t == 'sell':
        if portfolio['stock'][ticker] < shares:
            print('Error: Not enough shares to sell')
        else: 
            portfolio['stock'][ticker] -= shares
            portfolio['cash'] += total
    else:
        return 'Error: only buy and seel types allowed'
    
    print('Updated Portfolio:')
    print(portfolio)

    # write updated portfolio
    json_to_s3(bucket='trader-con', key='portfolios/portfolio.json', data=portfolio)
    
    # Write trade details
    timestr = time.strftime("%Y%m%d-%H%M%S")
    json_to_s3(bucket='trader-con', key=f'trades/trade_{timestr}.json', data=trade_details)
    with open(f'Data/trades/trade_{timestr}.json', 'w') as d:
        json.dump(trade_details, d)
    
    return None


