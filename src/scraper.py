
import boto3
import json
import pandas as pd
from alpha_vantage.timeseries import TimeSeries


# Your key here
key = 'OQX9VO6JOXSLUBGF'
ts = TimeSeries(key)
aapl, meta = ts.get_intraday(symbol='AAPL', interval='15min', outputsize='full')
print(aapl)

def parse_ts(ts:dict):
    """Given time series data in a dictionary format, return results in a pandas df"""
    return pd.DataFrame.from_dict(aapl, orient='index')


def write_json(ts:dict, key:str, bucket:str='trader-con'):
    """Take ts data return by API and write to s3 as JSON"""
    session = boto3.session.Session(profile_name='default')
    s3 = session.resource('s3')
    s3.Object(bucket, key).put(Body=(bytes(json.dumps(ts, indent=2).encode('UTF-8'))))


write_json(ts = aapl, key = 'dev/aapl.json')



