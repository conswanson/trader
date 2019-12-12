import pandas as pd
import numpy as np
from alpha_vantage.timeseries import TimeSeries


# Your key here
key = 'OQX9VO6JOXSLUBGF'
ts = TimeSeries(key)
aapl, meta = ts.get_intraday(symbol='AAPL')
print(aapl)


def parse_ts(ts:dict):
    """Given time series data in a dictionary format, return results in a pandas df"""
    pd.DataFrame.from_dict(aapl, orient='index')
