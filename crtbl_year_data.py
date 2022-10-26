import time
import requests
import json
import pandas as pd
from functions import create_connection_mysql, create_table_connection_mysql

pd.options.display.float_format = '{:.2f}'.format

#####-----------Create Connection
connection = create_connection_mysql()
cursor = connection.cursor()

with open("time_series.sql","r") as file:
    time_data_sql = file.read()

#Pull Records
df = pd.read_sql_query(
    time_data_sql, connection)



#https://api.coingecko.com/api/v3/coins/superbonds/ohlc?vs_currency=usd&days=365

import time
import datetime

yesterday = datetime.date.today() - datetime.timedelta(365)
unix_time= yesterday.strftime("%s") #Second as a decimal number [00,61] (or Unix Timestamp)


RightNow = time.time()

from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

cg.get_coin_market_chart_range_by_id(id=['superbonds'],vs_currency='usd', from_timestamp=unix_time,to_timestamp=RightNow)

a