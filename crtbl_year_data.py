import time
import pandas as pd
from functions import create_connection_mysql, create_table_connection_mysql
import datetime
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

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
YesterYear = datetime.date.today() - datetime.timedelta(365)
YesterYear= YesterYear.strftime("%s") #Second as a decimal number [00,61] (or Unix Timestamp)
RightNow = time.time()

df_insert = pd.DataFrame(columns=['ID','SYMBOL','NAME','PLATFORMS','PLATFORM_HASH','TYPE','TIMEPSTAMP','VALUE'])

print(len(df))


for i in range(len(df)):
#for i in range(1):
    print(i)
    ID = df.iloc[i]['ID']
    SYMBOL = df.iloc[i]['SYMBOL']
    NAME = df.iloc[i]['NAME']
    PLATFORMS = df.iloc[i]['PLATFORMS']
    PLATFORM_HASH = df.iloc[i]['PLATFORM_HASH']
    try:
        CHART = cg.get_coin_market_chart_range_by_id(id=ID, vs_currency='usd', from_timestamp=YesterYear, to_timestamp=RightNow)
        #Gather different types of informatin (prices,market_caps, and total_volumes)
        type_aggregate = [x for x in CHART]
        for j in type_aggregate:
            TYPE = j
            TYPE_CHART = CHART[j]
            for k in TYPE_CHART:
                TIME_STAMP = k[0]
                VALUE = k[1]
                sql = "insert into Crypto_Year_Price ( VALUE, ID, SYMBOL, NAME, PLATFORMS, PLATFORM_HASH, TYPE, TIMESTAMP)"\
                      " values( '"+str(VALUE)+"','"+ID+"','"+SYMBOL+"','"+NAME+"','"+PLATFORMS+"','"+PLATFORM_HASH+"','"+TYPE+"','"+str(TIME_STAMP)+"')"
                cursor.execute(sql)
                connection.commit()
        time.sleep(1)
    except:
        pass
cursor.close()
connection.close()

