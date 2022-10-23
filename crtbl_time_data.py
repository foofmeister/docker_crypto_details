import time
import requests
import json
import pandas as pd
from functions import create_connection_mysql, create_table_connection_mysql

#####-----------Create Connection
connection = create_connection_mysql()
cursor = connection.cursor()

with open("time_data.sql","r") as file:
    time_data_sql = file.read()

#Pull Records
df = pd.read_sql_query(
    time_data_sql, connection)

#Creating Iteration so we can run smaller amount through api
df['ITERATION'] = ""
COUNTER = 0
COUNTER2 = 0
for index, row in df.iterrows():
    if COUNTER != 150:
        row['ITERATION'] = COUNTER2
        COUNTER += 1
    else:
        COUNTER2 += 1
        row['ITERATION'] = COUNTER2
        COUNTER = 0


###------Creating Table to house coin data
column_names = ['ID', 'usd', 'usd_market_cap', 'usd_24h_vol', 'last_updated_at']
c = connection.cursor()
c.execute('CREATE TABLE IF NOT EXISTS DB.Time_Data (ID varchar (100) ,usd numeric,usd_market_cap numeric,usd_24h_vol numeric ,last_updated_at numeric)')
connection.commit()
connection.close()


###------Build second data frame, begin infinite loop
df2 = pd.DataFrame(columns=column_names)
integer = 0

while integer < 2:
    print(integer)
    integer = integer + 1
    for j in range(max(df['ITERATION'])):
        df_iteration = df[df['ITERATION'] == j]
        df_iteration = df_iteration.reset_index()
        query_string = ""
        for i in range(len(df_iteration.index)):
            a = df_iteration.iloc[i]['ID']
            if len(query_string) == 0:
                query_string = a
            else:
                query_string = query_string + "%2C" + a
        url = "https://api.coingecko.com/api/v3/simple/price?ids=" + query_string + "&vs_currencies=USD&include_market_cap=true&include_24hr_vol=true&include_24hr_change=false&include_last_updated_at=true"
        CoinPrice = requests.get(url)
        CoinPriceContent = CoinPrice.content
        y = json.loads(CoinPriceContent)
        time.sleep(13)
        for key in y:
            ID = key
            try:
                usd = y[key]['usd']
                usd_market_cap = y[key]['usd_market_cap']
                usd_24h_vol = y[key]['usd_24h_vol']
                last_updated_at = y[key]['last_updated_at']
                to_append = {'ID': [ID], 'usd': [usd], 'usd_market_cap': [usd_market_cap], 'usd_24h_vol': [usd_24h_vol],
                        'last_updated_at': [last_updated_at]}
                to_append = pd.DataFrame(to_append)
                df2 = pd.concat([to_append,df2],ignore_index=True)
            except:
                pass
                #print(ID + " failed")
    try:
        conn_create_table = create_table_connection_mysql()
        df2.to_sql('Time_Data', conn_create_table, if_exists='append', index=False)
    except:
        time.sleep(10)
        conn_create_table = create_table_connection_mysql()
        df2.to_sql('Time_Data', conn_create_table, if_exists='append', index=False)
    #print("finished")
    #time.sleep(30)
