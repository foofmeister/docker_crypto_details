
import requests
import json
import pandas as pd
from functions import create_table_connection_mysql


#####-----------Create Connection
#connection = create_connection_mysql()
#cursor = connection.cursor()


#####-----------Create Connection for importing tables
conn_create_table = create_table_connection_mysql()


####------------get csv and push
tradable_df = pd.read_csv("tradable_coins.csv")
tradable_df = tradable_df[['COIN','Platform']]
tradable_df.to_sql(con=conn_create_table, name="eligible_coins", if_exists="replace", index=False)


#####-----------Create empty data frame
column_names = ['ID','SYMBOL','NAME','PLATFORMS','PLATFORM_HASH']
df = pd.DataFrame(columns = column_names)
print(df)


#####-----------get list of coins
url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
coin_list_response = requests.get(url)
coin_list_response_content = coin_list_response.content
y = json.loads(coin_list_response_content)
#y = coin_list_response_content
#i = 10
for i in range(len(y)):
    ID = y[i]['id']
    SYMBOL = y[i]['symbol']
    NAME = y[i]['name']
    a = y[i]['platforms']
    try:
        key, value = list(a.items())[0]
        PLATFORMS = key
        PLATFORM_HASH = value
    except:
        PLATFORMS = ""
        PLATFORMS_HASH = ""
    list_concat = [ID,SYMBOL,NAME,PLATFORMS,PLATFORMS_HASH]
    df_length = len(df)
    df.loc[df_length] = list_concat


#Change SYMBOL to UPPER for matching
df['SYMBOL'] = df['SYMBOL'].str.upper()
df['SYMBOL'] = df['SYMBOL'].str.encode('ascii', 'ignore').str.decode('ascii')

####-----------Create table
df.to_sql('coin_list', conn_create_table, if_exists='replace', index=False)

##### coin_info_ref


"""
create table db_forest.TOP_MARKET_CAP (ID varchar(100))

	insert into db_forest.TOP_MARKET_CAP 
    select ID from 
    (
    select a.ID, a.usd_market_cap from db_forest.Time_Data_distinct a
    inner join 
		(
		select ID, max(last_updated_at) last_updated_at from db_forest.Time_Data_distinct
		group by ID)
        b on (a.ID = b.ID and a.last_updated_at = b.last_updated_at)
    left outer join 
		db_forest.coin_info_ref 
		c on a.ID = c.ID 
	where DO_NOT_USE is null
    ) a 
    order by usd_market_cap desc
    limit 1500
"""
