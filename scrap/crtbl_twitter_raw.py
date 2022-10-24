import requests
from datetime import datetime
import pandas as pd
import time
import json
from requests.structures import CaseInsensitiveDict
from functions import create_connection_mysql, create_table_connection_mysql

headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer AAAAAAAAAAAAAAAAAAAAABbeWAEAAAAANqPBKtHkYkgit1opPUASciPEJAA%3DJzHigWbVNWp2tEPgPmAAu8RG5YSToaayVE2Uvq62hnwEUGUx6H"

connection = create_connection_mysql()
cursor = connection.cursor()
conn_create_table = create_table_connection_mysql()


#------------------Read eligible coins data
df = pd.read_sql_query(
    "SELECT distinct a.ID, ifnull(c.SYMBOL_USE,a.SYMBOL) SYMBOL from coin_list a \n"
    "left outer join eligible_coins b on upper(a.SYMBOL) = upper(b.COIN) \n"
    "left outer join coin_info_ref c on a.ID = c.ID \n"
    "where (b.COIN is not null or PLATFORMS = 'ethereum') \n"
    "and DO_NOT_USE is null", connection)

#df = df[(df['ID'] == 'arpa-chain')|(df['ID'] == 'ankr')]
df = df.reset_index()
del df['index']

#---------------Create table if not exists
c = connection.cursor()
c.execute('CREATE TABLE IF NOT EXISTS db_forest.Twitter_Raw (ID varchar (100) ,SYMBOL varchar (100),start varchar (100),end varchar(100), tweet_count numeric)')
connection.commit()

column_names = ['ID', 'SYMBOL', 'start', 'end', 'tweet_count']
tweet_df = pd.DataFrame(columns=column_names)

START_DATE = '2021-12-22'

y = ""

for j in range(4767):
#for j in range(len(df)):
#for j in 300:len(df):
#for j in range(1):
    if j <= 4765:
        pass
    else:
        #j = 1
        #print(j)
        ID = df['ID'].loc[j]
        SYMBOL = df['SYMBOL'].loc[j]
        SYMBOL = SYMBOL.replace('$', '')
        url = "https://api.twitter.com/2/tweets/counts/recent?query=%24"+SYMBOL+"&granularity=hour&start_time="+START_DATE+"T00%3A00%3A00Z"
        resp = requests.get(url, headers=headers)
        content = resp.content
        del y
        y = json.loads(content)
        try:
            y = y['errors']
            apierrormessage = y[0]['message']
            print("error")
        except KeyError:
            y = y['data']
            apierrormessage = "no error"
            #print(j)
            #time.sleep(5)
            for i in range(len(y)):
                start = y[i]['start']
                end = y[i]['end']
                tweet_count = y[i]['tweet_count']
                to_append = {'ID': ID, 'SYMBOL': SYMBOL, 'start': start, 'end': end,'tweet_count': tweet_count}
                tweet_df = tweet_df.append(to_append,ignore_index=True)
        print(apierrormessage)
        if apierrormessage == "There were errors processing your request: Rules must contain at least one positive, non-stopword clause (at position 1)" or apierrormessage == "Invalid 'query':''. 'query' must be a non-empty string":
            print(str(j)+" didn't work, moving to next ")
        elif apierrormessage == "no error":
            print(str(j)+" no error")
        else:
            break
        time.sleep(3)
tweet_df['start'] = tweet_df['start'].str.rstrip('Z').apply(datetime.fromisoformat)
tweet_df['end'] = tweet_df['end'].str.rstrip('Z').apply(datetime.fromisoformat)
tweet_df.to_sql('Twitter_Raw', conn_create_table, if_exists='append', index=False)
connection.close()

"https://api.twitter.com/2/tweets/counts/recent?query=AT&granularity=hour&start_time=2021-12-05T00%3A00%3A00Z"



