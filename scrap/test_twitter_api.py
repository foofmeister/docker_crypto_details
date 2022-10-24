#https://api.twitter.com/1.1/search/tweets.json?q=nasa&result_type=popular
import requests
from datetime import datetime
import pandas as pd
import time
import json
from requests.structures import CaseInsensitiveDict
from functions import create_connection_mysql, create_table_connection_mysql

headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer AAAAAAAAAAAAAAAAAAAAABbeWAEAAAAANqPBKtHkYkgit1opPUASciPEJAA%3DJzHigWbVNWp2tEPgPmAAu8RG5YSToaayVE2Uvq62hnwEUGUx6H"

SYMBOL = "BTC"

url = "https://api.twitter.com/1.1/search/tweets.json?q=(%24"+SYMBOL+"%20)%20(move%20OR%20moving%20OR%20not%20OR%20up%20OR%20down%20OR%20alt%20OR%20fed%20OR%20regulation%20OR%20bad%20OR%20good%20OR%20great%20OR%20hodl%20OR%20buy%20OR%20accumulate)%20-giveaway%20-bonus%20-sign%20lang%3Aen%20-give%20-is%3Aretweet&start_time=2022-01-09T17:50:00.001Z&end_time=2022-01-09T18:50:00.000Z&max_results=100&tweet_mode=extended"

url = "https://api.twitter.com/1.1/search/tweets.json?q=(%24"+SYMBOL+"%20)%20until%3A2022-01-09T18:50:00.000Z%20since%3A2022-01-09T17:50:00.001Z%20lang%3Aen"

url = "https://api.twitter.com/1.1/search/tweets.json?q=%24NCT%20-is%20lang%3Aen%20%3Aretweet&tweet_mode=extended"

#url = "https://api.twitter.com/1.1/tweets/counts/recent?query=%24"+SYMBOL+"&granularity=hour"#&start_time="+START_DATE+"T00%3A00%3A00Z"

resp = requests.get(url, headers=headers)
content = resp.content
y = json.loads(content)
y1 = y['statuses']

query = "($" + SYMBOL + " ) (move OR moving OR not OR up OR down OR alt OR fed OR regulation OR bad OR good OR great OR hodl OR buy OR accumulate) -giveaway -bonus -sign lang:en -give -is:retweet"
# query = "($"+SYMBOL+" ) -giveaway -bonus -sign lang:en -give -is:retweet -filter:replies"


start_time = "2022-01-09T17:50:00.001Z"
end_time = "2022-01-09T18:50:00.000Z"

SYMBOL = 'CRYPTO'

query = "(#" + SYMBOL + " ) (move OR moving OR not OR up OR down OR alt OR fed OR regulation OR bad OR good OR great OR hodl OR buy OR accumulate) -giveaway -bonus -sign lang:en -give -is:retweet"

tweets = twitter.search_recent_tweets(query=query, tweet_fields=['context_annotations', 'created_at', 'author_id'],
                                      start_time=start_time, end_time=end_time,
                                      max_results=100)  # , next_token="b26v89c19zqg8o3fpe17gd7w7d5mxq5d190dmiykoj725")


df = pd.read_sql_query(
    "SELECT distinct a.ID, ifnull(c.SYMBOL_USE,a.SYMBOL) SYMBOL from coin_list a \n"
    "left outer join eligible_coins b on upper(a.SYMBOL) = upper(b.COIN) \n"
    "left outer join coin_info_ref c on a.ID = c.ID \n"
    "inner join TOP_MARKET_CAP tmc on a.ID = tmc.ID \n"
    "where (b.COIN is not null or PLATFORMS = 'ethereum') \n"
    "and DO_NOT_USE is null", connection)


import re

for tweet in tweets.data:
    tweet_text = tweet.text
    tweet_text_cashtag = tweet_text.find("$")
    if tweet_text_cashtag == -1:
        pass
    else:
        cash_tags = re.findall(r'\$\w+', tweet_text)
        print(cash_tags)

#https://twitter.com/search?q=(%24BTC%20)%20(move%20OR%20moving%20OR%20not%20OR%20up%20OR%20down%20OR%20alt%20OR%20fed%20OR%20regulation%20OR%20bad%20OR%20good%20OR%20great%20OR%20hodl%20OR%20buy%20OR%20accumulate)%20-giveaway%20-bonus%20-sign%20lang%3Aen%20-give%20-is%3Aretweet&src=typed_query&f=top



from functions import create_connection_mysql, create_table_connection_mysql, twitter_bearer_token, twitter_timestamp_to_reg
import tweepy


twitter = tweepy.Client(bearer_token=twitter_bearer_token())


query = "$NCT -is:retweet"

counts = twitter.get_recent_tweets_count(query=query, granularity='day')


#from functions import create_connection_mysql, create_table_connection_mysql
import pandas as pd
#####-----------Create Connection

connection = create_connection_mysql()

import pandas as pd

def create_connection_mysql():
    import mysql.connector
    mydb = None
    try:
        mydb = mysql.connector.connect(
            host='34.94.143.25',
            user='chad_h',
            password='Lumen123!Chad',
            port='3306',
            database='db_forest',
            autocommit=False
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return mydb

connection = create_connection_mysql()

df = pd.read_sql_query(
   """
   Select *
   from Twitter_Individual_Raw
   where ID = 'pluton'
   and created_at like '%2022-01-21%'
   limit 100000""", connection)

connection = create_connection_mysql()
cursor = connection.cursor()
c = connection.cursor()
c.execute(
    'CREATE TABLE IF NOT EXISTS db_forest.Twitter_Individual_Raw (ID varchar (100), tweet_ID varchar (100) ,tweet_text text,author_id varchar(100),created_at varchar(100), start_time varchar(100), end_time varchar(100), query text )')
connection.commit()

from functions import create_connection_mysql, create_table_connection_mysql

connection = create_connection_mysql()
cursor = connection.cursor()
c = connection.cursor()

query = """INSERT db_forest.Twitter_Individual_Raw_pluton 
     select ID from
     db_forest.Twitter_Individual_Raw limit 10 """
c.execute(query)
connection.commit()
