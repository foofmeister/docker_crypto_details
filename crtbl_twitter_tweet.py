import requests
from datetime import datetime, timedelta
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, filename='test.log')

pd.options.mode.chained_assignment = None  # default='warn'
import time
import json
from requests.structures import CaseInsensitiveDict
from functions import create_connection_mysql, create_table_connection_mysql, twitter_bearer_token, \
    twitter_timestamp_to_reg
import tweepy
import re

# method = "initial"
method = "recurring"
time_delta = 60
# Fix Time Delta

#####-----------Create Connection
connection = create_connection_mysql()
cursor = connection.cursor()

#####----------Twitter Headers
headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer " + twitter_bearer_token()
twitter = tweepy.Client(bearer_token=twitter_bearer_token())

df = pd.read_sql_query(
    "SELECT distinct a.ID, ifnull(c.SYMBOL_USE,a.SYMBOL) SYMBOL from coin_list a \n"
    "left outer join eligible_coins b on upper(a.SYMBOL) = upper(b.COIN) \n"
    "left outer join coin_info_ref c on a.ID = c.ID \n"
    # "inner join TOP_MARKET_CAP tmc on a.ID = tmc.ID \n"
    "where (b.COIN is not null or PLATFORMS = 'ethereum') \n"
    "and DO_NOT_USE is null", connection)

df['SYMBOL'] = "$" + df['SYMBOL']
df2 = df.copy(deep=True)
df2['SYMBOL'] = df2['ID'].str.upper()
df = df.append(df2, ignore_index=True)
del df2
df_keyword = pd.read_sql_query(
    "SELECT distinct HASHTAG from db_forest.HASH_TAG_CAPTURE_REF", connection)

if method == "recurring":

    now = datetime.utcnow() + timedelta(minutes=-1)
    now = now.replace(second=0)
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    now = now.replace(" ", "T")
    now = now + ".000Z"
    end_time = now

    now = datetime.utcnow() + timedelta(minutes=(-1 - time_delta))
    now = now.replace(second=0)
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    now = now.replace(" ", "T")
    now = now + ".001Z"
    start_time = now

elif method == "initial":
    start_time = "2022-01-12T16:50:00.001Z"
    start_time = "2022-01-28T14:50:00.001Z"
    end_time = "2022-01-28T18:50:00.000Z"

# Create Table if not exists
# try:
# delete_df = pd.read_sql_query("select * from db_forest.Twitter_Individual_Raw where limit 5")
# del delete_df
# except:
# connection = create_connection_mysql()
# cursor = connection.cursor()
# c = connection.cursor()
# c.execute('CREATE TABLE IF NOT EXISTS db_forest.Twitter_Individual_Raw (ID varchar (100), tweet_ID varchar (100) ,tweet_text text,author_id varchar(100),created_at varchar(100), start_time varchar(100), end_time varchar(100), query text )')
# connection.commit()
# c.execute('ALTER TABLE db_forest.Twitter_Individual_Raw CHANGE tweet_text tweet_text text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;')
# connection.commit()
# connection.close()

column_names = ["ID", "tweet_ID", "tweet_text", "author_id", "created_at"]
HASHTAG_USED = []
df_tweet = pd.DataFrame(columns=column_names)

tweet_data_df = pd.DataFrame(columns=["tweet_ID", "tweet_text", "author_id", "created_at"])

for i in range(len(df_keyword)):
    conn_create_table = create_table_connection_mysql()
    HASHTAG = df_keyword.HASHTAG[i]
    HASHTAG = HASHTAG.replace("$", "")
    HASHTAG = HASHTAG.replace("&", "")
    HASHTAG = HASHTAG.replace("#", "")
    # print(i)
    # time.sleep(1.5)
    # ID = df.ID[i]
    # query = "(#"+HASHTAG+" ) (move OR moving OR not OR up OR down OR alt OR fed OR regulation OR bad OR good OR great OR hodl OR buy OR accumulate) -giveaway lang:en -is:retweet"
    query = "(#" + HASHTAG + " ) -giveaway -give -giving lang:en -is:retweet"
    # query = "($"+SYMBOL+" ) -giveaway -bonus -sign lang:en -give -is:retweet -filter:replies"
    # tweets = twitter.search_recent_tweets(query=query, tweet_fields=['context_annotations', 'created_at', 'author_id'], start_time=start_time, end_time=end_time, max_results=100)#, next_token="b26v89c19zqg8o3fpe17gd7w7d5mxq5d190dmiykoj725")
    try:
        for tweet in tweepy.Paginator(twitter.search_recent_tweets, query=query,
                                      tweet_fields=['context_annotations', 'created_at', 'author_id'],
                                      start_time=start_time, end_time=end_time, max_results=100).flatten(limit=5000):
            #iteration_number = iteration_number+1
            tweet_ID = tweet.id
            tweet_text = tweet.text
            created_at = tweet.created_at
            author_id = tweet.author_id
            to_append_tweet_df = {'tweet_ID': [tweet_ID], 'tweet_text': [tweet_text], 'author_id': [author_id],
                                  'created_at': [created_at]}
            to_append_tweet_df = pd.DataFrame(to_append_tweet_df)
            tweet_data_df = pd.concat([tweet_data_df, to_append_tweet_df], ignore_index=True)
    except Exception as E:
        print(E)

del tweet_ID
del tweet_text
del created_at
del author_id

tweet_data_df2 = tweet_data_df.copy(deep=True)

tweet_data_df = pd.DataFrame.drop_duplicates(tweet_data_df)
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.replace('\n', ' ')
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.encode("ascii", "ignore")
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.decode("utf-8")
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.upper()
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.replace(",", "")
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.replace(".", "")
tweet_data_df['tweet_text'] = tweet_data_df['tweet_text'].str.replace("?", "")
tweet_data_df = tweet_data_df.reset_index()
del tweet_data_df['index']

df_insert_tweet = pd.DataFrame(columns=['ID', 'tweet_ID', 'tweet_text', 'author_id', 'created_at'])

for j in range(len(tweet_data_df)):
    # print(str(j) + "for 2")
    tweet_text = tweet_data_df.tweet_text[j]
    tweet_text_words = re.split(r'\s', str(tweet_text))
    # tweet_text_words = re.findall(r'\$\w+', str(tweet_text1))
    # tweet_text_words =  [each_string.upper() for each_string in tweet_text_words]
    # tweet_text_words = list(map(lambda st: str.replace(st, "$", ""), tweet_text_words))
    tweet_text_words = pd.DataFrame(tweet_text_words, columns=['SYMBOL'])
    tweet_text_words = tweet_text_words.merge(df, on="SYMBOL", how="inner")
    tweet_text_words = tweet_text_words['ID'].unique()
    if len(tweet_text_words) == 0:
        pass
    else:
        tweet_ID = tweet_data_df.tweet_ID[j]
        author_id = tweet_data_df.author_id[j]
        created_at = tweet_data_df.created_at[j]
        for k in range(len(tweet_text_words)):
            ID = tweet_text_words[k]
            to_append = {'ID': [ID], 'tweet_ID': [tweet_ID], 'tweet_text': [tweet_text], 'author_id': [author_id],
                         'created_at': [created_at]}
            to_append = pd.DataFrame(to_append)
            df_insert_tweet = pd.concat([to_append, df_insert_tweet], ignore_index=True)
conn_create_table = create_table_connection_mysql()
df_insert_tweet.to_sql('twitter_tweet', conn_create_table, if_exists='append', index=False)
