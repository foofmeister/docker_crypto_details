import pandas as pd
from functions import create_connection_mysql, create_table_connection_mysql

def prices_solana():
    import requests
    import json
    import pandas as pd
    from functions import create_connection_mysql, create_table_connection_mysql

    #####-----------Create Connection
    connection = create_connection_mysql()
    cursor = connection.cursor()

    # Pull Records
    df = pd.read_sql_query(
        "SELECT distinct a.ID from coin_list a \n"
        "left outer join eligible_coins b on upper(a.SYMBOL) = upper(b.COIN) \n"
        "left outer join coin_info_ref c on a.ID = c.ID \n"
        # "inner join TOP_MARKET_CAP tmc on a.ID = tmc.ID \n"
        # "where (b.COIN is not null or PLATFORMS in ('solana')) \n"
        "where PLATFORMS in ('solana') \n"
        "and DO_NOT_USE is null", connection)

    # Creating Iteration so we can run smaller amount through api
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

    ###------Build second data frame, begin infinite loop
    column_names = ['ID', 'usd', 'usd_market_cap', 'usd_24h_vol', 'last_updated_at']
    df2 = pd.DataFrame(columns=column_names)

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
        # time.sleep(13)
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
                df2 = pd.concat([to_append, df2], ignore_index=True)
            except:
                pass
                # print(ID + " failed")

    df2['last_updated_at_utc'] = pd.to_datetime(df2['last_updated_at'], unit='s')
    return df2


def twitter(time_delta):
    from datetime import datetime, timedelta
    import pandas as pd
    import logging
    logging.basicConfig(level=logging.INFO, filename='test.log')
    pd.options.mode.chained_assignment = None  # default='warn'
    from requests.structures import CaseInsensitiveDict
    from functions import create_connection_mysql, create_table_connection_mysql, twitter_bearer_token, \
        twitter_timestamp_to_reg
    import tweepy
    import re

    # method = "initial"
    method = "recurring"
    time_delta = time_delta
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
        "where PLATFORMS in ('solana') \n"
        # "where (b.COIN is not null or PLATFORMS in ('solana')) \n"
        "and DO_NOT_USE is null", connection)

    df['SYMBOL'] = "$" + df['SYMBOL']
    df2 = df.copy(deep=True)
    df2['SYMBOL'] = df2['ID'].str.upper()
    df = df.append(df2, ignore_index=True)
    del df2

    # df_keyword = pd.read_sql_query(
    #    "SELECT distinct HASHTAG from db_forest.HASH_TAG_CAPTURE_REF", connection)

    df_keyword = pd.DataFrame(columns=['HASHTAG'])
    df_keyword = df_keyword.append({'HASHTAG': '#SOL'}, ignore_index=True)
    df_keyword = df_keyword.append({'HASHTAG': '#SOLANA'}, ignore_index=True)
    df_keyword = df_keyword.append({'HASHTAG': 'SOL'}, ignore_index=True)
    df_keyword = df_keyword.append({'HASHTAG': 'SOLANA'}, ignore_index=True)

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
                                          start_time=start_time, end_time=end_time, max_results=100).flatten(
                limit=5000):
                # iteration_number = iteration_number+1
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
    return df_insert_tweet


################ Retrieve tweet counts by month for each crypto
def number_tweets_solana():
    from requests.structures import CaseInsensitiveDict
    import pandas as pd
    import requests
    import tweepy
    from functions import create_connection_mysql, create_table_connection_mysql, twitter_bearer_token, \
        twitter_timestamp_to_reg
    from datetime import datetime
    from datetime import timedelta

    #####-----------Create Connection
    connection = create_connection_mysql()
    cursor = connection.cursor()

    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer " + twitter_bearer_token()
    twitter = tweepy.Client(bearer_token=twitter_bearer_token())

    df = pd.read_sql_query(
        "SELECT distinct a.ID, a.SYMBOL from coin_list a \n"
        "left outer join eligible_coins b on upper(a.SYMBOL) = upper(b.COIN) \n"
        "left outer join coin_info_ref c on a.ID = c.ID \n"
        "inner join  (select distinct ID  from Time_Data_distinct where usd_market_cap > 0) d on a.ID = d.ID "
        #"inner join TOP_MARKET_CAP tmc on a.ID = tmc.ID \n"
        #"where (b.COIN is not null or PLATFORMS in ('solana')) \n"
        "where PLATFORMS in ('solana') \n"
        "and DO_NOT_USE is null", connection)

    start_time_utc = datetime.utcnow() - timedelta(hours=1)
    end_time_utc = datetime.utcnow()

    start_time_df = start_time_utc.strftime("%Y-%m-%d %H:00:00")
    end_time_df = end_time_utc.strftime("%Y-%m-%d %H:00:00")

    start_time = start_time_utc.strftime("%Y-%m-%dT%H:00:00.000Z")
    end_time = end_time_utc.strftime("%Y-%m-%dT%H:00:00.000Z")

    # start_time = (datetime.utcnow() + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00.000Z")
    # end_time = (datetime.utcnow() + timedelta(hours=2)).strftime("%Y-%m-%dT%H:00:00.000Z")


    column_names = ["ID", "start", "end", "tweet_count"]
    df_insert_tweet = pd.DataFrame(columns=column_names)


    for i in range(len(df)):
    #for i in range(2):
        ID = df['ID'].loc[i]
        SYMBOL = df['SYMBOL'].loc[i]
        query = "( " + SYMBOL + " OR " + ID + " OR #"+SYMBOL+" OR #"+ID+" ) ( #solana OR #sol OR solana OR sol ) lang:en -is:retweet"
        counts = twitter.get_recent_tweets_count(query=query, granularity='hour',
                                                 start_time=start_time, end_time=end_time)
        #end = counts[0][0]['end']
        #start = counts[0][0]['start']
        tweet_count = counts[0][0]['tweet_count']
        to_append = {'ID': [ID], 'start': [start_time_df], 'end': [end_time_df], 'tweet_count': [tweet_count]}
        to_append = pd.DataFrame(to_append)
        df_insert_tweet = pd.concat([to_append, df_insert_tweet], ignore_index=True)
    return df_insert_tweet


df_number_tweets_solana = number_tweets_solana()

df_prices_solana = prices_solana()

a = df_number_tweets_solana.merge(df_prices_solana, on ="ID")

conn_create_table = create_table_connection_mysql()
a.to_sql('Twitter_Price_Solana', conn_create_table, if_exists='append', index=False)







