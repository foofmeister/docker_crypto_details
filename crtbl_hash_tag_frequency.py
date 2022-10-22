from datetime import datetime
import pandas as pd
import time
from functions import create_connection_mysql, create_table_connection_mysql, extract_hashtags
from collections import OrderedDict

connection = create_connection_mysql()
cursor = connection.cursor()
conn_create_table = create_table_connection_mysql()

#########-----Get Twitter Individual Raw Query
df = pd.read_sql_query(
    "select * from db_forest.Twitter_Individual_Raw", connection)
df1 = df[['tweet_ID','tweet_text']]
df1 = df1.drop_duplicates().reset_index()

hash_tag_final_list = []

for i in range(len(df1)):
    tweet_text = df1.tweet_text[i]
    hash_tags = extract_hashtags(tweet_text)
    hash_tag_final_list = hash_tag_final_list + hash_tags




res = [(el, hash_tag_final_list.count(el)) for el in hash_tag_final_list]
hash_tag_frequency_list = list(OrderedDict(res).items())

#############
c = connection.cursor()
c.execute('create table IF NOT EXISTS db_forest.hash_tag_frequency (HashTag varchar (100), Frequency numeric)')
connection.commit()


df2 = pd.DataFrame(hash_tag_frequency_list, columns=['HashTag', 'Frequency'])

df3 = df2[df2['HashTag'].map(lambda x: x.isascii())]

df3.to_sql('hash_tag_frequency', conn_create_table, if_exists='replace', index=False)



abc = hash_tag_frequency_list.sort(key=lambda x:x[1])







