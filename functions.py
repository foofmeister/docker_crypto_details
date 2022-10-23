import sqlite3
from sqlite3 import Error
import os


def create_connection_mysql():
    import mysql.connector
    mydb = None
    try:
        mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='password',
            port='3306',
            database='DB',
            autocommit=False
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return mydb

def create_table_connection_mysql():
    from sqlalchemy import create_engine
    my_conn = create_engine("mysql+pymysql://root:password@localhost:3306/DB")
    my_conn = create_engine("mysql+pymysql://root:password@localhost/DB")
    return my_conn


def twitter_timestamp_to_reg(x):
    y = x.replace("T"," ")
    y = y[0:(len(y)-5)]
    return y


def extract_hashtags(text):
    text = text.replace('#'," #")
    text = text.replace('$', " $")
    text = text.upper()
    hashtag_list = []
    for word in text.split():
        if word[0] == '#':
            hashtag_list.append(word[1:])
        if word[0] == '$':
            hashtag_list.append(word[1:])
    #print("The hashtags in \"" + text + "\" are :")
    return hashtag_list

