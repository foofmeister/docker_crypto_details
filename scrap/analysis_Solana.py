import requests
import json
import pandas as pd
import pandas as pd

import numpy as np
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',15)
import matplotlib.pyplot as plt
from functions import create_connection_mysql, create_table_connection_mysql

#####-----------Create Connection
connection = create_connection_mysql()
cursor = connection.cursor()

# Pull Records
df = pd.read_sql_query(
    "SELECT * from db_forest.Twitter_Price_Solana", connection)

type(df.start[1])

df['start_time'] = pd.to_datetime(df['start'],format='%Y-%m-%d')

df = df[df['start_time'] > pd.to_datetime("2022-04-20",format = '%Y-%m-%d')]


def plot(ID_NAME, data_frame):
    df = data_frame
    df_v2 = df[['ID','start','usd','last_updated_at_utc']]
    df_v2 = df_v2[df_v2.ID == ID_NAME]
    df_3 = df.merge(df_v2, left_on=['end','ID'], right_on=['start','ID'])
    df_3['perc_difference_usd'] = df_3['usd_y']/df_3['usd_x']


    df_figure = df_3[['ID','tweet_count','perc_difference_usd','start_x','usd_y']]

    # x axis values
    x = df_figure.tweet_count
    # corresponding y axis values
    y = df_figure.usd_y
    z = df_figure.start_x
    title = df_figure.ID

    #Figure
    fig, ax = plt.subplots()
    # Plot linear sequence, and set tick labels to the same color
    ax.plot(x, color='red')
    ax.tick_params(axis='y', labelcolor='red')
    # Generate a new Axes instance, on the twin-X axes (same position)
    ax2 = ax.twinx()
    # Plot exponential sequence, or set scale to logarithmic and change tick color
    ax2.plot(y, color='green')
    #ax2.set_yscale('log')
    ax2.tick_params(axis='y', labelcolor='green')
    ax.set_title(ID_NAME,
                 fontsize=14)

    plt.show()



ar_unique_ID = df.ID.unique()

for i in ar_unique_ID:
    ID_NAME = i
    plt_s = plot(ID_NAME,df)


f = [0,1,2,3,4]

theSum = 0

for i in f:
    theSum = theSum + i

def solution(numbers):



numbers = [4,3,2,1]

for i in range(len(numbers)):
    print(numbers[i])

def solution(numbers):

x = numbers

for i in range(len(x)):
    y = []
    if i == 0:
        value = min(x[i-1],x[i])
    elif i == len(x):
        value = min(x[i],x[i-1])
    else:
        value = min(x[i],x[i-1],x[i+1])
        y.append(value)


    print(min(x[i+1],x[i-1],x[i]))

a = [1,2,3,4]

x = len(a)


def solution(elements):

elements = [3,2,1,5,4]


x = len(elements)
z = []
a = elements
answer1 = ""
for j in range(x):
    value = x - j
    z.append(value)
for i in range(len(elements)):
    a.insert(0, a.pop(x-1))
    if a == z:
        answer1 = i
        print("yes")
if 'answer1' in locals():
    answer = answer1+1
else:
    answer = "-1"

print(answer)




def solution(elements):


    print(min(x[i+1]))

solution(numbers)

number1 = numbers[1]





# plotting the points
plt.plot(x, z, color='green', linestyle='line', linewidth=3,
         marker='o', markerfacecolor='blue')

# plotting the points
plt.plot(y, z, color='blue', linestyle='line', linewidth=3,
         marker='o', markerfacecolor='blue')





