import pandas as pd
import os
from twilio.rest import Client
from datetime import timedelta, date

from functions import  twilio_account_sid, twilio_auth_token, create_connection_mysql, create_table_connection_mysql

percent_added = .1
account_sid = twilio_account_sid()
auth_token = twilio_auth_token()
connection = create_connection_mysql()

#df = pd.read_sql_query("select a.ID, a.last_updated_at, a.usd, a.usd_24h_vol, a.usd_market_cap, b.SYMBOL, b.PLATFORMS from Time_Data a inner join coin_list b on a.ID = b.ID limit 10000", connection)
df = pd.read_sql_query("""select a.ID, a.last_updated_at, a.usd, a.usd_24h_vol, a.usd_market_cap, b.SYMBOL, b.PLATFORMS from Time_Data a inner join coin_list b on a.ID = b.ID
                        union select a.ID, a.last_updated_at, a.usd, a.usd_24h_vol, a.usd_market_cap, b.SYMBOL, b.PLATFORMS from Time_Data_distinct a inner join coin_list b on a.ID = b.ID""", connection)


#convert to california time
df['last_updated_at'] = pd.to_datetime(df['last_updated_at'],unit='s')
df['last_updated_at'] = pd.to_datetime(df['last_updated_at']) - timedelta(hours=8)

df = df.drop_duplicates()

max_date = df.last_updated_at.max()
last_24_hours = max_date - timedelta(hours=24)
df2 = df[df['last_updated_at'] >= last_24_hours]
#df2 = df2[df2.ID == 'barnbridge']

#Get Median
median_df = df2.groupby(["ID","SYMBOL","PLATFORMS"])['usd'].median()
median_df = median_df.rename("median_usd")
median_df = median_df.to_frame()

#Get maximum usd value for date
idx = df2.groupby(["ID","SYMBOL","PLATFORMS"], sort=False)['last_updated_at'].transform(max) == df2['last_updated_at']
max_date_df = df2[idx]
max_date_df = max_date_df[["ID","SYMBOL","PLATFORMS","last_updated_at","usd"]]
max_date_df.columns = ["ID","SYMBOL","PLATFORMS","last_updated_at","max_date_usd"]


#Join Median and maximum
analysis_1_df = pd.merge(median_df,max_date_df, how = "inner", on="ID")
#pd.concat([median_df,max_date_df], axis = 1)
#analysis_1_df = median_df.to_frame().join(max_date_df)


#Get percent difference
analysis_1_df['Per_Dif'] = analysis_1_df['max_date_usd']/analysis_1_df['median_usd']
#percent_added = .1 #Located at header
median = analysis_1_df['Per_Dif'].median()
median_plus_percent = median + percent_added

#Reset index
#analysis_1_df = analysis_1_df.reset_index()

###------Creating Table to house coin data
column_names = ['Date_Time_Message_Sent', 'ID', 'Percent']
c = connection.cursor()
c.execute('CREATE TABLE IF NOT EXISTS Message_Sent_Meta (' + str(','.join(column_names)) + ')')
connection.commit()

analysis_1_df = analysis_1_df[analysis_1_df['Per_Dif'] >= median_plus_percent]
analysis_1_df = analysis_1_df.reset_index()
del analysis_1_df['index']


for i in range(len(analysis_1_df)):
    Per_Dif = analysis_1_df.iloc[i]['Per_Dif']
    Per_Dif = Per_Dif.round(3)
    ID = analysis_1_df.iloc[i]['ID']
    Date_Last_Pull = analysis_1_df.iloc[i]['last_updated_at']
    Date = date.today()
    Date = Date.strftime("%b-%d-%Y")
    query_string = "SELECT * from Message_Sent_Meta where ID = '"+ID+"' and Date_Time_Message_Sent = '"+Date+"'"
    check_df = pd.read_sql_query(query_string, connection)
    if len(check_df) == 0:
        SYMBOL = analysis_1_df.iloc[i]['SYMBOL']
        message_string = "Message from Forest\n" \
                         "ID = "+ID+"\n" \
                         "SYMBOL = "+SYMBOL+"\n" \
                         "Percent Difference is +"+str(Per_Dif)+"%\n" \
                         "24 hour market data trend: "+str(median.round(3))+"\n" \
                         "Last Date Pulled: "+str(Date_Last_Pull)
        client = Client(account_sid, auth_token)
        client.messages.create(
                # to="+17204701682",
                to="+17204701682",
                from_="+17272921938",
                body=message_string
        )
        #client.messages.create(
        #        # to="+17204701682",
        #        to="+13105690358",
        #        from_="+17272921938",
        #        body=message_string
        #)
        #client.messages.create(
        #        # to="+17204701682",
        #        to="+18109239678",
        #        from_="+17272921938",
        #        body=message_string
        #)
        to_append = {'Date_Time_Message_Sent': [Date], 'ID': [ID], 'Percent': [Per_Dif]}
        import_df = pd.DataFrame(to_append)
        import_df.to_sql('Message_Sent_Meta', connection, if_exists='append', index=False)
connection.close()