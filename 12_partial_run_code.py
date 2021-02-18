import rcdTools.DataCollect as dc
import pandas as pd
import pymysql as sql

# Stopping a run on 2021-02-08 16:35
# Week of 2020-05-27, 22nd week
# Subreddit: /r/LouisvilleColorado

# Will restart when the throughput stuff is done running.

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

WEEK_ID   = 17 
START_SUB = '/r/erie'

NC_SOURCE_USERS = 50 # Using less cause its at a week.
NC_SOURCE_ASUBS = 50

# Gets us the first day of each weekly period. 
first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
week_fd   = first_days[WEEK_ID-1]

start_row = df.index[df['subreddit url'] == START_SUB][0] # hacky
df_partial = df.iloc[start_row:,:]


dc.collectUserYWsAndActiveSubreddits(df_partial, 
								week_fd,
								NC_SOURCE_USERS,
								NC_SOURCE_ASUBS,
								conn) 
