import CountyUsers as cu
import pandas as pd
import pymysql as sql

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

WEEK_ID   = 2 # 0-indexed.
START_SUB = '/r/rochesteril'

NC_SOURCE_USERS = 50 # Using less cause its at a week.
NC_SOURCE_ASUBS = 50

# Gets us the first day of each weekly period. 
first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
week_fd   = first_days[WEEK_ID]

start_row = df.index[df['subreddit url'] == START_SUB][0] # hacky
df_partial = df.iloc[start_row:,:]


cu.collectUserYWsAndActiveSubreddits(df_partial, 
								week_fd,
								NC_SOURCE_USERS,
								NC_SOURCE_ASUBS,
								conn) 
