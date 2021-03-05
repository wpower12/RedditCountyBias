import rcdTools.DataCollectWeekly as dc
import pandas as pd
import pymysql as sql

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

YEAR  = 2020
WEEK  = 1
NC_SOURCE_USERS = 3 # Using less cause its at a week.
NC_SOURCE_ASUBS = 3

# subreddit_df, year, week, user_N, active_N, db_conn
dc.collectUserYWsAndActiveSubreddits(df, 
							YEAR, 
							WEEK, 
							NC_SOURCE_USERS,
							NC_SOURCE_ASUBS,
							conn) 
