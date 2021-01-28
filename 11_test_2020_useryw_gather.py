import CountyUsers as cu
import pandas as pd
import pymysql as sql

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

YEAR  = 2020
WEEKS = range(1, 13)

NC_SOURCE_USERS = 200 # Using less cause its at a week.
NC_SOURCE_ASUBS = 200

# subreddit_df, year, week, user_N, active_N, db_conn
for WEEK in WEEKS:
	print("---- YEAR {} WEEK {} ----".format(YEAR, WEEK))
	cu.collectUserYWsAndActiveSubreddits(df, 
								YEAR, 
								WEEK, 
								NC_SOURCE_USERS,
								NC_SOURCE_ASUBS,
								conn) 
