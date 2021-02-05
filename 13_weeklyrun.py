import CountyUsers as cu
import pandas as pd
import pymysql as sql

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

START_WEEK = 14 # Started full run on 2021-02-04 20:00

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

NC_SOURCE_USERS = 50 # Using less cause its at a week.
NC_SOURCE_ASUBS = 50

# Gets us the first day of each weekly period. 
first_days = list(pd.date_range('2020-01-01', '2020-12-31', freq='W-WED'))

# subreddit_df, year, week, user_N, active_N, db_conn
for start_date in first_days[START_WEEK-1:]:
	print("---- YEAR {} WEEK {} ----".format(start_date.year, start_date.week))
	cu.collectUserYWsAndActiveSubreddits(df, 
								start_date,
								NC_SOURCE_USERS,
								NC_SOURCE_ASUBS,
								conn) 
