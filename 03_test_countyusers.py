import rcdTools.DataCollectWeekly as cu
import pandas as pd
import time
import datetime
import pymysql as sql

FN = "./data/location_subreddit_counties.csv"
TARGET_YEAR = 2016
SOURCE_COMMENTS_CHECKED = 500
AS_COMMENTS_CHECKED    = 500

df = pd.read_csv(FN)
df = df[df['county'].notna()]

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

cu.collectUsersAndActiveSubreddits(df, 
								TARGET_YEAR,
								SOURCE_COMMENTS_CHECKED,
								AS_COMMENTS_CHECKED,
								conn,
								start_sub='/r/alpharetta')
								# start_sub='/r/dothan')
								# start_sub='/r/texarkana') # Left off Jan 163:45PM
								# start_sub='/r/flagstaff')
