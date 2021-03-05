import rcdTools.DataCollectWeekly as cu
import pandas as pd
import time
import datetime
import pymysql as sql

FN_NEW = "./data/locationsubs.csv"
FN_OLD = "./data/location_subreddit_counties.csv"

TARGET_YEAR = 2016
SOURCE_COMMENTS_CHECKED = 500
AS_COMMENTS_CHECKED    = 500


df_full = pd.read_csv(FN_NEW)
df_full.drop(labels=['Unnamed: 0'], inplace=True, axis=1)

df_old  = pd.read_csv(FN_OLD)
df_old.drop(labels=['Unnamed: 0', 'county'], inplace=True, axis=1)

# I want a new data frame that has every row in df_full, that is NOT in df_old
# ok wow, best SO answer I've ever found. keep=False means NONE of the dupes are kept.
df_missing = pd.concat([df_old, df_full]).drop_duplicates(keep=False)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

cu.collectUsersAndActiveSubreddits(df_missing, 
								TARGET_YEAR,
								SOURCE_COMMENTS_CHECKED,
								AS_COMMENTS_CHECKED,
								conn)