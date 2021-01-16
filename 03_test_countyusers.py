import CountyUsers as cu
import pandas as pd
import praw
import time
import datetime
import pymysql as sql

FN = "./data/location_subreddit_counties.csv"
TARGET_YEAR = 2016
SOURCE_COMMENTS_CHECKED = 500
AS_COMMENTS_CHECKED    = 500

df = pd.read_csv(FN)
df = df[df['county'].notna()]

reddit = praw.Reddit("data_enrich", user_agent="data_project_ua")

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_location_bias')

cu.collectUsersAndActiveSubreddits(df, 
								TARGET_YEAR,
								SOURCE_COMMENTS_CHECKED,
								AS_COMMENTS_CHECKED,
								reddit,
								conn,
								start_sub='/r/texarkana') # Left off Jan 163:45PM
								# start_sub='/r/flagstaff')
