import rcdTools.DataCollect as cu
import pandas as pd
import time
import datetime
import pymysql as sql

FN = "./data/locationsubs.csv"
TARGET_YEAR = 2016
SOURCE_COMMENTS_CHECKED = 500
AS_COMMENTS_CHECKED    = 500

df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

cu.collectUsersAndActiveSubreddits(df, 
								TARGET_YEAR,
								SOURCE_COMMENTS_CHECKED,
								AS_COMMENTS_CHECKED,
								conn,
								# start_sub='/r/GlacierPark')
								start_sub='/r/astoria')