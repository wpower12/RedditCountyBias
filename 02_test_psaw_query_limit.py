import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql

NUM_RESULTS = 513
TARGET_YEAR = 2016
SUB_NAME    = 'politics'

START_TS = int(time.mktime(datetime.date(TARGET_YEAR, 1,   1).timetuple()))
END_TS   = int(time.mktime(datetime.date(TARGET_YEAR, 12, 30).timetuple()))

reddit = praw.Reddit("data_enrich", user_agent="data_project_ua")
psapi  = PushshiftAPI(reddit)

cache = []
result_obj = psapi.search_comments(after=START_TS,
                     				before=END_TS,
                             		subreddit=SUB_NAME)

for c in result_obj:
	cache.append(c)

	if len(cache) >= NUM_RESULTS: break

print(len(cache))