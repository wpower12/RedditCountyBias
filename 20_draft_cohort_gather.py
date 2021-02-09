import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
import progressbar
import math
import pickle

MAX_RESPONSE_USERS = 500 # Max number of submissions pulled from single cohort query.
NC_SOURCE_ASUBS    = 200 # Number of comments to search for active subs.
USER_COHORT_SIZE   = 5   # Number of users to query at once 


WEEK_IDS = [31, 32]

WEEK_ID   = 30  # So i can drop w.e i add while testing.

COHORTS_SAVE_FN = 'data/cohorts/test_cohorts.p'

first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
cohorts = pickle.load(open(COHORTS_SAVE_FN, "rb"))

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

for WEEK in WEEK_IDS:
	week_fd = first_days[WEEK-1]
	i = 1
	for cohort in cohorts:
		print("- Week {}, cohort {}/{}, {} subreddits".format(WEEK, 
				i, 
				len(cohorts), 
				len(cohort[1])))
		cu.cohortCollect(cohort, 
				week_fd, 
				MAX_RESPONSE_USERS,
				NC_SOURCE_ASUBS, 
				USER_COHORT_SIZE,
				conn)
		i += 1