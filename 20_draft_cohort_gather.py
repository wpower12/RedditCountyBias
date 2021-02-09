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
NC_SOURCE_ASUBS    = 50  # Number of comments to search for active subs.
USER_COHORT_SIZE   = 10  # Number of users to query at once 
WEEK_ID   = 30  # So i can drop w.e i add while testing.

COHORTS_SAVE_FN = 'data/cohorts/test_cohorts.p'

first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
week_fd    = first_days[WEEK_ID-1]
cohorts = pickle.load(open(COHORTS_SAVE_FN, "rb"))

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

for cohort in cohorts[:1]:

	cu.cohortCollect(cohort, 
				week_fd, 
				MAX_RESPONSE_USERS,
				NC_SOURCE_ASUBS, 
				USER_COHORT_SIZE,
				conn)