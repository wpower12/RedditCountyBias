import rcdTools.DataCollect as dc
import pandas as pd
import time
import datetime
import pymysql as sql
import progressbar
import pickle

MAX_RESPONSE_USERS = 500 # Max number of submissions pulled from single cohort query.
NC_SOURCE_ASUBS    = 500 # Number of comments to search for active subs.
USER_COHORT_SIZE   = 20  # Number of users to query at once 

DAYS = range(0, 20)

# TODO - UPDATE THE COHORTS. 
COHORTS_SAVE_FN = 'data/cohorts/cohorts_comment_tp_daily.p'

days  = pd.date_range('2020-01-01', '2020-12-31', freq='D')

cohorts = pickle.load(open(COHORTS_SAVE_FN, "rb"))

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

for DAY in DAYS:
	day_dt = days[DAY-1]

	i = 0
	for cohort in cohorts:
		i += 1
		print("{}, cohort {}/{}, {} subreddits".format(day_dt, 
				i, 
				len(cohorts), 
				len(cohort[1])))

		dc.cohortCollectYD(cohort, 
				day_dt, 
				MAX_RESPONSE_USERS,
				NC_SOURCE_ASUBS, 
				USER_COHORT_SIZE,
				conn)
