import CountyUsers as cu
import pandas as pd
import time
import datetime
import pymysql as sql
import progressbar
import pickle

MAX_RESPONSE_USERS = 500 # Max number of submissions pulled from single cohort query.
NC_SOURCE_ASUBS    = 500 # Number of comments to search for active subs.
USER_COHORT_SIZE   = 5   # Number of users to query at once 

START_COHORT = 168 # Only applies to the first week in the list. 

WEEK_IDS = [43, 44, 45, 46] # Newest version has run on 41, 42

WEEK_ID   = 30  # So i can drop w.e i add while testing.

COHORTS_SAVE_FN = 'data/cohorts/cohorts_comment_tp.p'

first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
cohorts = pickle.load(open(COHORTS_SAVE_FN, "rb"))

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

skipping = True

for WEEK in WEEK_IDS:
	week_fd = first_days[WEEK-1]
	i = 0
	for cohort in cohorts:
		i += 1
		if( skipping and (i != START_COHORT )): 
			continue
		else:
			skipping = False
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
