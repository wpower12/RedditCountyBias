import pandas as pd
import pymysql as sql
import progressbar
import math

import rcdTools.DataCollectDaily as dc

YEAR = 2020
SUBS_Q_PER_ITER = 5
USER_Q_PER_ITER = 5
NUM_SUBS_PER_COHORT  = 30
NUM_USERS_PER_COHORT = 30 

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

running = True
iteration = 0	
while(running):
	for DAY in range(150, 155):
		print("Iteration {}, Day {}".format(iteration, DAY))

		# Subreddit Gather
		u_add_total  = 0
		for q in range(SUBS_Q_PER_ITER):
			try:
				subs    = dc.getCandidateSubreddits(conn, YEAR, DAY, NUM_SUBS_PER_COHORT)
				u_count = dc.subredditCohortGather(conn, subs, YEAR, DAY)
				u_add_total += u_count
				dc.increaseSubredditScrapeCount(conn, subs)

			except KeyboardInterrupt as kbi:
				running = False
				break
			except Exception as e:
				print(e)
		print("- {} users attempt-added".format(u_add_total))
		
		# User Gather
		as_add_total = 0
		for q in range(USER_Q_PER_ITER):
			try:
				users    = dc.getCandidateUserYDs(conn, YEAR, DAY, NUM_USERS_PER_COHORT)
				as_count = dc.userydASCohortGather(conn, users, YEAR, DAY)
				as_add_total += as_count
				dc.increaseUserydScrapeCount(conn, users)

			except KeyboardInterrupt as kbi:
				running = False
				break
			except Exception as e:
				print(e)
		print("- {} AS links attempt-added".format(as_add_total))

	iteration += 1


