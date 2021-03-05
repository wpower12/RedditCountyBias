import pandas as pd
import pymysql as sql
import progressbar
import math

import rcdTools.DataCollectDaily as dc

YEAR = 2020
NUM_SUBS_PER_COHORT  = 20
NUM_USERS_PER_COHORT = 20 

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

running = True
while(running):
	iteration = 0
	for DAY in range(1, 365):
		try:
			print("Iteration {}, Day {}".format(iteration, DAY))
			# Subreddit Gather
			subs    = dc.getCandidateSubreddits(conn, DAY, YEAR, NUM_SUBS_PER_COHORT)
			u_count = dc.subredditCohortGather(conn, subs, DAY, YEAR) 
			print("- {} users attempt-added".format(u_count))

			# User Gather
			users    = dc.getCandidateUserYDs(conn, DAY, YEAR, NUM_USERS_PER_COHORT)
			as_count = dc.userydASCohortGather(conn, users, DAY, YEAR) 
			print("- {} AS links attempt-added".format(as_count))
	
		# Totally clean code, right?
		except KeyboardInterrupt as kbi:
			pass

		except Exception as e:
			print(e)

	iteration += 1


