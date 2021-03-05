import pandas as pd
import pymysql as sql
import progressbar
import math

import rcdTools.DataCollectDaily as dc

YEAR = 2020
NUM_SUBS_PER_COHORT = 10

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

running = True
while(running):
	iteration = 0
	for DAY in range(1, 365):
		try:
			# Subreddit Gather
			subs    = dc.getCandidateSubreddits(conn, DAY, NUM_SUBS_PER_COHORT)
			u_count = dc.subredditCohortGather(subs, YEAR, DAY, conn) 

			# User Gather
			
	

		# Totally clean code, right?
		except KeyboardInterrupt as kbi:
			pass

		except Exception as e:
			print(e)

		finally:
			running = False
			break

