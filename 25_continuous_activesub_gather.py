import pandas as pd
import pymysql as sql
import progressbar
import math
from psaw import PushshiftAPI
import rcdTools.DataCollect as dc

# Because psaw is noisy on 200s/202s
# import warnings
# warnings.simplefilter("ignore")

# First lets just get a single year pass written, then we can make it loop forever.
USERS_PER_WEEK   = 500
USER_COHORT_SIZE = 5
WEEKS = range(1, 53)
A_WEEK = pd.Timedelta(value=7, unit="days")
FIRST_DAYS = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

psapi  = PushshiftAPI()

START_WEEK = 1
iteration = 1
skipping = True

while(True):
	for WEEK in WEEKS:

		if skipping:
			if(WEEK == START_WEEK):
				skipping = False
			else:
				continue

		users_full_batch = dc.getCandidateUseryws(conn, WEEK, USERS_PER_WEEK)
		user_cohorts = dc.splitUsersIntoCohorts(users_full_batch, USER_COHORT_SIZE)

		start_date = FIRST_DAYS[WEEK-1]
		START_TS = int(start_date.timestamp())
		END_TS   = int((start_date+A_WEEK).timestamp())
		print("processing iter {}, week {}, {}".format(iteration, WEEK, start_date))
		print(" - {} cohorts, {} users".format(len(user_cohorts), len(users_full_batch)))

		links_checked    = 0
		comments_checked = 0
		ch_bar = progressbar.ProgressBar(max_value=len(user_cohorts), redirect_stdout=True)
		i = 0
		for cohort in user_cohorts:
			N_comments, N_links = dc.scrapeUserCohortAS(cohort, psapi, conn, START_TS, END_TS)
			links_checked    += N_links
			comments_checked += N_comments
			i += 1
			ch_bar.update(i)
		ch_bar.finish()

		print(" - {} comments, {} links checked.".format(comments_checked, links_checked))
	iteration += 1




