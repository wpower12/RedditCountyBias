import pymysql as sql
import progressbar
import rcdTools.DataCollectDaily as dc

SUB_PER_COHORT   = 10
DAYS_PER_COHORT  = 10
USERS_PER_COHORT = 10

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

iteration = 1
running = True
while running:
	## SUBREDDIT PASS - collecting more users (hopefully)
	# Do a pass over each day, for each cohort. 
	lsub_cohorts = dc.getRandPartSubreddits(SUB_PER_COHORT, conn)
	for day in range(1, 366):
		daily_total = 0
		daily_pbar = progressbar.ProgressBar(max_value=len(lsub_cohorts), 
				prefix="Subs: Day {}, Iter {}: ".format(day, iteration),
				redirect_stdout=True)
		i = 0
		for lsub_cohort in lsub_cohorts:
			u_count = dc.subredditCohortGather(conn, lsub_cohort, 2020, day)
			daily_total += u_count
			i += 1
			daily_pbar.update(i)
		daily_pbar.finish()
		print("Subs: Day {}, Iter {}: {}".format(day, iteration, daily_total))

	## USER PASS 
	# Do a pass over each day, for a new set of random users from that day.
	for day in range(1, 366):
		# Forgot I don't handle the 'sets' in the gather yet.
		user_cohorts = dc.getRandPartUseryds([day], USERS_PER_COHORT, conn)
		daily_total = 0
		daily_pbar = progressbar.ProgressBar(max_value=len(user_cohorts), 
				prefix="AS's: Day {}, Iter {}: ".format(day, iteration),
				redirect_stdout=True)
		i = 0
		for user_cohort in user_cohorts:
			as_count = dc.userydASCohortGather(conn, user_cohort, 2020, day)
			daily_total += as_count
			i += 1
			daily_pbar.update(i)
		daily_pbar.finish()
		print("AS's: Day {}, Iter {}: {}".format(day, iteration, daily_total))

	iteration += 1



# for ds in day_sets:
# 	user_cohorts = dc.getRandPartUseryds(ds, USERS_PER_COHORT, conn)

# 	print(len(user_cohorts))
