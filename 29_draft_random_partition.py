import pymysql as sql
import rcdTools.DataCollectDaily as dc

SUB_PER_COHORT   = 10
DAYS_PER_COHORT  = 10
USERS_PER_COHORT = 10

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

lsub_cohorts = dc.getRandPartSubreddits(SUB_PER_COHORT, conn)
print(len(lsub_cohorts))

days = list(range(1, 366))
day_sets = []
for i in range(0, len(days), DAYS_PER_COHORT):
	day_sets.append(days[i:i+DAYS_PER_COHORT])


for ds in day_sets:
	user_cohorts = dc.getRandPartUseryds(ds, USERS_PER_COHORT, conn)

	print(len(user_cohorts))

