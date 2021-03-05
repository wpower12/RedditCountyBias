import pandas as pd
import pymysql as sql
import progressbar
import math
from psaw import PushshiftAPI
import rcdTools.DataCollectWeekly as dc

WEEK = 1
COHORT_SIZE = 5

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

psapi  = PushshiftAPI()

def getCandidateUseryws(conn, week, amount):
	CU_QUERY = """
	 			SELECT
				    uyw.useryw_id,
				    uyw.user_reddit_name,
				    uyw.user_reddit_id,
				    uyw.scrape_count,
				    COUNT(asub.useryw_id) AS 'links'
				FROM
				    useryw AS uyw
				JOIN
				    activesubreddit AS asub ON uyw.useryw_id=asub.useryw_id
				WHERE
				    uyw.week = {}
				GROUP BY
				    useryw_id
				ORDER BY
				    COUNT(asub.useryw_id) ASC, uyw.scrape_count ASC
				LIMIT {};"""

	users = []
	with conn.cursor() as cur:
		cur.execute(CU_QUERY.format(week, amount))
		users = cur.fetchall()
	return users

users = getCandidateUseryws(conn, WEEK, COHORT_SIZE)
for u in users:
	print(u)

print("running the new cohort process script")
first_days = pd.date_range('2020-01-01', '2020-12-31', freq='W-WED')
start_date = first_days[WEEK-1]

START_TS = int(start_date.timestamp())
a_week   = pd.Timedelta(value=7, unit="days")
END_TS   = int((start_date+a_week).timestamp())

# user_cohort, psapi, db_conn, START_TS, END_TS
dc.scrapeUserCohortAS(users, psapi, conn, START_TS, END_TS)

# This all seems to work. 