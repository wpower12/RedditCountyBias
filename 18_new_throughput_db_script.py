import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
import progressbar
import warnings
warnings.simplefilter("ignore")

FIRST_N_WEEKS = 5
MAX_RESPONSES = 500
START_SUB     = '/r/alabama'

FN_IN  = "./data/locationsubs_withCounties.csv"
df_in  = pd.read_csv(FN_IN)

# Connections
psapi  = PushshiftAPI()
conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

# Getting the right weeks to pass in. 
first_days   = list(pd.date_range('2020-01-01', '2020-12-31', freq='W-WED'))
target_weeks = first_days[:FIRST_N_WEEKS] # First 5 weeks used 

total = len(df_in)
sub_bar = progressbar.ProgressBar(max_value=total, redirect_stdout=True)
sub_bar.start()
i = 1
skipping = True

try:
	for row in df_in.iterrows():
		state    = row[1]['state']
		sub_url  = row[1]['subreddit url']
		sub_name = row[1]['subreddit name']
		sub_id   = row[1]['subreddit id']
		county   = row[1]['county']

		# Skipping to START_SUB - Assumes consistent ordering in the dataframe bw runs. 
		if( skipping and (sub_url != START_SUB )): 
			skipping = False
			continue

		sub_mu, sub_var = cu.estimateWeeklyThroughput(sub_name, 
											psapi, 
											target_weeks, 
											MAX_RESPONSES)

		sub_bar.update(i)
		i += 1

		# Now we add this shit to the database.
		UPDATE_SUB_SQL = """
						UPDATE reddit_data.subreddit 
						SET
							tp_mean     = {},
						    tp_variance = {}
						WHERE
							subreddit_id = '{}';"""

		with conn.cursor() as cur:
			cur.execute(UPDATE_SUB_SQL.format(sub_mu, sub_var, sub_id))
		conn.commit()

		print("added {} - mu: {} var: {}".format(sub_url, sub_mu, sub_var))

finally:
	sub_bar.finish()