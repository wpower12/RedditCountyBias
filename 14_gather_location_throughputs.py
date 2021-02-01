import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql

FIRST_N_WEEKS = 5
MAX_RESPONSES = 400

FN_IN  = "./data/locationsubs_withCounties.csv"
FN_OUT = "./data/locationsubs_withThroughputs.csv"
df_in  = pd.read_csv(FN_IN)

psapi  = PushshiftAPI()
first_days   = list(pd.date_range('2020-01-01', '2020-12-31', freq='W-WED'))
target_weeks = first_days[:FIRST_N_WEEKS] # First 5 weeks used 

data_raw = []
total = len(df_in)
for row in df_in.iterrows():
	state    = row[1]['state']
	sub_url  = row[1]['subreddit url']
	sub_name = row[1]['subreddit name']
	sub_id   = row[1]['subreddit id']
	county   = row[1]['county']

	sub_mu, sub_var = cu.estimateWeeklyThroughput(sub_url, 
										psapi, 
										target_weeks, 
										MAX_RESPONSES)

	new_row = [state, sub_url, sub_name, sub_id, county, sub_mu, sub_var]
	data_raw.append(new_row)

cols = ['state', 
		'subreddit url', 
		'subreddit name', 
		'subreddit id', 
		'county', 't_mu', 
		't_var']

df_new = pd.DataFrame(data=data_raw, columns=cols)
df_new.to_csv(FN_OUT)

print("wrote to {}".format(FN_OUT))