import CountyUsers as cu
import pandas as pd
import time
import datetime
import pymysql as sql
import praw
from progress.bar import Bar


FN_IN  = "./data/locationsubs.csv"
FN_OUT = "./data/locationsubs_withIDs.csv"

df_raw = pd.read_csv(FN_IN)

reddit = praw.Reddit("data_enrich", user_agent="data_project_ua")

data_raw = []


bar = Bar("Subreddits", max=len(df_raw))
for row in df_raw.iterrows():
	state    =  row[1]['state']
	sub_url  =  row[1]['subreddit url']
	sub_name =  row[1]['subreddit name']

	sub = reddit.subreddit(sub_url[3:])
	
	try:
		new_row = [state, sub_url, sub_name, sub.id]
		data_raw.append(new_row)
	except Exception as e:
		pass
	finally:
		bar.next()


bar.finish()
cols = ['state', 'subreddit url', 'subreddit name', 'subreddit id']
df_new = pd.DataFrame(data=data_raw, columns=cols)
df_new.to_csv(FN_OUT)
print("wrote to {}".format(FN_OUT))
