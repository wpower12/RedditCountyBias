import rcdTools.DataCollect as dc
import pandas as pd
import time
import datetime
import pymysql as sql
import urllib

WAIT_SECONDS = 8.0
FN_OLD = "./data/locationsubs_withIDs.csv"
FN_OUT = "./data/locationsubs_withCounties.csv"
df_raw = pd.read_csv(FN_OLD)

data_raw = []
total = len(df_raw)
i = 1
for row in df_raw.iterrows():
	state    = row[1]['state']
	sub_url  = row[1]['subreddit url']
	sub_name = row[1]['subreddit name']
	sub_id   = row[1]['subreddit id']
	county   = None
	
	try:
		query  = "{} {}".format(sub_name, state)
		county = dc.get_county(query)
		print("processing {}/{}, {} {}".format(i, total, state, sub_name))
	except Exception as e:
		# pass
		print("\t error: {}".format(e))
	finally:
		new_row = [state, sub_url, sub_name, sub_id, county]
		data_raw.append(new_row)
		i += 1

	time.sleep(WAIT_SECONDS)

cols = ['state', 'subreddit url', 'subreddit name', 'subreddit id', 'county']
df_new = pd.DataFrame(data=data_raw, columns=cols)
df_new.to_csv(FN_OUT)
print("wrote to {}".format(FN_OUT))
