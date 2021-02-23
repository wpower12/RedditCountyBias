import pandas as pd
import pymysql as sql
import progressbar
import math
import scipy.sparse as ss
import numpy as np

import rcdTools.DataPrepare as dp

conn = sql.connect(host='localhost',
		   user='bill',
		   password='password',
		   database='reddit_data')

SAVE_DIR    = 'data/test_data'
NETWORK_DIR = 'data/county_network' 

# Ok so we need to grab the x/y info. So for each week, we need each county, and their 
# list of activities for the X, and then for the Y, for each week, for each county,
# their covid data.

# I think it will be important to get a map from a subreddit to an index, so we could
GET_SUBS_QUERY = """SELECT sr.subreddit_id, sr.subreddit_name
		FROM 
			(SELECT DISTINCT(subreddit_id) FROM activesubreddit) AS T
		LEFT JOIN 
			subreddit as sr ON T.subreddit_id=sr.subreddit_id;"""

subs = []
with conn.cursor() as cur:
	cur.execute(GET_SUBS_QUERY)
	subs = cur.fetchall()

sub_id2idx, sub_id2name, sub_idx2id, sub_idx2name = dp.createSubredditMaps(subs)
county_fips2idx, county_idx2fips = dp.createCountyMaps(NETWORK_DIR)


# SO. We make numpy tensors of the appropriate size, empty.
# Then, we just iterate over the indexes for each dimension
# and use the relevant maps to figure out what to fill in for
# the FIPS value and week # in the above query. 
NUM_WEEKS    = 52
NUM_COUNTIES = len(county_fips2idx) # Either map would work
NUM_SUBS     = len(sub_id2idx) # Really the len of any sub map would do.

# WxCxS
# full_data = np.ones((NUM_WEEKS, NUM_COUNTIES, NUM_SUBS),dtype=int)

# Sparse Sequence, just saving them to a dir.
SHAPE = (NUM_COUNTIES, NUM_SUBS)
for WEEK in range(NUM_WEEKS):
	# If each weekly slice is a CxS, that means each row is a county
	rows = [] # county index
	cols = [] # subreddit index
	data = [] # actual count value

	for COUNTY in range(NUM_COUNTIES):
		fips_val = county_idx2fips[COUNTY]
		activity_summary = dp.getActivitySummary(WEEK+1, fips_val, conn)

		for sub in activity_summary:
			sub_name, sub_count, sub_id = sub
			sub_idx = sub_id2idx[sub_id]

			# again, we have full_data[COUNTY_IDX][SUB_IDX] = VAL
			rows.append(COUNTY) # add the county 
			cols.append(sub_idx)
			data.append(sub_count)

	cs_data = ss.coo_matrix((data, (rows, cols)), shape=SHAPE, dtype='int')
	ss.save_npz("{}/WEEK_{}.npz".format(SAVE_DIR, "{}".format(WEEK+1).zfill(2)), cs_data)


