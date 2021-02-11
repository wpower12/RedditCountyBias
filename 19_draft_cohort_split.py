import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
import progressbar
import math
import pickle

"""
Splits the provided csv of location subreddits into cohorts based on their
estimated size/throughputs. 
"""

MAX_RESPONSE_SIZE = 450
SOLO_CUTOFF       = 0.85*MAX_RESPONSE_SIZE
MAX_COHORT_SIZE   = 30

COHORTS_SAVE_FN = 'data/cohorts/cohorts_comment_tp.p'

FN = 'data/locationsubs_withThroughputs.csv'
df = pd.read_csv(FN)

num_subs = 0
cohorts = []
curr_cohort = []
curr_size   = 0
for row in df.iterrows():
	sub_id    = row[1]['subreddit_id']
	sub_name  = row[1]['subreddit_name']
	sub_url   = row[1]['subreddit_url']
	county_id = row[1]['county_id']
	tp_mean   = row[1]['tp_mean']
	tp_var    = row[1]['tp_variance']
	
	sub_row = [sub_id, sub_name, sub_url, county_id, tp_mean, tp_var]
	
	size_est = tp_mean + 2.0*math.sqrt(tp_var)
	
	if size_est > SOLO_CUTOFF:
		# Create solo cohort
		cohorts.append([sub_row])

	elif ((curr_size+size_est) > MAX_RESPONSE_SIZE) or len(curr_cohort) >= MAX_COHORT_SIZE:
		# Append current cohort and start a new one, with this sub in it.
		cohorts.append(curr_cohort)
		curr_cohort = [sub_row]
		curr_size   = size_est

	else:
		# Add to the current cohort
		curr_cohort.append(sub_row)
		curr_size += size_est
		
	num_subs += 1
labelled_cohorts = []
i = 0
for c in cohorts:
	labelled_cohorts.append([i, c])
	print(" - {}th cohort, size {} - ".format(i, len(c)))
	for r in c:
		print(" -- {}".format(r))
	i += 1

pickle.dump(labelled_cohorts, open(COHORTS_SAVE_FN, "wb"))
print("{} subs split into {} cohorts. saved to {}.".format(num_subs, 
										len(cohorts), 
										COHORTS_SAVE_FN))