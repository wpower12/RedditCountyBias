import pandas as pd
import pymysql as sql
import progressbar
import math
import scipy.sparse as ss

conn = sql.connect(host='localhost',
		   user='bill',
		   password='password',
		   database='reddit_data')

SAVE_DIR = 'data/test_data'

# Ok so we need to grab the x/y info. So for each week, we need each county, and their 
# list of activities for the X, and then for the Y, for each week, for each county,
# their covid data.

# I think it will be important to get a map from a subreddit to an index, so we could
GET_SUBS_QUERY = """SELECT sr.subreddit_id, sr.subreddit_name
		FROM 
			(SELECT DISTINCT(subreddit_id) FROM activesubreddit) AS T
		LEFT JOIN 
			subreddit as sr ON T.subreddit_id=sr.subreddit_id;"""



