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

subs_raw = dp.getResidentSubs(conn)
sub_id2idx, sub_id2name, sub_idx2id, sub_idx2name = dp.createSubredditMaps(subs_raw)
county_fips2idx, county_idx2fips = dp.createCountyMaps(NETWORK_DIR)

dp.prepareWeeklyDataset(SAVE_DIR, county_idx2fips, sub_id2idx, conn)
