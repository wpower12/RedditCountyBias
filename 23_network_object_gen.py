# So lets try this by hand. The import wizard in mysql wb sucks.
import pandas as pd
import pymysql as sql
import progressbar
import math
import scipy.sparse as ss

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

SAVE_DIR = 'data/county_network'

## Adjacency Matrix and List 
COUNTY_ADJ_FN = "data/county_adjacency2010.csv"
county_df = pd.read_csv(COUNTY_ADJ_FN)

# I think I'll just be straight forward and do 2 passes. One to get 
# all the FIPS->Index's figured out, and also to know the exact number
# of 'nodes'. Then a second pass to fill in an actual NxN adj matrix, and 
# then the adj list. 

# First Pass - Get map. Add all FIPS to a set, then turn set into map and list.
fips_set = set()
for edge in county_df.iterrows():
	fips_u = edge[1]['fipscounty']
	fips_v = edge[1]['fipsneighbor']

	fips_set.add("{}".format(fips_u).zfill(5))
	fips_set.add("{}".format(fips_v).zfill(5))

fips_to_idx = dict()
idx_to_fips = []
i = 0
for fips in fips_set:
	fips_to_idx[fips] = i
	idx_to_fips.append(fips)
	i += 1

# Second Pass - 
#   At this point, these indexes should be common between the dict and
# the list. We can then use these to index into a NxN sparse matrix while
# we iterate over the df again. 
rows = []
cols = []
data = []
for edge in county_df.iterrows():
	fips_u = "{}".format(edge[1]['fipscounty']).zfill(5)
	fips_v = "{}".format(edge[1]['fipsneighbor']).zfill(5)

	idx_u = fips_to_idx[fips_u]
	idx_v = fips_to_idx[fips_v]

	rows.append(idx_u)
	cols.append(idx_v)
	data.append(True)

N_county = len(fips_set)
c_adj_matrix = ss.coo_matrix( (data, (rows, cols)), shape=(N_county, N_county), dtype='bool')

ss.save_npz("{}/county_adj.npz".format(SAVE_DIR), c_adj_matrix)

f2i_fn = "fips_to_idx.csv"
i2f_fn = "idx_to_fips.csv"
f_f2i = open("{}/{}".format(SAVE_DIR, f2i_fn), 'w')
f_i2f = open("{}/{}".format(SAVE_DIR, i2f_fn), 'w')

idx = 0
for fips in idx_to_fips:
	f_i2f.write("{}, {}\n".format(idx, fips))
	idx += 1


for fips in fips_to_idx:
	idx = fips_to_idx[fips]
	f_f2i.write("{}, {}\n".format(fips, idx))


f_f2i.close()
f_i2f.close()