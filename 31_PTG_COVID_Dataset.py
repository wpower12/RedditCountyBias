import pandas as pd
import pymysql as sql
import progressbar
import math
import scipy.sparse as ss
import numpy as np

import torch
from torch_geometric.data import Data

START_DATE  = pd.to_datetime("04/12/2020")
END_DATE    = pd.to_datetime("12/31/2020")
NUM_DAYS    = (END_DATE-START_DATE).days
MAP_SAVE_FN = "data/fips_date_nid_map.csv"

# Now we need the COUNTY_FIPS list
# We have a file in the data directory that contains all the counties in
# our database, with their DB county ids, their FIPS ids.
# Can iterate over this to build the index. 
# Maybe we use a hash? Concatenate the FIPS+DATE strings, use those
# as the keys to a hashmap that returns the node id within the full graph.

COUNTY_FIPS_CSV_FN = "data/counties_fips.csv"
county_fips_df = pd.read_csv(COUNTY_FIPS_CSV_FN, dtype={'fips': 'str'}, )

days = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
countydate_nid_map = {}
nid_countydate_map = []

curr_id = 0
for county_row in county_fips_df.iterrows():
	fips = county_row[1]['fips']
	for day in days:
		key_string = "{}-{}".format(fips, day.date())
		countydate_nid_map[key_string] = curr_id
		nid_countydate_map.append([fips, day])
		curr_id += 1

print("county/date to node id map created.")
print("{} nodes".format(len(countydate_nid_map)))

save_df = pd.DataFrame.from_dict(countydate_nid_map, orient='index')
with open(MAP_SAVE_FN, "w") as f:
	save_df.to_csv(f)

print("map saved as csv to {}".format(MAP_SAVE_FN))

### Feature Tensor
# MOBILITY_FN = "data/2020_US_Region_Mobility_Report.csv"
# target_cols = ["census_fips_code",
# 	"date",
# 	"retail_and_recreation_percent_change_from_baseline",
# 	"grocery_and_pharmacy_percent_change_from_baseline",
# 	"parks_percent_change_from_baseline",
# 	"transit_stations_percent_change_from_baseline",
# 	"workplaces_percent_change_from_baseline",
# 	"residential_percent_change_from_baseline"]

# DTYPES = {'census_fips_code': 'str',
# 	"date": 'str'}

# mobility_df = pd.read_csv(MOBILITY_FN, dtype=DTYPES)
# mobility_df = mobility_df[target_cols]
# m_df = mobility_df[mobility_df['census_fips_code'].notna()]
# m_df.fillna(0, inplace=True)

# print("{} items in df".format(len(mobility_df)))

# # I think first Ill make an empty list then fill it in as I
# # iterate over the dataframe. We have 6 mobility features. 
# x = [[0.0 for o in range(6)] for i in range(len(nid_countydate_map))]
# rows_touched = 0
# for m_row in m_df.iterrows():
# 	m_raw = m_row[1]

# 	fips = m_raw[0]
# 	date = m_raw[1]
# 	data = [m_raw[i+2]/100.0 for i in range(6)]

# 	node_key = "{}-{}".format(fips, date)

# 	# I have mismatching date ranges, so there will 
# 	# be key errors if i iterate over the much larger
# 	# date range of the full mobility dataframe. This
# 	# hack catches that. 
# 	try:
# 		idx = countydate_nid_map[node_key]
# 	except Exception as e:
# 		idx = -1
# 	finally:
# 		if idx != -1:
# 			x[idx] = data
# 			rows_touched += 1

# X_SAVE_FN = "data/X_features.csv"
# save_df = pd.DataFrame(x)
# with open(X_SAVE_FN, 'w') as f:
# 	save_df.to_csv(f)

# print("feature tensor complete.")
# print("{} rows touched. {} in full tensor".format(rows_touched, len(x)))
# print("saved to {}".format(X_SAVE_FN))


# ## Adjacency Information
# # Need to make the COO list. 
# ADJ_CSV_FN = "data/county_adjacency2010.csv"
# ADJ_DTYPES = {'fipscounty': 'str', 'fipsneighbor': 'str'}
# adj_df = pd.read_csv(ADJ_CSV_FN)

# # So if you are a node u in the past, you point 'up' towards
# # the HIST_WINDOW many nodes v in the future. 
# HIST_WINDOW = 5

# coo_list = []
# adj_count = 0
# for link in adj_df.iterrows():
# 	# Adding links u -> v and v -> u over all days.
# 	_, u, _, v = link[1]

# 	for day in days:
# 		day_str = day.date()
# 		u_key = "{}-{}".format(u, day_str)
# 		v_key = "{}-{}".format(v, day_str)
# 		try:
# 			u_idx = countydate_nid_map[u_key]
# 			v_idx = countydate_nid_map[v_key]
# 			coo_list.append([u_idx, v_idx])
# 			coo_list.append([v_idx, u_idx])
# 			adj_count += 1
# 		except KeyError:
# 			pass
# 		except Exception as e:
# 			print(e)

# # Temporal Links
# temp_count = 0
# for base_day_idx in range(0, len(days)-HIST_WINDOW):
# 	base_day = days[base_day_idx]
# 	bd_str = base_day.date()
# 	for future_day in days[base_day_idx+1 : base_day_idx+HIST_WINDOW+1]:
# 		fd_str = future_day.date()

# 		# iterate over each county fips
# 		for county_row in county_fips_df.iterrows():
# 			fips = county_row[1]['fips']

# 			# Need a link from base_day to future_day
# 			u_key = "{}-{}".format(fips, bd_str)
# 			v_key = "{}-{}".format(fips, fd_str)
# 			try:
# 				u_idx = countydate_nid_map[u_key]
# 				v_idx = countydate_nid_map[v_key]
# 				coo_list.append([u_idx, v_idx])
# 				coo_list.append([v_idx, u_idx])
# 				temp_count += 1
# 			except KeyError:
# 				pass
# 			except Exception as e:
# 				print(e)

# print("coo list created.")
# print("{} adj links, {} temporal links".format(adj_count, temp_count))
# COO_SAVE_FN = "data/coo_list.csv"
# coo_save_df = pd.DataFrame(coo_list)
# with open(COO_SAVE_FN, 'w') as f:
# 	coo_save_df.to_csv(f)

# print("coo list saved to {}".format(COO_SAVE_FN))

# Now the only missing part are the targets.
# So we go over each county, each date and add in the total?
# blarg. 

# Even with this, I think we have a DataFrame? We could viz it i think.
# Would take fucking forever. Just don't have a target. Still can be
# used for unsuperivsed tasks. 

# Ok now onto the target values. 
CSV_DIR = "data/csse_covid_19_daily_reports"
y_raw = [[0, 0] for i in range(len(countydate_nid_map))]
e_count = 0

for day in days:
	csv_fn = "{:0>2}-{:0>2}-{}.csv".format(day.month, day.day, day.year)
	day_df = pd.read_csv("{}/{}".format(CSV_DIR, csv_fn), dtype={'FIPS': 'str'})
	day_df = day_df[day_df['FIPS'].notna()]
	day_df = day_df[day_df['FIPS'].str.len() == 5]

	# Should now have a county-only dataframe.
	for c_row in day_df.iterrows():
		c_raw = c_row[1]
		fips  = c_raw[0]
		n_confirmed = c_raw[7] # TODO - Double check these!!!
		n_dead      = c_raw[8]

		key_str  = "{}-{}".format(fips, day.date())

		try:
			node_idx = countydate_nid_map[key_str]
			y_raw[node_idx] = [n_confirmed, n_dead]

		except KeyError:
			pass

		except Exception as e:
			e_count += 1

print("y tensor complete.")
print("{} entries".format(len(y_raw)))
print("{} exceptions to figure out.".format(e_count))

Y_SAVE_FN = "data/Y_targets.csv"
y_save_df = pd.DataFrame(y_raw)
with open(Y_SAVE_FN, 'w') as f:
	y_save_df.to_csv(f)

print("saved to {}".format(Y_SAVE_FN))
