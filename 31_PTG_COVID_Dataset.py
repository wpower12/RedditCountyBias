import pandas as pd
import pymysql as sql
import progressbar
import math
import scipy.sparse as ss
import numpy as np

START_DATE = pd.to_datetime("04/12/2020")
END_DATE   = pd.to_datetime("12/31/2020")
NUM_DAYS   = (END_DATE-START_DATE).days
SAVE_FN    = "data/fips_date_nid_map.csv"

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

curr_id = 0
for county_row in county_fips_df.iterrows():
	fips = county_row[1]['fips']
	for day in days:
		key_string = "{}-{}".format(fips, day.date())
		countydate_nid_map[key_string] = curr_id
		curr_id += 1

print("county/date to node id map created.")
print("{} nodes".format(len(countydate_nid_map)))

save_df = pd.DataFrame.from_dict(countydate_nid_map, orient='index')

with open(SAVE_FN, "w") as f:
	save_df.to_csv(f)

print("map saved as csv to {}".format(SAVE_FN))