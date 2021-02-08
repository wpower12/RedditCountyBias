import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime as dt
import pymysql as sql
import progressbar
import requests
import math

COUNTY_FN = "./data/counties_fips.csv"

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

df = pd.read_csv(COUNTY_FN)


UPDATE_COUNTY_SQL = """UPDATE county SET fips='{}' WHERE county_id={};"""


for row in df.iterrows():
	state     = row[1]['state']
	county    = row[1]['county_name']
	county_id = row[1]['county_id']
	fips_val  = row[1]['fips']

	if not math.isnan(fips_val):
		# need to make sure this gets 0' padded at the front. 
		fips_val  = "{:0>5}".format(int(row[1]['fips']))
	else:
		fips_val  = -1

	with conn.cursor() as cursor:
		cursor.execute(UPDATE_COUNTY_SQL.format(fips_val, county_id))
	conn.commit()