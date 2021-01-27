import CountyUsers as cu
import pandas as pd
import time
import datetime
import pymysql as sql

FN = "./data/locationsubs_withCounties.csv"
df = pd.read_csv(FN)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')


def clean_c_name_str(county_name):
	return county_name.replace('\'', '')


# Get a set of the unique state/county_name pairs
df_counties_only = df[df['county'].notna()]
print("found {} rows with county labels".format(len(df_counties_only)))
county_set = set()
for row in df_counties_only.iterrows():
	state  = row[1]['state']
	c_name = clean_c_name_str(row[1]['county'])
	county_set.add((state, c_name))
print("found {} unique state/county pairs".format(len(county_set)))


COUNTY_SQL = """
			INSERT INTO reddit_data.county 
				(state, county_name) 
			VALUES 
				('{}', '{}');
			"""

# Add these to db. 
for sc in county_set:
	state, county = sc
	print("adding {}, {}".format(county, state))
	with conn.cursor() as cursor:
		cursor.execute(COUNTY_SQL.format(state, county))
	conn.commit()


# Second pass to add them in.
for row in df_counties_only.iterrows():
	sub_name = row[1]['subreddit name']
	sub_id   = row[1]['subreddit id']
	state    = row[1]['state']
	c_name   = clean_c_name_str(row[1]['county'])

	# Get county id from db
	C_ID_SQL = """
			SELECT county_id from reddit_data.county 
			WHERE
				state='{}' AND county_name='{}';
			"""
	with conn.cursor() as cursor:
		cursor.execute(C_ID_SQL.format(state, c_name))
		res = cursor.fetchall()

	# print(res)
	c_id = res[0][0]

	print("updating {} {} {} {}".format(sub_id, state, c_name, c_id))
	UPDATE_SQL = """
				UPDATE reddit_data.subreddit 
				SET 
					county_id={}
				WHERE
					subreddit_id='{}';
				"""
	with conn.cursor() as cursor:
		cursor.execute(UPDATE_SQL.format(c_id, sub_id))
	conn.commit()
