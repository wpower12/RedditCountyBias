# So lets try this by hand. The import wizard in mysql wb sucks.
import pandas as pd
import pymysql as sql
import progressbar
import math

COUNTY_FN = "db/testdb_source/county.csv"
SUBRED_FN = "db/testdb_source/subreddit.csv"
USERYW_FN = "db/testdb_source/useryw.csv"
ACTIVE_FN = "db/testdb_source/activesubreddit.csv"

county_df = pd.read_csv(COUNTY_FN, sep=';')
subred_df = pd.read_csv(SUBRED_FN, sep=';')
useryw_df = pd.read_csv(USERYW_FN, sep=';')
active_df = pd.read_csv(ACTIVE_FN, sep=';')

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_data')

##### COUNTY Insert
C_INS_SQL = """ INSERT INTO `reddit_data`.`county`
				(`county_id`,
				`county_name`,
				`state`,
				`fips`)
				VALUES
				({}, '{}', '{}', '{}');"""

print("Inserting COUNTY Data")
c_bar = progressbar.ProgressBar(max_value=len(county_df), redirect_stdout=True)
i = 1
for c_row in county_df.iterrows():
	# print(c_row[1])
	c_id    = c_row[1]['county_id']
	c_name  = c_row[1]['county_name']
	c_state = c_row[1]['state']
	c_fips  = c_row[1]['fips']

	with conn.cursor() as cur:
		cur.execute(C_INS_SQL.format(c_id, c_name, c_state, c_fips))
	conn.commit()
	c_bar.update(i)
	i += 1
c_bar.finish()

##### SUBREDDIT Insert
S_INS_SQL = """ INSERT INTO `reddit_data`.`subreddit`
				(`subreddit_id`,
				`subreddit_name`,
				`subreddit_url`,
				`county_id`,
				`tp_mean`,
				`tp_variance`)
				VALUES
				('{}', '{}', '{}', {}, {}, {});"""

print("Inserting SUBREDDIT Data")
s_bar = progressbar.ProgressBar(max_value=len(subred_df), redirect_stdout=True)
i = 1
for s_row in subred_df.iterrows():
	s_id   = s_row[1]['subreddit_id']
	s_name = s_row[1]['subreddit_name']
	s_url  = s_row[1]['subreddit_url']
	s_cid  = s_row[1]['county_id']
	s_tpm  = s_row[1]['tp_mean']
	s_tpv  = s_row[1]['tp_variance']

	if(math.isnan(s_cid)):
		s_cid = 'NULL'

	with conn.cursor() as cur:
		cur.execute(S_INS_SQL.format(s_id, s_name, s_url, s_cid, s_tpm, s_tpv))
	conn.commit()
	s_bar.update(i)
	i += 1
s_bar.finish()

##### USERYW Insert
U_INS_SQL = """ INSERT INTO `reddit_data`.`useryw`
				(`useryw_id`,
				`user_reddit_id`,
				`user_reddit_name`,
				`year`,
				`week`,
				`home_subreddit`)
				VALUES
				({}, '{}', '{}', {}, {}, '{}');"""

print("Inserting USERYW Data")
u_bar = progressbar.ProgressBar(max_value=len(useryw_df), redirect_stdout=True)
i = 1
for u_row in useryw_df.iterrows():
	u_id     = u_row[1]['useryw_id']
	u_r_id   = u_row[1]['user_reddit_id']
	u_r_name = u_row[1]['user_reddit_name']
	u_year   = u_row[1]['year']
	u_week   = u_row[1]['week']
	u_home   = u_row[1]['home_subreddit']

	with conn.cursor() as cur:
		cur.execute(U_INS_SQL.format(u_id, 
						u_r_id, 
						u_r_name, 
						u_year, 
						u_week, 
						u_home))
	conn.commit()
	u_bar.update(i)
	i += 1
u_bar.finish()

##### ACTIVESUB Insert
A_INS_SQL = """ INSERT INTO `reddit_data`.`activesubreddit`
				(`subreddit_id`,
				`useryw_id`)
				VALUES
				('{}', {});"""

print("Inserting ACTIVESUB Data")
a_bar = progressbar.ProgressBar(max_value=len(active_df), redirect_stdout=True)
i = 1
for a_row in active_df.iterrows():
	a_sid = a_row[1]['subreddit_id']
	a_uid = a_row[1]['useryw_id']

	with conn.cursor() as cur:
		cur.execute(A_INS_SQL.format(a_sid, a_uid))
	conn.commit()
	a_bar.update(i)
	i += 1
a_bar.finish()