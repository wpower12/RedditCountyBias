import pandas as pd


def getResidentSubs(conn):
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

	return subs


def createSubredditMaps(subs):
	# Maybe this is overkill but idc. These are all the things I usually want/need.
	id2idx   = {} 
	id2name  = {} 
	idx2id   = []
	idx2name = []
	idx = 0
	for sub in subs:
		sub_id, sub_name = sub

		id2idx[sub_id]  = idx
		id2name[sub_id] = sub_name
		idx2id.append(sub_id)
		idx2name.append(sub_id)

	return id2idx, id2name, idx2id, idx2name


def createCountyMaps(network_DIR):
	county_idx2fips_df = pd.read_csv("{}/idx_to_fips.csv".format(network_DIR))
	idx2fips = []
	fips2idx = {}
	for row in county_idx2fips_df.iterrows():
		idx  = row[1][0]
		fips = "{}".format(row[1][1]).zfill(5)

		idx2fips.append(fips)
		fips2idx[fips] = idx

	return fips2idx, idx2fips


def getActivitySummary(week, fips, conn):
	# print(fips)
	AS_SQL = """SELECT sname.subreddit_name, COUNT(*), sname.subreddit_id as activeusers
	FROM 
		reddit_data.activesubreddit as a
	JOIN 
		reddit_data.useryw    as u on a.useryw_id=u.useryw_id
	JOIN 
		reddit_data.subreddit as s on u.home_subreddit=s.subreddit_id
	JOIN 
		reddit_data.county    as c on s.county_id=c.county_id
	JOIN 
		reddit_data.subreddit as sname on a.subreddit_id = sname.subreddit_id
	WHERE 
		c.county_id=(SELECT county_id 
					 FROM reddit_data.county 
					 WHERE fips={}) AND 
		u.week={} AND
		sname.subreddit_id <> u.home_subreddit
	GROUP BY sname.subreddit_name, sname.subreddit_id
	ORDER BY activeusers DESC;"""
	res = []
	with conn.cursor() as cur:
		cur.execute(AS_SQL.format(fips, week))
		res = cur.fetchall()

	return res


def prepareWeeklyDataset(save_dir, c_idx2fips, s_id2idx, db_conn):
	NUM_WEEKS    = 52
	NUM_COUNTIES = len(c_idx2fips) # Either map would work
	NUM_SUBS     = len(s_id2idx)   # Really the len of any sub map would do.

	TOTAL_QUERIES = NUM_WEEKS*NUM_COUNTIES
	q_bar = progressbar.ProgressBar(max_value=TOTAL_QUERIES)
	q = 0
	SHAPE = (NUM_COUNTIES, NUM_SUBS)
	for WEEK in range(NUM_WEEKS):
		# If each weekly slice is a CxS, that means each row is a county
		rows = [] # county index
		cols = [] # subreddit index
		data = [] # actual count value

		for COUNTY in range(NUM_COUNTIES):
			fips_val = c_idx2fips[COUNTY]
			activity_summary = dp.getActivitySummary(WEEK+1, fips_val, conn)
			for sub in activity_summary:
				sub_name, sub_count, sub_id = sub
				sub_idx = s_id2idx[sub_id]

				# again, we have full_data[COUNTY_IDX][SUB_IDX] = VAL
				rows.append(COUNTY) # add the county 
				cols.append(sub_idx)
				data.append(sub_count)
			q += 1
			q_bar.update(q)
		cs_data = ss.coo_matrix((data, (rows, cols)), shape=SHAPE, dtype='int')
		ss.save_npz("{}/WEEK_{}.npz".format(SAVE_DIR, "{}".format(WEEK+1).zfill(2)), cs_data)
