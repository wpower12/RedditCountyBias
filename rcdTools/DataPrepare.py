import pandas as pd

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

def addFeatureVector(full_tensor, fips, week, conn):
	# Will work on the full tensor, adding in the appropriate values. 
	# Will need the sub maps, I think. 



	pass