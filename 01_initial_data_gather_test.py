import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql


FN = "./data/location_subreddit_counties.csv"
TARGET_YEAR = 2016
SOURCE_COMMENTS_CHECKED = 500
NUM_COMMENTS_CHECKED    = 500


START_TS = int(time.mktime(datetime.date(TARGET_YEAR, 1,   1).timetuple()))
END_TS   = int(time.mktime(datetime.date(TARGET_YEAR, 12, 30).timetuple()))

df = pd.read_csv(FN)
df = df[df['county'].notna()]

reddit = praw.Reddit("data_enrich", user_agent="data_project_ua")
psapi  = PushshiftAPI(reddit)

conn = sql.connect(host='localhost',
				   user='bill',
				   password='password',
				   database='reddit_location_bias')

# Collecting subreddits and users
for sub_row in df.iterrows():
	try:
		sub_url  = sub_row[1]['subreddit url']
		sub_name = sub_row[1]['subreddit name']
		
		sub_obj  = reddit.subreddit(sub_url[3:])
		sub_id   = sub_obj.id  # Takes a full query with praw :(

		## Add subreddit to table.
		SUB_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.subreddit 
							(subreddit_id, subreddit_name, subreddit_url) 
						VALUES (\'{}\', \'{}\', \'{}\');"""

		with conn.cursor() as cursor:
			cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_url))
		conn.commit()
		print("insert attempted: {}".format(sub_url))


		## Finding 'Active Users' in subreddit by looking at comments.- The PushShift Query. 
		sc_cache = []
		source_comments = psapi.search_comments(after=START_TS,
                                     		 before=END_TS,
                                     		 subreddit=sub_name,
                                     		 limit=SOURCE_COMMENTS_CHECKED)
		for c in source_comments:
			sc_cache.append(c)

		USER_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.user 
							(user_id, user_name, home_subreddit) 
						VALUES 
							(\'{}\', \'{}\', \'{}\');"""
		user_count = 0
		comments_checked = 0
		comments_skipped = 0
		for comment in sc_cache:
			## Attempt to add user to the database
			try:
				with conn.cursor() as cursor:
					cursor.execute(USER_INS_SQL.format(comment.author.id, 
													   comment.author, 
													   sub_id))
				conn.commit()

				## Add 'Active Subreddits' by looking at the comments made by the author
				uc_cache = []
				user_comments = psapi.search_comments(after=START_TS,
		                                     		  before=END_TS,
		                                     		  author=comment.author,
		                                     		  limit=NUM_COMMENTS_CHECKED)
				for c in user_comments:
					uc_cache.append(c)

			except:
				pass

			AS_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.active_subreddits
								(user_id, subreddit_id, year, unique_hash)
							VALUES
								(\'{}\', \'{}\', {} ,\'{}\');"""

			for user_comment in uc_cache:
				# first we ignore add the sub
				try:
					with conn.cursor() as cursor:
						cursor.execute(SUB_INS_SQL.format(user_comment.subreddit_id, 
														  user_comment.subreddit, 
														  user_comment.subreddit))
					conn.commit()

					# now the AS
					with conn.cursor() as cursor:
						unique_hash = "{}{}{}".format(comment.author.id, 
												   	  user_comment.subreddit_id[3:], 
												   	  TARGET_YEAR,)
						cursor.execute(AS_INS_SQL.format(comment.author.id, 
													   	 user_comment.subreddit_id[3:], 
													   	 TARGET_YEAR,
													   	 unique_hash))
					conn.commit()
					comments_checked += 1
				except:
					comments_skipped += 1
			user_count += 1

		print(" - added {} users".format(user_count))
		print(" - checked {} comments".format(comments_checked))
		print(" - skipped {} comments".format(comments_skipped))

	except Exception as e:
		print("exception: {}, {}".format(sub_row[1]['subreddit url'], e))
