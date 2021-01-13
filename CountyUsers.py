import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
import sys

"""
inputs:
 subreddit_df - Pandas df of the subreddits to process. Assumed ordered alphabetiacally.
 start_sub    - name of the sub to start with - NOT IMPLEMENTED YET.
 year         - Year of users/comments to process.
 user_N       - number of source comments to process for each subreddit when finding users.
 active_N     - number of source comments to process for each user when finding active subreddits
 reddit_obj   - A connected PRAW reddit object
 db_conn      - A pymysql connection to a properly set-up database, meaning:
							 (User, Subreddit, ActiveSubreddit) tables. 
 start_sub    - name of the subreddit (its url stub, technically) to start on. Assumes the list is 
                sorted alphabetically. 

side-effects:
  Fills the database tables with the retrieved users, and their active subreddits using queries
  limited to the year provided. The initial list of users is found by scraping the authors of
  comments of submissions in subreddits listed in the subreddit_df.

"""
def collectUsersAndActiveSubreddits(subreddit_df, 
									year, 
									user_N, 
									active_N, 
									reddit_obj, 
									db_conn, 
									start_sub=None):
	START_TS = int(time.mktime(datetime.date(year, 1,   1).timetuple()))
	END_TS   = int(time.mktime(datetime.date(year, 12, 30).timetuple()))

	psapi  = PushshiftAPI(reddit_obj)
	skipping = (not start_sub == None)

	# Collecting subreddits and users
	for sub_row in subreddit_df.iterrows():
		sub_url  = sub_row[1]['subreddit url']
		sub_name = sub_row[1]['subreddit name']

		if skipping:
			if sub_url == start_sub:
				skipping = False
			else:
				continue # Skip this row. 
		try:
			sub_obj  = reddit_obj.subreddit(sub_url[3:])
			sub_id   = sub_obj.id  # Takes a full query with praw :(

			## Add subreddit to table.
			SUB_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.subreddit 
										(subreddit_id, subreddit_name, subreddit_url) 
										VALUES (\'{}\', \'{}\', \'{}\');"""

			with db_conn.cursor() as cursor:
				cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_url))
			db_conn.commit()
			print("processing: {}".format(sub_url))

			## Finding 'Active Users' in subreddit by looking at comments. First PushShift Query
			sc_cache = []
			source_comments = psapi.search_comments(after=START_TS,
	                                     		 before=END_TS,
	                                     		 subreddit=sub_name)
	                                     		# limit=SOURCE_COMMENTS_CHECKED)
			for c in source_comments:
				sc_cache.append(c)
				if len(sc_cache) >= user_N: break

			USER_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.user 
											(user_id, user_name, home_subreddit) 
										VALUES 
											(\'{}\', \'{}\', \'{}\');"""

			user_count = 0
			comments_checked = 0
			comments_skipped = 0

			user_set = {}
			# Note: Do a first pass to fill in this set. I think it needs to be a dict? Youre
			#       saving a praw object. key is the user id, value is the praw object. 
			#       see notes in weekly write up. 

			for comment in sc_cache:
				## Attempt to add user to the database
				try:
					with db_conn.cursor() as cursor:
						cursor.execute(USER_INS_SQL.format(comment.author.id, 
														   comment.author, 
														   sub_id))
					db_conn.commit()

					## Add 'Active Subreddits' by looking at the comments made by the author
					uc_cache = []
					user_comments = psapi.search_comments(after=START_TS,
			                                     		  before=END_TS,
			                                     		  author=comment.author)
			                                     		 # limit=NUM_COMMENTS_CHECKED)
					for c in user_comments:
						uc_cache.append(c)
						if len(uc_cache) >= active_N: break

				except KeyboardInterrupt:
						print("USER KeyboardInterrupt")
						sys.exit()

				except Exception as e:
					print(" -- Skipped user due to: {}".format(e))
					pass

				AS_INS_SQL = """INSERT IGNORE INTO reddit_location_bias.active_subreddits
									(user_id, subreddit_id, year, unique_hash)
								VALUES
									(\'{}\', \'{}\', {} ,\'{}\');"""

				for user_comment in uc_cache:
					# first we ignore add the sub
					try:
						with db_conn.cursor() as cursor:
							cursor.execute(SUB_INS_SQL.format(user_comment.subreddit_id, 
															  user_comment.subreddit, 
															  user_comment.subreddit))
						db_conn.commit()

						# now the AS
						with db_conn.cursor() as cursor:
							unique_hash = "{}{}{}".format(comment.author.id, 
													   	  user_comment.subreddit_id[3:], 
													   	  year,)
							cursor.execute(AS_INS_SQL.format(comment.author.id, 
														   	 user_comment.subreddit_id[3:], 
														   	 year,
														   	 unique_hash))
						db_conn.commit()
						comments_checked += 1

					except KeyboardInterrupt:
						print("USER KeyboardInterrupt")
						sys.exit()

					except Exception as e:
						comments_skipped += 1
				user_count += 1

			print(" - added {} users".format(user_count))
			print(" - checked {} comments".format(comments_checked))
			print(" - skipped {} comments".format(comments_skipped))

		except Exception as e:
			print("exception: {}, {}".format(sub_row[1]['subreddit url'], e))
