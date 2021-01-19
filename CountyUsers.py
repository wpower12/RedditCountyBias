import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
from progress.bar import Bar

"""
inputs:
 subreddit_df - Pandas df of the subreddits to process. Assumed ordered alphabetiacally.
 start_sub    - name of the sub to start with - NOT IMPLEMENTED YET.
 year         - Year of users/comments to process.
 user_N       - number of source comments to process for each subreddit when finding users.
 active_N     - number of source comments to process for each user when finding active subreddits
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
									db_conn, 
									start_sub=None):
	START_TS = int(time.mktime(datetime.date(year, 1,   1).timetuple()))
	END_TS   = int(time.mktime(datetime.date(year, 12, 30).timetuple()))

	# psapi  = PushshiftAPI(reddit_obj)

	psapi  = PushshiftAPI()
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
			## Finding 'Active Users' in subreddit by looking at comments. First PushShift Query
			sc_cache = []
			source_comments = psapi.search_comments(after=START_TS,
	                                     		 before=END_TS,
	                                     		 subreddit=sub_name)
	                                     		# limit=SOURCE_COMMENTS_CHECKED)
			for c in source_comments:
				sc_cache.append(c)
				if len(sc_cache) >= user_N: break

			# Use one of the comments to get the subreddit id for insertion.
			sub_id = sc_cache[0].subreddit_id[3:]

			## Add subreddit to table.
			SUB_INS_SQL = """INSERT IGNORE INTO reddit_data.subreddit 
										(subreddit_id, subreddit_name, subreddit_url) 
										VALUES (\'{}\', \'{}\', \'{}\');"""

			with db_conn.cursor() as cursor:
				cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_url))
			db_conn.commit()
			print("processing: {}".format(sub_url))

			# Do a pass over the cache to retrieve the set of unique comment authors, the 'users'.
			user_set = set()
			for comment in sc_cache:
				# print(comment)
				try:
					c_author    = comment.author
					c_author_id = comment.author_fullname[3:]
					user_set.add((c_author_id, c_author))
				except Exception as e:
					# Catches the deleted users. 
					pass

			# Now we process each user to find their 'active subreddits' in the same time period.
			user_count = 0
			comments_checked = 0
			comments_skipped = 0
			print(" - {} in user set".format(len(user_set)))
			sub_pbar = Bar(" - users", max=len(user_set))
			for user in user_set:
				## Attempt to add user to the database
				author_id, author_name = user
				USER_INS_SQL = """INSERT IGNORE INTO reddit_data.user 
											(user_id, user_name, home_subreddit) 
									VALUES 
											(\'{}\', \'{}\', \'{}\');"""
				try:
					with db_conn.cursor() as cursor:
						cursor.execute(USER_INS_SQL.format(author_id, 
														   author_name, 
														   sub_id))
					db_conn.commit()

					## Add 'Active Subreddits' by looking at the comments made by the author
					uc_cache = []
					user_comments = psapi.search_comments(after=START_TS,
			                                     		  before=END_TS,
			                                     		  author=author_name)
					for c in user_comments:
						uc_cache.append(c)
						if len(uc_cache) >= active_N: break

				except Exception as e:
					print(" -- Skipped user due to: {}".format(e))
					pass

				AS_INS_SQL = """INSERT IGNORE INTO reddit_data.active_subreddits
									(user_id, subreddit_id, year, unique_hash)
								VALUES
									(\'{}\', \'{}\', {} ,\'{}\');"""

				for user_comment in uc_cache:
					# first we ignore add the sub
					try:
						with db_conn.cursor() as cursor:
							cursor.execute(SUB_INS_SQL.format(user_comment.subreddit_id[3:], 
															  user_comment.subreddit, 
															  user_comment.subreddit))
						db_conn.commit()

						# now the AS
						with db_conn.cursor() as cursor:
							unique_hash = "{}{}{}".format(author_id, 
													   	  user_comment.subreddit_id[3:], 
													   	  year,)
							cursor.execute(AS_INS_SQL.format(author_id, 
														   	 user_comment.subreddit_id[3:], 
														   	 year,
														   	 unique_hash))
						db_conn.commit()
						comments_checked += 1

					except Exception as e:
						comments_skipped += 1
				user_count += 1
				sub_pbar.next()
			sub_pbar.finish()
			print(" - added {} users".format(user_count))
			print(" - checked {} comments".format(comments_checked))
			print(" - skipped {} comments".format(comments_skipped))

		except Exception as e:
			print("exception: {}, {}".format(sub_row[1]['subreddit url'], e))
