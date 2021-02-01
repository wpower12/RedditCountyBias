import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime
import pymysql as sql
import urllib
import json
import xmltodict
from progress.bar import Bar
import progressbar
from datetime import date, timedelta


def estimateWeeklyThroughput(sub_name, psapi, weeks, max_responses):
	num_weeks = len(weeks)*1.0
	counts = []

	for w in weeks:
		START_TS = int(w.timestamp())
		a_week   = pd.Timedelta(value=7, unit="days")
		END_TS   = int((w+a_week).timestamp())

		sc_cache = []
		source_comments = psapi.search_comments(after=START_TS,
	                                     		 before=END_TS,
	                                     		 subreddit=sub_name)

		for c in source_comments:
			sc_cache.append(c)
			if len(sc_cache) >= max_responses: break

		counts.append(len(sc_cache))

	count_s = pd.Series(counts)

	return count_s.mean(), count_s.var()



"""
inputs:
 subreddit_df - Pandas df of the subreddits to process. Assumed ordered alphabetiacally.
 year         - Year of users/comments to process.
 week         - week
 user_N       - number of source comments to process for each subreddit when finding users.
 active_N     - number of source comments to process for each user when finding active subreddits
 db_conn      - A pymysql connection to a properly set-up database, meaning:
							 (User, Subreddit, ActiveSubreddit) tables. 

side-effects:
  Fills the database tables with the retrieved useryw's, and their activesubreddits using queries
  limited to the year and week provided. 
"""
def collectUserYWsAndActiveSubreddits(subreddit_df, start_date, user_N, active_N, db_conn):
	import warnings
	warnings.simplefilter("ignore")

	# Assuming that the start_date is actually a pandas TimeStamp object
	START_TS = int(start_date.timestamp())
	a_week   = pd.Timedelta(value=7, unit="days")
	END_TS   = int((start_date+a_week).timestamp())

	year = start_date.year
	week = start_date.week

	psapi  = PushshiftAPI()

	# Collecting subreddits and users
	sub_bar = progressbar.ProgressBar(max_value=len(subreddit_df), redirect_stdout=True)
	sub_bar.start()
	i = 0
	for sub_row in subreddit_df.iterrows():
		sub_state = sub_row[1]['state']
		sub_url   = sub_row[1]['subreddit url']
		sub_name  = sub_row[1]['subreddit name']
		sub_id    = sub_row[1]['subreddit id']

		try:
			## Finding 'Active Users' in subreddit by looking at comments. First PushShift Query
			sc_cache = []
			source_comments = psapi.search_comments(after=START_TS,
	                                     		 before=END_TS,
	                                     		 subreddit=sub_name)

			for c in source_comments:
				sc_cache.append(c)
				if len(sc_cache) >= user_N: break
			
			print("processing: {}, {}, {}".format(start_date, sub_state, sub_url))
			if len(sc_cache) == 0:
				continue

			# Use one of the comments to get the subreddit id for insertion.
			sub_id = sc_cache[0].subreddit_id[3:]

			## Add subreddit to table.
			SUB_INS_SQL = """INSERT IGNORE INTO reddit_data.subreddit 
										(subreddit_id, subreddit_name, subreddit_url) 
										VALUES (\'{}\', \'{}\', \'{}\');"""

			with db_conn.cursor() as cursor:
				cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_url))
			db_conn.commit()

			# Do a pass over the cache to retrieve the set of unique comment authors, the 'users'.
			user_set = set()
			for comment in sc_cache:
				try:
					c_author    = comment.author
					c_author_id = comment.author_fullname[3:]
					user_set.add((c_author_id, c_author))
				except Exception as e:
					# Catches the deleted/banned users. 
					pass

			# Now we process each user to find their 'active subreddits' in the same time period.
			user_count = 0
			comments_checked = 0
			comments_skipped = 0
			for user in user_set:
				## Attempt to add user to the database
				author_id, author_name = user


				USER_INS_SQL = """INSERT INTO reddit_data.useryw 
											(user_reddit_id, user_reddit_name, home_subreddit, year, week) 
									VALUES 
											(\'{}\', \'{}\', \'{}\', {}, {});"""
				try:
					with db_conn.cursor() as cursor:
						cursor.execute(USER_INS_SQL.format(author_id, 
														   author_name, 
														   sub_id,
														   year,
														   week))
						useryw_id = cursor.lastrowid # Get id of the recently added useryw.
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
					pass

				AS_INS_SQL = """INSERT IGNORE INTO reddit_data.activesubreddit
									(useryw_id, subreddit_id)
								VALUES
									({}, \'{}\');"""

				for user_comment in uc_cache:
					try:
						# first we ignore add the sub
						with db_conn.cursor() as cursor:
							cursor.execute(SUB_INS_SQL.format(user_comment.subreddit_id[3:], 
															  user_comment.subreddit, 
															  user_comment.subreddit))
						db_conn.commit()

						# now the AS
						with db_conn.cursor() as cursor:
							cursor.execute(AS_INS_SQL.format(useryw_id, 
														   	 user_comment.subreddit_id[3:]))
						db_conn.commit()

					except Exception as e:
						pass

		except Exception as e:
			# print("exception: {}, {}".format(sub_row[1]['subreddit url'], e))
			pass
		finally:
			i += 1
			sub_bar.update(i)

	sub_bar.finish()

def collectUsersAndActiveSubreddits(subreddit_df, year, user_N, active_N, db_conn, start_sub=None):
	START_TS = int(time.mktime(datetime.date(year, 1,   1).timetuple()))
	END_TS   = int(time.mktime(datetime.date(year, 12, 30).timetuple()))

	psapi  = PushshiftAPI()
	skipping = (not start_sub == None)

	# Collecting subreddits and users
	for sub_row in subreddit_df.iterrows():
		sub_state = sub_row[1]['state']
		sub_url   = sub_row[1]['subreddit url']
		sub_name  = sub_row[1]['subreddit name']

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
			
			print("processing: {}, {}".format(sub_state, sub_url))
			if len(sc_cache) == 0:
				print(" - zero posts found for mining users.")
				continue

			# Use one of the comments to get the subreddit id for insertion.
			sub_id = sc_cache[0].subreddit_id[3:]

			## Add subreddit to table.
			SUB_INS_SQL = """INSERT IGNORE INTO reddit_data.subreddit 
										(subreddit_id, subreddit_name, subreddit_url) 
										VALUES (\'{}\', \'{}\', \'{}\');"""

			with db_conn.cursor() as cursor:
				cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_url))
			db_conn.commit()

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

def get_county(query):
    # initial search
    # http://api.geonames.org/searchJSON?q=long%20beach%20island%20nj&maxRows=10&country=US&username=wpower3dabi
    query = query.replace(' ', '%20')
    search_url = "http://api.geonames.org/searchJSON?q={}&country=US&username=wpower3dabi&maxRows=10".format(query)
    
    geoname_id = -1 
    with urllib.request.urlopen(search_url) as response:
        results = json.loads(response.read())
        
        try:
            if len(results['geonames']) > 0:
                geoname_id = results['geonames'][0]['geonameId']
            else:
                return None
        except:
            return None
        
    # Get request
    get_query_url = "http://api.geonames.org/get?geonameId={}&username=wpower3dabi".format(geoname_id)
    with urllib.request.urlopen(get_query_url) as response:
        results = xmltodict.parse(response.read())
        
        try:
            if results['geoname']:
                county = results['geoname']['adminName2']
            else:
                return None
        except:
            return None
    
    # process
    return county