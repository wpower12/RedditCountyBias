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

from .Tools import splitUsersIntoCohorts


def cohortCollectYD(cohort, start_date, max_response, active_N, user_cohort_size, db_conn):
	import warnings
	warnings.simplefilter("ignore")

	# print(" -- {}th cohort -- ".format(cohort[0]))
	
	# Getting Time stamps.
	START_TS = int(start_date.timestamp())
	a_day   = pd.Timedelta(value=1, unit="days")
	END_TS   = int((start_date+a_day).timestamp())

	year = start_date.year
	day = start_date.dayofyear

	psapi  = PushshiftAPI()

	# Need to create the 'common query' for pushshift, which means
	# getting a comma separated list of the subreddit names. 
	sub_name_list = ""
	for c in cohort[1]:
		sub_name = c[1]
		sub_name_list += "{}, ".format(sub_name)
	sub_name_list = sub_name_list[:-2]
	
	submission_cache   = []
	source_submissions = psapi.search_submissions(after=START_TS, 
												before=END_TS,
												subreddit=sub_name_list,
												size=500)

	for c in source_submissions:
		submission_cache.append(c)
		if len(submission_cache) >= max_response: break

	if len(submission_cache) == 0:
		print("- no users found in cohort")
		return

	user_set = set() # For each submission, we need to track (sub_id, user_id, user_name)
	for submission in submission_cache:
		try:
			sub_id    = submission.subreddit_id[3:]
			sub_name  = submission.subreddit
			user_name = submission.author
			user_id   = submission.author_fullname[3:]
			user_set.add((user_id, user_name, sub_id, sub_name))
		except Exception as e:
			# Catches banned users/reddits. 
			pass

	if len(user_set) == 0:
		print("- no searchable users found")
		return
	else:
		print("- {} in userset".format(len(user_set)))

	# Before we process the users into cohorts and look for AS,
	# we add all the users to the db.
	user_list = [] 
	user_map  = {}
	for user in user_set:
		u_id, u_name, s_id, s_name = user
		USER_INS_SQL = """INSERT INTO reddit_data.useryd 
								(user_reddit_id, user_reddit_name, home_subreddit, year, day) 
						  VALUES 
								(\'{}\', \'{}\', \'{}\', {}, {});"""
		try:
			with db_conn.cursor() as cursor:
				cursor.execute(USER_INS_SQL.format(u_id, u_name, s_id, year, day))
				useryd_id = cursor.lastrowid # Get id of the recently added useryw.
			db_conn.commit()

			user_list.append((useryd_id, u_name))
			user_map[u_name] = useryd_id  # Save these to make inserting the AS easier.

		except Exception as e:
			print(e)
			pass

	print("-- splitting {} users into cohorts".format(len(user_list)))
	user_cohorts = splitUsersIntoCohorts(user_list, user_cohort_size)
	c_count  = 0
	as_count = 0
	cohort_bar = progressbar.ProgressBar(max_value=len(user_cohorts), redirect_stdout=True)
	for user_cohort in user_cohorts:
		author_list = ""
		for u in user_cohort:
			u_id, u_name = u
			author_list += "{}, ".format(u_name)
		author_list = author_list[:-2]

		comment_cache = []
		cohort_comments = psapi.search_comments(after=START_TS, 
												before=END_TS,
												author=author_list,
												size=500)

		for c in cohort_comments:
			comment_cache.append(c)
			if len(comment_cache) >= 500: break

		if len(comment_cache) == 0:
			continue

		for comment in comment_cache:
			try:
				# Just need info for an IGNORE INSERT of a sub, and an AS
				uyd_id   = user_map[comment.author]
				sub_id   = comment.subreddit_id[3:]
				sub_name = comment.subreddit

				# INSERT IGNORE the sub. 
				SUB_INS_SQL = """INSERT IGNORE INTO reddit_data.subreddit 
									(subreddit_id, subreddit_name, subreddit_url) 
								 VALUES 
								 	(\'{}\', \'{}\', \'{}\');"""
				with db_conn.cursor() as cursor:
					cursor.execute(SUB_INS_SQL.format(sub_id, sub_name, sub_name))
				db_conn.commit()

				# Now the AS
				AS_INS_SQL = """INSERT IGNORE INTO reddit_data.activesubreddit_yd
									(useryd_id, subreddit_id)
								VALUES
									({}, \'{}\');"""
				with db_conn.cursor() as cursor:
					cursor.execute(AS_INS_SQL.format(uyd_id, 
												   	 sub_id))
				db_conn.commit()

				as_count += 1

			except Exception as e:
				print(" error in AS comment; {}".format(e))
		c_count += 1
		cohort_bar.update(c_count)
	cohort_bar.finish()
	print("-- {} Active Subreddits found".format(as_count))

