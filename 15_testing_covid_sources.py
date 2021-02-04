import CountyUsers as cu
import pandas as pd
import praw
from psaw import PushshiftAPI
import time
import datetime as dt
import pymysql as sql
import progressbar
import requests

TARGET_STATE = 'new jersey'
FN_OUT = './data/testing_covid.csv'

# This gets us a set of boudns for each week to make it easy to test.
first_days = list(pd.date_range('2020-01-01', '2020-12-31', freq='W-WED'))
a_week   = pd.Timedelta(value=7, unit="days")

week_bounds = []
week_num = 1
for first_day in first_days:
	start_ts = int(first_day.timestamp())
	end_ts   = int((first_day+a_week).timestamp())
	week_bounds.append((start_ts, end_ts, week_num))
	week_num += 1

def get_week_num(bounds, date_str):
	test_ts = int(dt.datetime.strptime(date_str, "%m/%d/%y").timestamp())
	for w in bounds:
		d_min, d_max, w_num = w
		if( (test_ts < d_max) and (test_ts >= d_min) ):
			return w_num

	return -1

# So lets just make a dataframe? save it? Check it out? 
raw_data = []
get_ind_state = "https://corona.lmao.ninja/v2/historical/usacounties/{}?lastdays={}"
res_counties  = requests.get(get_ind_state.format(TARGET_STATE, 'all'))
county_data = res_counties.json()

for county in county_data:
	county_name = county['county']

	# Collections to save totals
	# Note - we are not ADDING to these, we replace if larger. They are cumulative values. 
	weekly_deaths = [0 for x in range(52)] 
	weekly_cases  = [0 for x in range(52)]

	for date_update in county['timeline']['deaths']:
		week_num = get_week_num(week_bounds, date_update)
		deaths   = county['timeline']['deaths'][date_update]
		if not (week_num > 0 and week_num <= 52):
			continue
		if deaths > weekly_deaths[week_num-1]:
			weekly_deaths[week_num-1] = deaths

	for date_update in county['timeline']['cases']:
		week_num = get_week_num(week_bounds, date_update)
		cases   = county['timeline']['cases'][date_update]
		if not (week_num > 0 and week_num <= 52):
			continue
		if cases > weekly_cases[week_num-1]:
			weekly_cases[week_num-1] = cases

	for i in range(52):
		# i represents the week_num-1, so i=0 == week 1
		new_row = [TARGET_STATE, county_name, i+1, weekly_deaths[i], weekly_cases[i]]
		raw_data.append(new_row)

cols = ['state', 'county name', 'week', 'cumulative deaths', 'cumulative cases']

df = pd.DataFrame(data=raw_data, columns=cols)
df.to_csv(FN_OUT)





