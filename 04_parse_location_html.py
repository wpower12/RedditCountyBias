import pandas as pd
import json
import urllib.request
import xmltodict
import time
from bs4 import BeautifulSoup as bs

OUTPUT_FN = './data/locationsubs.csv'

subreddit_list_fo = open('./data/reddit_NA_location_sr_list.html', 'r')
sr_list_html = subreddit_list_fo.read()
subreddit_list_fo.close()

srl_soup = bs(sr_list_html, 'html.parser')
table_headers = srl_soup.find_all('h5')

subs_by_area = {} # dict; keys - name of geo area, value - list of subreddits

for table_header in table_headers:
	geo_area_name = table_header.contents[0]
	table = table_header.find_next_sibling('table').find('tbody')

	subs_by_area[geo_area_name] = []

	for sub_row in table.find_all('tr'):
		for a_obj in sub_row.find_all('a', href=True):
			sub_str  = a_obj['href']
			sub_name = a_obj.contents[0]
			new_item = (sub_str, sub_name)
			subs_by_area[geo_area_name].append(new_item)

state_names = ["Alaska", 
               "Alabama", 
               "Arkansas", 
               "American Samoa", 
               "Arizona", 
               "California", 
               "Colorado", 
               "Connecticut", 
               "District of Columbia", 
               "Delaware", 
               "Florida", 
               "Georgia", 
               "Guam", 
               "Hawaii", 
               "Iowa", 
               "Idaho", 
               "Illinois", 
               "Indiana", 
               "Kansas", 
               "Kentucky", 
               "Louisiana", 
               "Massachusetts", 
               "Maryland", 
               "Maine", 
               "Michigan", 
               "Minnesota", 
               "Missouri", 
               "Mississippi", 
               "Montana", 
               "North Carolina", 
               "North Dakota",
               "Nebraska", 
               "New Hampshire", 
               "New Jersey", 
               "New Mexico", 
               "Nevada", 
               "New York",
               "Ohio", 
               "Oklahoma", 
               "Oregon", 
               "Pennsylvania", 
               "Puerto Rico", 
               "Rhode Island", 
               "South Carolina", 
               "South Dakota", 
               "Tennessee", 
               "Texas", 
               "Utah", 
               "Virginia", 
               "Virgin Islands", 
               "Vermont", 
               "Washington", 
               "Wisconsin", 
               "West Virginia", 
               "Wyoming"]

# lets just count up the number of geo names we have that are also in the list of usa 'states'
subs_by_state = {}

state_count = 0
location_count = 0
for key in subs_by_area:
    if key in state_names:
        state_count += 1
        location_count += len(subs_by_area[key])
        subs_by_state[key] = subs_by_area[key]

raw_data_rows = []
for state in subs_by_state:
    for subreddit in subs_by_state[state]:
        new_item = [state, subreddit[0], subreddit[1]]
        raw_data_rows.append(new_item)

df = pd.DataFrame(data=raw_data_rows)
df.columns = ['state', 'subreddit url', 'subreddit name']
df.to_csv(OUTPUT_FN)
print("{} large geo areas, {} county level geo areas".format(df['state'].nunique(), len(df))) 
print("saved to {}".format(OUTPUT_FN))