import urllib
import json
import xmltodict
from progress.bar import Bar
import progressbar

""" get_county
inputs:
 query - The geo query to run. Usually the concatenation of 'State'+'Location Name'

outputs:
 county - name of the found US county. None if no county is found or if the query errors out. 
"""
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



""" splitUsersIntoCohorts
Utility method for the cohort collect. Splits a list of users into 
equally sized cohorts. These are then used for the batch comment
processing where the AS are collected for each user. 
"""
def splitUsersIntoCohorts(users, cohort_size):
	cohorts = []
	curr_cohort = []
	for u in users:
		curr_cohort.append(u)
		if len(curr_cohort) >= cohort_size:
			cohorts.append(curr_cohort)
			curr_cohort = []
	if(len(curr_cohort) > 0):
		cohorts.append(curr_cohort)
	return cohorts
