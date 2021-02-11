import pandas as pd
from psaw import PushshiftAPI
import time
import datetime

api = PushshiftAPI()
date_after = "12/01/2020"
TS_AFTER = int(time.mktime(datetime.datetime.strptime(date_after, "%d/%m/%Y").timetuple()))

print(TS_AFTER)

# None of these work. 
# res = api.search_comments(author='willpower12', after=TS_AFTER, aggs='subreddit')
# res = api.search_submissions(author='willpower12', aggs='subreddit')
# res = api.redditor_subreddit_activity(author='willpower12')

# print(next(res))

# I GUESS its good news that these don't work now. Otherwise all the work ive done would be
# a waste. But then again. Fuck these would have done the whole thing in one afternoon.