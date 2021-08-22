### /r/conservative sentiment analysis ###

# Date ranges will be hard-coded into the function for now for ease of tinkering under the "before"
# and "after" local variables
##before = int(dt.datetime(2021,1,1,0,0).timestamp())
##after = int(dt.datetime(2015,6,18,0,0).timestamp())
# Jan 1 2021 is before the insurrection and while Trump still is president
# June 18th 2015 was one day after Trump's first presidential rally (Manchester Community College in New Hampshire)
# Between these two dates it is reasonable to infer some level of "unbiased" /r/conservative rhetoric
# about countries to invade

# 'limit' will currently be hard-coded in at 100,000 to follow the medium tutorial (https://medium.com/swlh/how-to-scrape-large-amounts-of-reddit-data-using-pushshift-1d33bde9286)
# but in future may need to be raised considerably

# Best to iterate through different date ranges to avoid ~40 hours of continuous processing
# By 6 months is far too large, considering


##### MAKE SURE TO CHANGE THE CSV FILE NAME AFTER EACH ROUND OR FACE LOSING HOURS OF API CALL DATA#####

###
## Round 1 - Jan 1 2021 -> July 1st 2020
# before = int(dt.datetime(2021,1,1,0,0).timestamp())
# after = int(dt.datetime(2020,7,1,0,0).timestamp())
###

###
## Round 2 - June 30th 2020 -> Jan 2nd 2020
# before = int(dt.datetime(2020,6,30,0,0).timestamp())
# after = int(dt.datetime(2020,1,2,0,0).timestamp())
###

###
## Round 3 - Jan 1 2020 -> July 1st 2019
# before = int(dt.datetime(2020,1,1,0,0).timestamp())
# after = int(dt.datetime(2019,7,1,0,0).timestamp())
###

###
## Round 4 - June 30th 2019 -> Jan 2nd 2019
# before = int(dt.datetime(2019,6,30,0,0).timestamp())
# after = int(dt.datetime(2019,1,2,0,0).timestamp())
###

###
## Round 5 - Jan 1 2019 -> July 1st 2018
# before = int(dt.datetime(2019,1,1,0,0).timestamp())
# after = int(dt.datetime(2018,7,1,0,0).timestamp())
###

###
## Round 6 - June 30th 2018 -> Jan 2nd 2018
# before = int(dt.datetime(2018,6,30,0,0).timestamp())
# after = int(dt.datetime(2018,1,2,0,0).timestamp())
###

###
## Round 7 - Jan 1 2018 -> July 1st 2017
# before = int(dt.datetime(2018,1,1,0,0).timestamp())
# after = int(dt.datetime(2017,7,1,0,0).timestamp())
###

###
## Round 8 - June 30th 2017 -> Jan 2nd 2017
# before = int(dt.datetime(2017,6,30,0,0).timestamp())
# after = int(dt.datetime(2017,1,2,0,0).timestamp())
###

###
## Round 9 - Jan 1 2017 -> July 1st 2016
# before = int(dt.datetime(2017,1,1,0,0).timestamp())
# after = int(dt.datetime(2016,7,1,0,0).timestamp())
###

###
## Round 10 - June 30th 2016 -> Jan 2nd 2016
# before = int(dt.datetime(2016,6,30,0,0).timestamp())
# after = int(dt.datetime(2016,1,2,0,0).timestamp())
###

###
## Round 11 - Jan 1 2016 -> June 18th 2015
# before = int(dt.datetime(2016,1,1,0,0).timestamp())
# after = int(dt.datetime(2015,6,18,0,0).timestamp())
###

##### MAKE SURE TO CHANGE THE CSV FILE NAME AFTER EACH ROUND OR FACE LOSING HOURS OF API CALL DATA#####


import datetime as dt
from pmaw import PushshiftAPI
import pandas as pd

from datetime import date, timedelta

def pushshift_api_call(subreddit):
    api = PushshiftAPI()
    before = int(dt.datetime(2021,1,1,0,0).timestamp())
    after = int(dt.datetime(2020,7,1,0,0).timestamp())
    limit = 3000000
    comments = api.search_comments(subreddit=subreddit, limit=limit, before=before, after=after, num_workers = 60, mem_safe = True)
    print(f'Retrieved {len(comments)} comments from Pushshift')
    comments_df = pd.DataFrame(comments)
	#preview the comments data
    comments_df.head(5)
    comments_df.to_csv('./conservative_comments_round_1.csv', header=True, index=False, columns=list(comments_df.axes[1]))

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

if __name__ == '__main__':
    pushshift_api_call("conservative")
    #start_date = date(2013, 1, 1)
    #end_date = date(2015, 6, 2)
    #DateList = []
    #DateList = [single_date.strftime("%Y-%m-%d") for single_date in daterange(start_date, end_date)]
    #DateList = [int(dt.datetime(single_date).timestamp()) for single_date in daterange(start_date, end_date)]