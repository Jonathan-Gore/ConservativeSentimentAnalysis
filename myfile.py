### API Date Range Ingestion Engine ###
## /r/conservative sentiment analysis ##

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

##### MAKE SURE TO CHANGE THE CSV FILE NAME AFTER EACH ROUND OR FACE LOSING HOURS OF API CALL DATA#####

## 2:20am 8/22/2021
# Currently finished the API date ingenstion engine, as off last check there may be a problem with sometimes
# not successfully retrieving comments, but it could just be some over calling the API today in mass
# will need to stress test after my IP address has "cooled down" a bit


import datetime as dt
from pmaw import PushshiftAPI
import pandas as pd
import time

from datetime import datetime, timedelta

## Calls the Pushshift.io API for a specific Subreddit and grabs all comments within a user supplied date range
# converts the datetime float class of the 'before' and 'after' parameters into integers (required for api.search.comments())
# calls pmaw api.search.comments() for user inputed subreddit, limit, and date range(start, stop, step)
# num_workers is 4x how many logical processors you CPU has for optimized multithreading
# mem_safe is for not overloading RAM while calling huge numbers of comment data from the Reddit API

def pushshift_api_call(subreddit, before, after, limit = 10000000):
    api = PushshiftAPI()

    print(str(before) + " " + str(after))    
    before = int(before)
    after = int(after)
    print(str(before) + " " + str(after))
    
    comments = api.search_comments(subreddit=subreddit, limit=limit, before=before, after=after, num_workers = 50, mem_safe = True)
    print(f'Retrieved {len(comments)} comments from Pushshift')
    comments_df = pd.DataFrame(comments)
	#preview the comments data
    comments_df.head(5)
    comments_df.to_csv('./conservative_comments' + datetime.fromtimestamp(before).strftime("%Y%b%d") +
                        "_" + datetime.fromtimestamp(after).strftime("%Y%b%d") + '.csv',
                        header=True, index=False, columns=list(comments_df.axes[1]))


## Just for fun, ended up not being super useful, keeping it because of how short it is!

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


## Example Call:
## NewList = list(date_range('20150101', '20150228', 4))
# This function ingests dates in the str form 'yyyymmdd' which is formally recognized by datetime as "%Y%m%d"
# it takes these ingested dates and finds the difference between them and divides it by the intervals selected
# then the start date is iterated forward by the differece date multiplied by the current interval iteration (diff * interval)
# each individual iteration through the dates is then yielded outward to be written to some external variable/dataframe/list
# yield is used so this function can stay lightweight and continue operating per each iteration instead of having to write
# a variable internally

def date_range(start, end, intv):
    start = datetime.strptime(start,"%Y%m%d")
    end = datetime.strptime(end,"%Y%m%d")
    diff = (end  - start ) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y%m%d")
    yield end.strftime("%Y%m%d")


## Converts a list of dates formated '%Y%m%d' into a list of unix times
# Very similar to the date_range() function, but converts dates to unix date format
# I seperated this out from date_rangeso more datetime operations could be applied to
# non-timestamped converted dates if the user prefers

def date2timestamp(DateList):
    TimeStampedDateList = []
    for i in range(len(DateList)):
        TimeStampedDateList.append(int(time.mktime(dt.datetime.strptime(DateList[i], "%Y%m%d").timetuple())))
    return TimeStampedDateList


def pushshiftAPIController(subreddit, before, after, interval, limit = 10000000):
    print("debugging date formats, entering function as: " + before + " " + after)
    DateList = list(date2timestamp(list(date_range(before, after, interval))))
    print("debugging date formats, returning from date2timestamp() function as: " + str(list(DateList)))
    try:
        for i in range(len(DateList)):
            pushshift_api_call(subreddit, DateList[i], DateList[i+1], limit)
            print("Finished Date Iteration " + str(DateList[i]) + " - " + str(DateList[i+1]))
    except IndexError:
        print("List index out of range, but not an actual problem. Just practicing error handling in Python")
        


if __name__ == '__main__':
    beforeDate = '20200215'
    afterDate = '20200101'
    pushshiftAPIController("conservative", beforeDate, afterDate, interval = 5, limit = 100)