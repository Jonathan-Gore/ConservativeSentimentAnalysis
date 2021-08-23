#### Comment Engine ####
## Engine for ingesting Reddit comment data and applying Sentiment Analysis to it ##

###8/23/2021####
# Currently need to add an additional step to the cleanDataFrame() function for dropping empty cells
# Seems to occur with deleted authors. Right now the main query is getting hung up on the empty cells
# - Jonathan

import pandas as pd
import numpy as np
from API_DRIE import date_range, date2timestamp
from pandas import read_csv, DataFrame

import os
from os import listdir
from os.path import isfile, join

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#note: depending on how you installed (e.g., using source code download versus pip install), you may need to import like this:
#from vaderSentiment import SentimentIntensityAnalyzer
#Compound score > 0.05 is positive, 0.05 - -0.05 neutral, < -0.05 negative

## Function for ingesting csv data
# First it looks through a userinputed path -- in future would like to make this a base directory of the script
# Then it finds only the csv files and creates a list
# Finally it combines all of the indivdual csv files into one large pandas dataframe
# or in to a dictionary of all of the smaller dataframes, still deciding how this works

def comment2MasterDataframe(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and os.path.splitext(f)[1] == '.csv']
    CommentDataframes = (pd.read_csv(f) for f in onlyfiles)
    MasterDataframe = pd.concat(CommentDataframes, ignore_index=True)
    # Turns out this an issue with how excel reads python
    # "â€™" is not found in the base txt file
    #MasterDataframe = MasterDataframe.replace(to_replace = "â€™", value = 'TEST REPLACE')

    return MasterDataframe

def countWordFrequency(commentDataframe):
    ## This looks interesting, come back to it
    #freq = commentDataframe.groupby(['bodyclean']).count() 
    print("creating new_df and counting word frequencies")
    new_df = commentDataframe.bodyclean.str.split(expand=True).stack().value_counts().reset_index()
    print("adding 'word' and 'frequency' columns to new_df")
    new_df.columns = ['Word', 'Frequency']
    return new_df

## This fucking worked the first god damn time, LET'S GO!!!
def cleanDataframe(CommentDataframe):
    # create a new column with body text stripped of punctuation and all lowercase
    CommentDataframe["bodyclean"] = CommentDataframe['body'].str.lower().str.replace('[^\w\s]','')
    # replace all "deleted" comments with NaN to be dropped shortly
    CommentDataframe["bodyclean"].replace('deleted', np.nan, inplace=True)
    # Drop all NaN values (previously "deleted" comments), cleaning up the dataset
    CommentDataframe.dropna(subset=["bodyclean"], inplace=True)

    return CommentDataframe

## This works!
def appendSentimentScore(CleanCommentDF):
    comments = CleanCommentDF["body"]
    analyzer = SentimentIntensityAnalyzer()
    vs_list = []
    
    for comment in comments:
        vs = analyzer.polarity_scores(str(comment))
        vs_list.append(vs["compound"])
        print("{:-<65} {}".format(comment, str(vs['compound'])))
    #    #print(CleanMasterCSVtest['id'])
    
    CleanCommentDF["Sentiment Score"] = vs_list
    return CleanCommentDF

## This function searches each 'bodyclean' of the Clean and Scored Master comment csv
# parses the comment into individual words and writes a new column to the user inputted keyword/value.
# If the value/word is present then '1' is record in the new column, if not the '0' is recorded
# Input: Comment Dataframe with 'bodyclean' column
# Output: Dataframe with Keyword/value column appended with '1' for value/word present and '0' for absent
def matchValues2Sentiment(ScoredComments, SentimentValue):
    Comment = []
    for i in range(len(ScoredComments)):
        Comment = list(ScoredComments.loc[i,'bodyclean'].split())
        print(str(ScoredComments.iloc[i,1]))
        if SentimentValue in Comment:
            ScoredComments.loc[i,str(SentimentValue)] = 1
        else:
            ScoredComments.loc[i,str(SentimentValue)] = 0
            continue
    
    return ScoredComments


def commentQueryController(ScoredComments, WordsOfInterest):
    for word in WordsOfInterest:
        ScoredComments = matchValues2Sentiment(ScoredComments, word)

    return ScoredComments


if __name__ == '__main__':

    ## These successfully combined all of the API csv files into one master file
    #NewDataFrame = comment2MasterDataframe('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis')
    #NewDataFrame.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master.csv')
    #########

    #MasterCSV_DF = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master.csv')
    #countries = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/statistics.csv', encoding = "utf-8")

    #NewMasterCSV_DF = countWordFrequency(MasterCSV_DF, countries)
    #NewMasterCSV_DF.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    #print("complete")

    ## These succesfully cleaned and removed "deleted" comments
    #CleanMasterCSV = cleanDataframe(MasterCSV_DF)
    #CleanMasterCSV.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    #print("complete")

    ## These successfully counted all word frequencies in the master_clean.csv and wrote them to a new csv file
    #print("Loading in master_clean.csv into python")
    #CleanMasterCSV = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    #print("beginning countWordFrequency() function call")
    #WordList = countWordFrequency(CleanMasterCSV)
    #WordList.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/wordlist.csv')
    #print("complete")

    ## This worked! Created a list of sentiment values
    # then wrote that to a new column/csv file
    #CleanMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    ##ScoreAppendedDF = appendSentimentScore(CleanMasterCSVtest)
    ##ScoreAppendedDF.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores.csv')

    ## These succesfully appended a keyword/value column onto the master comment list with word/value presence 1/0 binary data
    #CleanMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores_test.csv')
    #SentimentScoresTest = matchValues2Sentiment(CleanMasterCSVtest, "you")
    #SentimentScoresTest.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/SentimentKeyValueScores_test.csv')

    ## These succesfully appended a keyword/value column onto the TEST master comment list with word/value presence 1/0 binary data
    # Currently need to add an additional step to the cleanDataFrame() function for dropping empty cells
    # Seems to occur with deleted authors. Right now the main query is getting hung up on the empty cells
    ScoredMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores.csv')
    KeyWordQueriedBinaryComments = commentQueryController(ScoredMasterCSVtest, ["you", "illegal"])
    
    KeyWordQueriedBinaryComments.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_scored_queried.csv')
    print("complete")