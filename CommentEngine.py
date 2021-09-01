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
# note: depending on how you installed (e.g., using source code download versus pip install), you may need to import like this:
# from vaderSentiment import SentimentIntensityAnalyzer
# Compound score > 0.05 is positive, 0.05 - -0.05 neutral, < -0.05 negative
# May need to cluster compound scores to get a better assessment of negativity

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
    CommentDataframe["bodyclean"].replace('removed', np.nan, inplace=True)
    CommentDataframe["bodyclean"].replace('', np.nan, inplace=True)
    # Drop all NaN values (previously "deleted" comments), cleaning up the dataset
    CommentDataframe.dropna(subset=["bodyclean"], inplace=True)
    #print("Cleaned file: " + str(CommentDataframe.head(3)))

    return CommentDataframe

## This DOES NOT work!
# Needs to use SQL for inputing line by line, the database is too large to load in via CSV
# Need to make the internal csv write call dynamic not static
def appendSentimentScore(CleanCommentDF):
    comments = CleanCommentDF["body"]
    analyzer = SentimentIntensityAnalyzer()
    #vs_list = []
    
    i = 0
    for comment in comments:
        vs = analyzer.polarity_scores(str(comment))
        #vs_list.append(vs["compound"])
        CleanCommentDF.loc[i, "Sentiment Score"] = vs["compound"]
        #CleanCommentDF.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores.csv', index=False)
        #print("{:-<65} {}".format(comment, str(vs['compound'])))
        i = i + 1
    #    #print(CleanMasterCSVtest['id'])
    
    #CleanCommentDF["Sentiment Score"] = vs_list
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
        print(str(SentimentValue) + " " + str(i) + "/" + str(len(ScoredComments)) + " " + str(ScoredComments.loc[i,'id']))
        if SentimentValue in Comment:
            ScoredComments.loc[i,str(SentimentValue)] = 1
        else:
            ScoredComments.loc[i,str(SentimentValue)] = 0
            continue
    
    return ScoredComments


def commentQueryController(ScoredComments, WordsOfInterest, WordCategory):
    for word in WordsOfInterest:
        ScoredComments = matchValues2Sentiment(ScoredComments, word)
        ScoredComments.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_scored_queried.csv', index = False)
    
    ScoredComments[WordCategory] = ['Y' if x > 0 else 'N' for x in np.sum(ScoredComments.filter(like=WordsOfInterest).values == 'Y',1)]

    return print("completed")


def commentCleaningController(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and os.path.splitext(f)[1] == '.csv']

    for file in onlyfiles:
        print("Beginning to clean and score file: " + str(file))
        limboCSV = appendSentimentScore(cleanDataframe(pd.read_csv(mypath + file)))
        limboCSV.to_csv(mypath + "_cleanedANDscored" + file, index = False)
        print("Completed cleaning and scoring file: " + str(file))
    
    print("cleaned all files in folder: " + str(mypath))
    return onlyfiles


if __name__ == '__main__':

    ## These successfully combined all of the API csv files into one master file
    #NewDataFrame = comment2MasterDataframe('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis')
    #NewDataFrame.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master.csv', index=False)
    #########

    #MasterCSV_DF = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master.csv')
    #countries = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/statistics.csv', encoding = "utf-8")

    #NewMasterCSV_DF = countWordFrequency(MasterCSV_DF, countries)
    #NewMasterCSV_DF.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv', index=False)
    #print("complete")

    ## These succesfully cleaned and removed "deleted" comments
    #MasterCSV_DF = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master.csv')
    #CleanMasterCSV = cleanDataframe(MasterCSV_DF)
    #CleanMasterCSV.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv', index=False)
    #print("complete")

    ## These successfully counted all word frequencies in the master_clean.csv and wrote them to a new csv file
    #print("Loading in master_clean.csv into python")
    #CleanMasterCSV = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    #print("beginning countWordFrequency() function call")
    #WordList = countWordFrequency(CleanMasterCSV)
    #WordList.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/wordlist.csv', index=False)
    #print("complete")

    ## This worked! Created a list of sentiment values
    # then wrote that to a new column/csv file
    #CleanMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean.csv')
    #ScoreAppendedDF = appendSentimentScore(CleanMasterCSVtest)
    #appendSentimentScore(CleanMasterCSVtest)
    #ScoreAppendedDF.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores_test.csv', index=False)
    #print("complete")

    ## These succesfully appended a keyword/value column onto the master comment list with word/value presence 1/0 binary data
    #CleanMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_clean_scores_test.csv')
    #SentimentScoresTest = matchValues2Sentiment(CleanMasterCSVtest, "you")
    #SentimentScoresTest.to_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/SentimentKeyValueScores_test.csv', index=False)

    ## These succesfully appended a keyword/value column onto the TEST master comment list with word/value presence 1/0 binary data
    # Currently need to add an additional step to the cleanDataFrame() function for dropping empty cells
    # Seems to occur with deleted authors. Right now the main query is getting hung up on the empty cells
    #ScoredMasterCSVtest = pd.read_csv('C:/Users/Jonathan/Documents/GitHub/ConservativeSentimentAnalysis/master_scored_queried.csv')
    #FemaleKeywords = ["she","her","woman","women","girl","girls","female","females","lady","ladies","mom","mother","daughter","grandma",
    #"grandmother","girlfriend","girlfriends","sister","sisters","wife","wives","matriarch","feminine"]
    
    #MaleKeywords = ["he", "him","his","man","men","boy","boys","male","males","father","grandpa","grandfather","son","boyfriend",
    #"boyfriends","brother","brothers", "bro", "bros","husband","husbands","patriarch","masculine"]

    #MaleKeyWordSingle = ["he"]
    
    #KeyWordQueriedBinaryComments = commentQueryController(ScoredMasterCSVtest, MaleKeyWordSingle, "Sentiment Female")

    ## Working on the new commentCleaningController(), in future steps will be as follows:
    # FileDirectory of all raw csvs -> commentCleaningController -> iterate through all csvs and clean/attach a sentiment score
    # Then these cleaned and scored csvs will be uploaded to PostgreSQL database administrated with DBeaver
    commentCleaningController("C:/git/ConservativeSentimentAnalysisDATA/RAWr_conservative/")