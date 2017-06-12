import nltk
import csv
import pymysql
import MySQLdb
import sys
import string
import pickle
import io
from nltk.sentiment import vader
sia = vader.SentimentIntensityAnalyzer()

def vaderSentiment(tweet):
    return sia.polarity_scores(tweet)['compound']

def getReviewSentiments(sentimentCalculator, listOfElements):
    tweets =list(map(getTweetData, listOfElements))
    return [sentimentCalculator(tweet) for tweet in tweets]

def getUniqueHashTag(conn):
    return [["Something"], ["Health"], ["Sports"]]
    # cur = conn.cursor()
    # cur.execute("SELECT DISTINCT HashTag FROM  tweets WHERE userLanguage = 'en'")    
    # return cur.fetchall()

def getTweetData(tweet):
    return tweet[0]

def getAllEnglishTweets(conn, hashtag):
    return [["This is nice"], ["This is bad"], ["This is super nice"]]
    # cur = conn.cursor()
    # Query also by hashtag use the hashtag param to query the db
    # cur.execute("SELECT Text FROM tweets WHERE userLanguage = 'en'")    
    # return cur.fetchall()

negativeReviewFileName = "/home/igorjakovljevic/NLPProjects/NLPToolkit/School/tweets.csv"
# myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )

# hashtags = getUniqueHashTag(myConnection)

hashtags = getUniqueHashTag("")
result = []
for hashtag in hashtags:
    # tweets = getAllEnglishTweets(myConnection, hashtag)
    tweets = getAllEnglishTweets("", hashtag)
    resultSentiment = getReviewSentiments(vaderSentiment,tweets)
    result.append({"hashtag": hashtag[0], "PositiveTweets" : sum([1 for x in resultSentiment if x > 0]), "negativeTweets": sum([1 for x in resultSentiment if x < 0])})

print result


