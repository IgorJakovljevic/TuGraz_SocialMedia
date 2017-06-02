import nltk
import csv
from nltk.sentiment import vader
sia = vader.SentimentIntensityAnalyzer()

def getTweetData(tweet):
    tweetData =  tweet.split(';')
    tweetDataLength = len(tweetData)
    if(tweetDataLength > 2):
        return str(tweetData[2])

    return ''
    
def vaderSentiment(tweet):
    return sia.polarity_scores(tweet)['compound']

def getReviewSentiments(sentimentCalculator):
    filteredTweets =list(map(getTweetData, tweets))
    return [sentimentCalculator(tweet) for tweet in filteredTweets]

negativeReviewFileName = "/home/igorjakovljevic/NLPProjects/NLPToolkit/School/tweets.csv"

with open(negativeReviewFileName, 'r') as f:
    tweets = f.readlines()

resultSentiment = getReviewSentiments(vaderSentiment)

print "Positive count"
print sum([1 for x in resultSentiment if x > 0])

print "Negative count"
print sum([1 for x in resultSentiment if x < 0])


