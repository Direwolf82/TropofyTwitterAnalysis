"""
Author:      www.tropofy.com

Copyright 2015 Tropofy Pty Ltd, all rights reserved.

This source file is part of Tropofy and governed by the Tropofy terms of service
available at: http://www.tropofy.com/terms_of_service.html

This source file is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the license files for details.
"""
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Text, Float, Integer
from sqlalchemy.schema import Column, ForeignKeyConstraint, UniqueConstraint
from tropofy.database.tropofy_orm import DataSetMixin
from tropofy.app import AppWithDataSets, Step, StepGroup
from tropofy.widgets import SimpleGrid, KMLMap, ExecuteFunction, Chart
from simplekml import Kml
import tweepy
from textblob import TextBlob

class TweetGetter():
    __consumerKey = ''
    __consumerSecret = ''
    __accessToken = ''
    __accessTokenSecret = ''

    @classmethod
    def __ConnectToTwitter(cls):
        __consumerKey = 'oJups5z5W8NZkjFipIYdW4vXw'
        __consumerSecret = 'YYsbjXPGXMvNkqRZ2YgBYi3YUC5Josd635IJKiJmVFUzYA13GP'
        __accessToken = '25891255-ptVBZPyNfbwhW2MpbffFeX7GJmOR4Uclxu90PZuOz'
        __accessTokenSecret = 'ilR1zn3LxcRg3QtbXrjYBN3H3qb1gshdxW8mOEUAudAXB'
        auth = tweepy.OAuthHandler(__consumerKey, __consumerSecret)
        auth.set_access_token(__accessToken, __accessTokenSecret)
        api = tweepy.API(auth)
        return api

    @classmethod
    def GetTweets(self, searchString, sinceId = 0):
        api = self.__ConnectToTwitter()
        public_tweets = api.search(searchString, since_id=sinceId)
        return public_tweets

class Tweet(DataSetMixin):
    author = Column(Text, nullable = False)
    text = Column(Text, nullable=False)
    coordinates = Column(Text, nullable = True)
    tweet_id = Column(Text, nullable = False)
    sentiment_polarity = Column(Float, nullable = False)
    sentiment_subjectivity = Column(Float, nullable = False)
    search_term_used = Column(Text, nullable = False)

    def __init__(self, author, text, coordinates, tweetId, polarity, subjectivity, search_term):
        self.author = author
        self.text = text
        self.coordinates = coordinates
        self.tweet_id = tweetId
        self.sentiment_polarity = polarity
        self.sentiment_subjectivity = subjectivity
        self.search_term_used = search_term

    @classmethod
    def get_table_args(cls):
        return(UniqueConstraint('tweet_id','data_set_id'),)

class TweetSentiment(DataSetMixin):
    tweet_id = Column(Text, nullable = False)
    sentiment_polarity = Column(Float, nullable = False)
    sentiment_subjectivity = Column(Float, nullable = False)
    search_term = Column(Text, nullable = False)

    def __init__(self, id, polarity, subjectivity, search_term):
        self.tweet_id = id
        self.sentiment_polarity = polarity
        self.sentiment_subjectivity = subjectivity
        self.search_term = search_term

    @classmethod
    def get_table_args(cls):
        return(
            UniqueConstraint('tweet_id', 'search_term', 'data_set_id'),
            ForeignKeyConstraint(['tweet_id', 'data_set_id'], ['tweet.tweet_id', 'tweet.data_set_id'], ondelete='CASCADE', onupdate='CASCADE'),
        )

class TweetSearchTerms(DataSetMixin):
    search_term = Column(Text, nullable = False)
    max_twitter_id = Column(Text, nullable = True)

    @classmethod
    def get_table_args(cls):
        return(
            UniqueConstraint('search_term', 'data_set_id'),
        )

class MyKMLMap(KMLMap):
    def get_kml(self, app_session):
        kml = Kml()
        for tweet in app_session.data_set.query(Tweet).all():
            kml.newpoint(name=tweet.author + "-"+ str(tweet.tweetId),
                         coords=[tweet.coordinates])
        return kml.kml()

class ExecuteGetTweets(ExecuteFunction):
    def get_button_text(self, app_session):
        return "Retrieve Tweets"

    def execute_function(self, app_session):
        tweet_data =[]
        #tweet_sentiments = []
        for searchTerms in app_session.data_set.query(TweetSearchTerms).all():
            searchString = searchTerms.search_term
            max_id = searchTerms.max_twitter_id
            if max_id  is None:
                tweets = TweetGetter.GetTweets(searchString)
            else:
                tweets = TweetGetter.GetTweets(searchString, max_id)
            for tweet in tweets:
                blob = TextBlob(tweet.text)
                twt = Tweet(tweet.author.name, tweet.text, tweet.coordinates, tweet.id, blob.sentiment.polarity, blob.sentiment.subjectivity, searchString )
                tweet_data.append(twt)
                #sentiment = TweetSentiment(twt.tweet_id, blob.polarity, blob.subjectivity, searchString )
                #tweet_sentiments.append(sentiment)

        #app_session.data_set.add_all(tweet_sentiments)
        app_session.data_set.add_all(tweet_data)

class SentimentScatterChart(Chart):
    def get_chart_type(self, app_session):
        return Chart.SCATTERCHART

    def get_table_schema(self, app_session):
        return{
            "sentiment_subjectivity": ("number", "Subjectivity"),
            "sentiment_polarity": ("number", "Polarity"),
            "search_term": ("string", "Search Term")
        }

    def get_table_data(self, app_session):
        results = []
        search_terms = app_session.data_set.query(TweetSearchTerms.search_term).distinct()
        for term in search_terms:
            tweets = app_session.data_set.query(Tweet).filter_by(search_term = term)
            for tweet in tweets:
                results.append({
                    "sentiment_subjectivity": tweet.sentiment_subjectivity,
                    "sentiment_polarity": tweet.sentiment_polarity,
                    "search_term": search_term
                })

class MyFirstApp(AppWithDataSets):
    def get_name(self):
        return "Basic Twitter Analytics"

    def get_gui(self):
        step_group_1 = StepGroup(name='Input')
        step_group_1.add_step(Step(name='Search Terms', widgets=[SimpleGrid(TweetSearchTerms)]))
        step_group_1.add_step(Step(name='Get Tweets', widgets=[ExecuteGetTweets()]))

        step_group_2 = StepGroup(name = 'Results')
        step_group_2.add_step(Step(name='Tweets', widgets=[SimpleGrid(Tweet, editable=False)]))

        step_group_3 = StepGroup(name = 'Visualisations')
        step_group_3.add_step(Step(name="Sentiment Distribution", widgets=[SentimentScatterChart()]))

        return [step_group_1, step_group_2, step_group_3]
