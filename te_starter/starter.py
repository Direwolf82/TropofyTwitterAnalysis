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
from tropofy.widgets import SimpleGrid, KMLMap, ExecuteFunction, GridWidget
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
    tweetId = Column(Text, nullable = False)

    def __init__(self, author, text, coordinates, tweetId):
        self.author = author
        self.text = text
        self.coordinates = coordinates
        self.tweetId = tweetId

    @classmethod
    def get_table_args(cls):
        return(UniqueConstraint('tweetId','data_set_id'),)

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
        results =[]
        for searchTerms in app_session.data_set.query(TweetSearchTerms).all():
            searchString = searchTerms.search_term
            max_id = searchTerms.max_twitter_id
            if max_id  is None:
                tweets = TweetGetter.GetTweets(searchString)
            else:
                tweets = TweetGetter.GetTweets(searchString, max_id)
            for tweet in tweets:
                twt = Tweet(tweet.author.name, tweet.text, tweet.coordinates, tweet.id )
                results.append(twt)
        app_session.data_set.add_all(results)
        return results

class MyFirstApp(AppWithDataSets):
    def get_name(self):
        return "Basic Twitter Analytics"

    def get_gui(self):
        step_group_1 = StepGroup(name='Input')
        step_group_1.add_step(Step(name='Search Terms', widgets=[SimpleGrid(TweetSearchTerms)]))
        step_group_1.add_step(Step(name='Get Tweets', widgets=[ExecuteGetTweets()]))

        step_group_2 = StepGroup(name = 'Results')
        step_group_2.add_step(Step(name='Tweets', widgets=[SimpleGrid(Tweet, editable=False)]))

        #step_group_3 = StepGroup(name = 'Visualisations')


        return [step_group_1, step_group_2]

