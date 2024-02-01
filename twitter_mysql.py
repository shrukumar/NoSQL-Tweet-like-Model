"""
author: Shru Kumar
filename: twitter_mysql.py
description: functions that the TwitterAPI can perform
"""
import pandas as pd
import random
from dbutils import DBUtils


class TwitterAPI:

    def __init__(self, user, password, database, host="localhost"):
        self.dbu = DBUtils(user, password, database, host)
    def clear_tables(self, table_name):
        """clears all data from sql table

        :param table_name (str): name of table to clear
        """
        sql = f"DELETE FROM {table_name}"
        self.dbu.execute(sql)
    def populate_followers(self, file):
        """adds follower data into follower sql table

        :param file (str): file containing follower data
        """
        # storing data in df
        df_followers = pd.read_csv(file)
        for idx, row in df_followers.iterrows():
            # inserting each row into sql one at a time
            sql = "INSERT INTO Follow (user_id, follows_id) VALUES (%s, %s) "
            self.dbu.insert_one(sql, tuple(row))
    def post_tweet(self, tweet):
        """adds a single tweet's data to sql tweets table

        :param tweet (Tweet): Tweet object containing user_id and tweet_text
        """
        sql = "INSERT INTO Tweets (user_id, tweet_text) VALUES (%s, %s) "
        val = (tweet.user_id, tweet.tweet_text)
        self.dbu.insert_one(sql, val)

    def get_tweets(self):
        """gets all the tweet data in the tweets sql table
        :return:
            df_users (pd.dataframe): df of all tweets and their users
        """
        sql = f"""
            SELECT Tweets.user_id, tweet_ts, tweet_text FROM Tweets JOIN Follow 
            ON Tweets.user_id = Follow.follows_id
                    """
        df_users = self.dbu.execute(sql)
        return df_users
    def get_random_user(self):
        """selects random user

        :return: random user via user_id
        """
        sql = "SELECT DISTINCT user_id AS user FROM Tweets"
        df_users = self.dbu.execute(sql)
        return random.choice(list(df_users['user']))
    
    def get_timeline(self, rand_user):
        """gets the timeline(10 most recent posts from all people a user follows

        :param rand_user: random user's user_id
        :return:
            timeline(sql table): user's timeline
        """
        sql = f"""
            SELECT Tweets.user_id, tweet_ts, tweet_text FROM Tweets JOIN Follow 
            ON Tweets.user_id = Follow.follows_id
            WHERE Follow.user_id = {rand_user}
            ORDER BY tweet_ts DESC
            LIMIT 10
            """
        timeline = self.dbu.execute(sql)
        return timeline


