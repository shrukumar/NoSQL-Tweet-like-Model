"""
author: Shru Kumar
filename: main.py
description: driver of the API, utilizes other API files to generate followers table, post tweets,
get timelines, and calculate API calls per second
"""
import os
import pandas as pd
from twitter_mysql import TwitterAPI
from tweet_object import Tweet
import time

def main():

    # Authenticate
    api = TwitterAPI(os.environ["TWITTER_USER"], os.environ["TWITTER_PASSWORD"], "Twitter")

    # clearing all tables
    api.clear_tables("Follow")
    api.clear_tables("Tweets")

    # add follower data 
    api.populate_followers("follows.csv")

    # add tweets
    start_time = time.time()
    df_tweets = pd.read_csv("tweet.csv")
    for idx, row in df_tweets.iterrows():
        tweet = Tweet(user_id = row[0], tweet_text = row[1])
        api.post_tweet(tweet)
    time_elapsed = time.time() - start_time

    # getting df of added tweets to use for calls/second calc
    df_twitter = api.get_tweets()

    # getting timelines
    # initializing variables to calculate calls/second
    time_elapsed1 = 0
    timeline_count = 0

    start_time1 = time.time()
    while time_elapsed1 < 60:
        rand_user = api.get_random_user()
        timeline = api.get_timeline(rand_user)
        timeline_count += 1
        print(timeline)

        # while loop checks if time elapsed goes past target time
        time_elapsed1 = time.time() - start_time1

    # results
    print(f"It took {time_elapsed} seconds to post {len(df_twitter)} tweets.")
    print(f"Made {round(len(df_twitter) / time_elapsed, 1)} post API calls per second")

    print(f"Retrieved {timeline_count} timelines in {time_elapsed1} seconds")
    print(f"Made {round(timeline_count / time_elapsed1, 2)} timeline API calls per second")


if __name__ == '__main__':
    main()