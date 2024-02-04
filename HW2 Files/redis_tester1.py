from redis_apis import TwitterAPI1
from tweet_object import Tweet
import pandas as pd
import time

def main():
    # establishing API connection
    api = TwitterAPI1()

    # adding followers
    api.populate_followers('follows.csv')

    # start timer for posts
    start_time = time.time()

    # adding tweets
    df_tweets = pd.read_csv('tweet.csv')
    for idx, row in df_tweets.iterrows():
        tweet = Tweet(tweet_id = idx, user_id=row[0], tweet_text=row[1], tweet_ts = time.time())
        api.post_tweet(tweet)

    # mapping user_id keys to tweet_id values
    api.map_tweets_users()

    # end timer for posts
    time_elapsed = time.time() - start_time

    # establish timer and counter for timelines
    time_elapsed1 = 0
    timeline_count = 0

    # start timer for timelines
    start_time1 = time.time()
    while time_elapsed1 < 60:
        # retrieve timelines for random users
        user, timeline = api.get_timeline()
        # printing timelines tweet by tweet
        print(f'Timeline for user {user}:')
        for post in timeline:
            print(post)
        print()
        timeline_count += 1

        # while loop checks if time elapsed goes past target time
        time_elapsed1 = time.time() - start_time1

    # close connection
    api.r.close()

    # results
    print(f"It took {time_elapsed} seconds to post {len(df_tweets)} tweets.")
    print(f"Made {round(len(df_tweets) / time_elapsed, 1)} post API calls per second")

    print(f"Retrieved {timeline_count} timelines in {time_elapsed1} seconds")
    print(f"Made {round(timeline_count / time_elapsed1, 2)} timeline API calls per second")

main()