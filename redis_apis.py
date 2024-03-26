import redis
import pandas as pd
import random

class TwitterAPI1:
    def __init__(self, host = 'localhost', port = 6379, decode_responses = True):
        self.r = redis.Redis(host = host, port = port, decode_responses = decode_responses)
        self.r.flushall()

    def close(self):
        self.r.quit()

    def populate_followers(self, file):
        """adds follower data into redis keys and values

        :param file (str): file containing follower data
        """
        df_followers = pd.read_csv(file)
        for idx, row in df_followers.iterrows():
            self.r.lpush(f'follow_user:{int(row[0])}', int(row[1]))

    def post_tweet(self, tweet):
        """adds a single tweet's data into redis key and values

       :param tweet (Tweet): Tweet object containing user_id and tweet_text and tweet_ts
       """
        self.r.hset(f'tweet:{tweet.tweet_id}', mapping={'user': tweet.user_id, 'text': tweet.tweet_text, \
                                                           'time': tweet.tweet_ts})
    def get_random_user(self):
        """returns one random user id

        :return: rand_user: random choice user id
        """
        # collecting users id keys
        users = self.r.keys('follow_user:*')
        user_ids = []
        # adding just the user id numbers
        for user in users:
            user_ids.append(user.split(':')[1])

        rand_user = random.choice(user_ids)
        return rand_user

    def map_tweets_users(self):
        """matches all tweet id as values to user id key
        """
        tweet_ids = self.r.keys('tweet:*')
        for tweet_id in tweet_ids:
            self.r.lpush(f'user_id:{self.r.hget(tweet_id, "user")}', tweet_id)

    def get_timeline(self):
        """gets timeline of random user

       :return: rand_user: id of random user
               timeline (list): list of ten most recent posts that user follows
       """
        rand_user = self.get_random_user()
        # gets list of users that random user follows
        following = self.r.lrange(f'follow_user:{rand_user}', 0, -1)

        # building timeline
        for user in following:
            for tweet_id in self.r.lrange(f'user_id:{user}', 0, -1):
                text = self.r.hget(tweet_id, 'text')
                user = self.r.hget(tweet_id, 'user')
                time = self.r.hget(tweet_id, 'time')
                self.r.zadd('timeline', {f'User {user} tweeted "{text}" at {time}.': time})

        # getting 10 most recent posts
        timeline = self.r.zrevrange('timeline', 0, 9)
        return rand_user, timeline

class TwitterAPI2:

    def __init__(self, host = 'localhost', port = 6379, decode_responses = True):
        self.r = redis.Redis(host = host, port = port, decode_responses = decode_responses)
        self.r.flushall()

    def close(self):
        self.r.quit()

    def populate_followers(self, file):
        """adds follower data into redis keys and values

        :param file (str): file containing follower data
        """
        df_followers = pd.read_csv(file)
        for idx, row in df_followers.iterrows():
            self.r.lpush(f'follow_user:{int(row[1])}', int(row[0]))

    def post_tweet(self, tweet):
        """adds a single tweet's data into redis key and values and adds to timelines

       :param tweet (Tweet): Tweet object containing user_id and tweet_text and tweet_ts
       """
        self.r.hset(f'tweet_id:{tweet.tweet_id}', mapping = {'user': tweet.user_id, 'text': tweet.tweet_text, \
                                                             'time': tweet.tweet_ts})

        text = self.r.hget(f'tweet_id:{tweet.tweet_id}', 'text')
        user = self.r.hget(f'tweet_id:{tweet.tweet_id}', 'user')
        time = self.r.hget(f'tweet_id:{tweet.tweet_id}', 'time')


        for follower in self.r.lrange(f'follow_user:{tweet.user_id}', 0, -1):
            self.r.lpush(f'timeline_for_user:{follower}', f'User {user} tweeted "{text}" at {time}.')

    def get_random_user(self):
        """returns one random user id

        :return: rand_user: random choice user id
        """
        # collecting users id keys
        users = self.r.keys('follow_user:*')
        user_ids = []
        # adding just the user id numbers
        for user in users:
            user_ids.append(user.split(':')[1])

        rand_user = random.choice(user_ids)
        return rand_user

    def get_timeline(self):
        """gets timeline of random user

        :return: rand_user: id of random user
                timeline (list): list of ten most recent posts that user follows
        """
        rand_user = self.get_random_user()
        timeline = self.r.lrange(f'timeline_for_user:{rand_user}', 0, 9)
        return rand_user, timeline
