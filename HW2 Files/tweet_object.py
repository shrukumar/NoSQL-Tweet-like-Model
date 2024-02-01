"""
author: Shru Kumar
filename: tweet_object.py
description: establishing class Tweet and its store data
"""
class Tweet:

    def __init__(self, tweet_id, user_id, tweet_text, tweet_ts):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_text = tweet_text
        self.tweet_ts = tweet_ts

    def __str__(self):
        return f"{self.tweet_text} - user {self.user_id} at {self.tweet_ts}"
        

