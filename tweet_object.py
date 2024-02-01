"""
author: Shru Kumar
filename: tweet_object.py
description: establishing class Tweet and its store data
"""
class Tweet:

    def __init__(self, user_id, tweet_text):
        self.user_id = user_id
        self.tweet_text = tweet_text

    def __str__(self):
        return f"{self.tweet_text} - user {self.user_id} at {self.tweet_ts}"
        

