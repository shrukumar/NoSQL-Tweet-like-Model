"""
Demonstrate receiving and processing jobs
using redis publish / subscribe
"""

import redis
import time
import random as rnd

def main():
    r = redis.Redis('localhost', 6379, decode_responses=True)

    # Clear the database
    r.flushall()

    id = 0
    while True:
        id += 1
        x = rnd.randrange(1000)
        y = rnd.randrange(1000)
        user = rnd.randrange(10)
        msg = str(id)+","+str(user)+","+str(x)+","+str(y)
        r.publish("jobs", msg)
        time.sleep(1)


main()