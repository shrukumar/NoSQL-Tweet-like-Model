"""
Created on Thu Feb  4 21:56:24 2021

@author: rachlin
@file  : redis_test.py
"""

import redis

#%% Create a connection and clear the database
r = redis.Redis(host='localhost', port=6379, db = 0, decode_responses=True)
r.flushall()

#%% Some random tests
r.set('foo', 100)
x = r.get('foo')
print('foo', x)

r.lpush('friends', 'joe')
r.lpush('friends', 'bob')
r.lpush('friends', 'cal')

friends = r.lrange('friends', 0, -1)

print(friends)

redis.Redis()


#%% Store 1 million keys

import time

r.flushall()
N = 100000

start = time.time()
for i in range(N):
    #r.set('key:'+str(i), 'This is tweet '+str(i))
    r.set(f'key:{i}',i)
finish = time.time()
diff = finish - start
rate = N / diff


print(f'Stored {N} keys in {diff} seconds (Rate={rate}/sec')
