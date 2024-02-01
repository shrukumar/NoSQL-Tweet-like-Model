import redis


def main():
    r = redis.Redis('localhost', 6379, decode_responses=True)

    # Clear the database
    r.flushall()

    # Set and get
    r.set("foo", 42)
    x = int(r.get("foo"))
    print(x)

    # lists
    r.lpush("people", 1)
    r.lpush("people", 2, 3, 4)
    folks = r.lrange("people", 0, 3)
    print(folks)

    # sets
    r.sadd('carmakes', 'Honda', 'Toyota', 'Toyota', 'Acura')
    cars = r.smembers('carmakes')
    print(cars)

    # sorted sets
    r.zadd('ratings', {'Interstellar': 10})
    r.zadd('ratings', {'Star Wars': 9})
    r.zadd('ratings', {'Moonfall': 2})

    ratings = r.zrange('ratings', 0, -1)
    good_movies = r.zrangebyscore('ratings', 9, 10)
    print(ratings)
    print(good_movies)

    # hashmaps
    r.hset('user:john', 'name', 'John')
    r.hset('user:john', mapping={'name': 'John', 'age': 29, 'email': 'john@aol.com'})
    age = r.hget('user:john', 'age')
    john = r.hgetall('user:john')
    print(age, john)


if __name__ == '__main__':
    main()

