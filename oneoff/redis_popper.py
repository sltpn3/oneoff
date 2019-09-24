import redis

host = '103.18.244.20'
port = 6379
db = 1
keyname = ''

redis_pointer = redis.StrictRedis(host=host, port=port, db=db)

run = True

while run:
    response = redis_pointer.lpop(keyname)
    print response