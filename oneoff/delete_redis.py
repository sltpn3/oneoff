import redis

_redis = redis.StrictRedis(host='192.168.150.25',
                           port=6379, db=1)

keys = _redis.keys('beanstalk:facebook:fans_page:1000*')

print len(keys)
