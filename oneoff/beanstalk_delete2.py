import beanstalkc

host = '192.168.150.21'
tube = 'demography_citus_twitter_topic_tweet_mapping'

beans = beanstalkc.Connection(host)
beans.connect()
# beans.watch(tube)
beans.use(tube)

_days = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
        '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
        '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
        '31']

run = True
for _day in _days[::-1]:
    body = '6633|201901{}'.format(_day)
    print body
    beans.put(body, 1, 0, 3600)
