from library.eBeanstalk import Pusher
from datetime import datetime, timedelta


tube = 'test'
pusher = Pusher(tube, '192.168.150.150')

topics = [32768, ]
last_day = datetime.now()
range_day = 180

for topic in topics:
    for n in range(range_day):
        _day = (last_day - timedelta(days=n)).strftime('%Y%m%d')
        job = '{}|{}'.format(topic, _day)
        print job