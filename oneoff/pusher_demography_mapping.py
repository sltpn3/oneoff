import beanstalkc
from datetime import datetime, timedelta
# 
beans = beanstalkc.Connection('192.168.150.21')
beans.use('demography_citus_twitter_topic_tweet_mapping')

now = datetime.now()

# with open('topic', 'r') as f:
#     topic_list = f.readlines()
# 
# # print topic_list
# 
# for n in range(60):
#     _date = (now - timedelta(days=n)).strftime('%Y%m%d')
#     for topic in topic_list:
#         job = '{}|{}'.format(topic.replace('\n', ''), _date)
#         beans.put(job, priority=100)

beans.watch('demography_citus_twitter_topic_tweet_mapping')
run = True
while run:
    job = beans.reserve()
    print job.stats()['pri']
    if job.stats()['pri'] == 50:
        job.delete()