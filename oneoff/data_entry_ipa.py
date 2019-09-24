from datetime import datetime
from ConfigParser import ConfigParser
import requests
import json
import psycopg2
import psycopg2.extras

config = ConfigParser()
config.read("config_data_entry.conf")

data = []
# param1 = {'q': 'pub_day:{}'.format(datetime.now().strftime('%Y%m%d')),
#           'rows': 0,
#           'wt': 'json'}
# param2 = {'q': 'd_day:{}'.format(datetime.now().strftime('%Y%m%d')),
#           'rows': 0,
#           'wt': 'json'}
# 
# resp = requests.get(config.get('solr', 'twitter_tweet_master'), params=param1)
# data.append({'name': 'twitter', 'value': resp.json()['response']['numFound']})
# 
# resp = requests.get(config.get('solr', 'fb_post'), params=param1)
# data.append({'name': 'facebook', 'value': resp.json()['response']['numFound']})
# 
# resp = requests.get(config.get('solr', 'youtube'), params=param1)
# data.append({'name': 'youtube', 'value': resp.json()['response']['numFound']})
# 
# resp = requests.get(config.get('solr', 'instagram'), params=param1)
# data.append({'name': 'instagram', 'value': resp.json()['response']['numFound']})

citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )

cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
sql = """SELECT COUNT(*) FROM "pilpres_candidate_tweet_mapping"
         WHERE created_at BETWEEN '{0} 00:00:00' AND '{0} 23:59:59'
         """.format(datetime.now().strftime('%Y-%m-%d'))

cursor.execute(sql)
row = cursor.fetchone()
cursor.close()
citus_conn.close()
data.append({'name': 'pilpres', 'value': int(row[0])})
# print data
with open('data_entry.log', 'w') as f:
    f.write(json.dumps(data))
