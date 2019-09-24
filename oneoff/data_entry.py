from datetime import datetime
from ConfigParser import ConfigParser
import requests
import json

config = ConfigParser()
config.read("config.conf")


data = []
param1 = {'q': 'pub_day:{}'.format(datetime.now().strftime('%Y%m%d')),
          'rows': 0,
          'wt': 'json'}
param2 = {'q': 'd_day:{}'.format(datetime.now().strftime('%Y%m%d')),
          'rows': 0,
          'wt': 'json'}

resp = requests.get(config.get('solr', 'news'), params=param1)
data.append({'name': 'online_news', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'newspaper'), params=param1)
data.append({'name': 'printed_news', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'tv'), params=param1)
data.append({'name': 'tv_news', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'tweet_stat'), params=param2)
data.append({'name': 'twitter_discover', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'social'), params=param1)
data.append({'name': 'twitter_analyze', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'facebook'), params=param1)
data.append({'name': 'facebook_analyze', 'value': resp.json()['response']['numFound']})

resp = requests.get(config.get('solr', 'facebook_discover'), params=param2)
data.append({'name': 'facebook_discover', 'value': resp.json()['response']['numFound']})

with open('data_entry.log', 'w') as f:
    f.write(json.dumps(data))
