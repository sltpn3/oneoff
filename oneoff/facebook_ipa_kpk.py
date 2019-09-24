from library.eBsolr import eBsolr

solr = eBsolr('http://192.168.150.101:8983/solr/fb_post', '')

query = '(kpk OR "komisi pemberantasan korupsi") AND pub_day:[20180622 TO 20190621]'

print 'request solr start'
response = solr.getDocs(query, 'user_id', rows=300000)
print 'request solr done'

# print response['docs']

ids = {}
for doc in response['docs']:
    if 'user_id' in doc:
        try:
            ids[doc['user_id']] += 1
        except:
            ids[doc['user_id']] = 1
            
print len(ids)
