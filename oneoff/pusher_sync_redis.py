import MySQLdb
from library.eBsolr import eBsolr
import redis
import json

solr_facebook = eBsolr('http://10.11.12.88:8983/solr/facebook', 'config')


def pusher(_date):
    redis_pointer = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)
    conn = MySQLdb.connect('10.11.12.35', 'backend', 'rahasia123', 'fb_master')
    query = 'pub_day:{}'.format(_date)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    run = True
    start = 0
    while run:
        response = solr_facebook.getDocs(query, start=start, rows=10000, fields='id')
        for doc in response['docs']:
            sql = "SELECT * FROM fb_master_posts WHERE id='{}'".format(doc['id'])
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                row['insert_date'] = row['insert_date'].strftime('%Y-%m-%d %H:%M:%S')
                redis_pointer.rpush('facebook_analyze', json.dumps(row))
                print row['id']
        start += 10000
        if len(response['docs']) < 10000:
            run = False
    cursor.close()
    conn.close()

pusher()