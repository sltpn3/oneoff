from DBUtils.PersistentDB import PersistentDB
from library.eBsolr import eBsolr

import psycopg2
import psycopg2.extras

dbname = 'postgres'
dbuser = 'postgres'
dbpwd = 'rahasia'
dbhost = '192.168.150.155'
dbport = 5432

pool_pg = PersistentDB(psycopg2, host=dbhost, user=dbuser,
                       password=dbpwd, database=dbname)

conn = pool_pg.connection()
cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

solr_host = 'http://192.168.150.132:8983/solr/twitter_tweet'

solr = eBsolr(solr_host, 'config.conf')

sql = "SELECT * FROM topic_tweet_mapping WHERE t_id=6965 AND country <> ''  AND pub_day BETWEEN '20190801' AND '20190831';"

cursor.execute(sql)
rows = cursor.fetchall()
for row in rows:
    query = 'id:{}'.format(row['tweet_id'])
    response = solr.getDocs(query, 'text')
    try:
        text = response['docs'][0]['text'].encode('utf-8')
    except:
        text = response['docs'][0]['text']
    try:
        province = row['province'].split('|')[-1]
    except:
        province = row['province']
    try:
        city = row['city'].split('|')[-1]
    except:
        city = row['city']
    print '{}|{}|{}|{}|{}|{}|{}|{}'.format(row['tweet_id'],
                    row['user_id'],
                    row['created_at'],
                    row['country'].split('|')[-1],
                    province,
                    city,
                    row['age'],
                    text.replace('\n', '').replace('|', '-'))
