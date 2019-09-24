import psycopg2
import psycopg2.extras
from DBUtils.PersistentDB import PersistentDB
import MySQLdb

pool = PersistentDB(psycopg2, database='postgres',
                    user='postgres',
                    password='rahasia',
                    host='192.168.150.155',
                    port='5432')
print pool._creator.__name__

# conn = pool.connection()
# cursor = conn.cursor()
# 
# sql = """SELECT
#                  topic,
#                  pub_day,
#                  stat_group,
#                  stat_type,
#                  SUM(hll_cardinality(stat_value)) AS value
#              FROM
#                  topic_tweet_events_rollup_minute
#              WHERE topic = 5494 AND 
#              pub_day = '20190117'
#              GROUP BY topic, pub_day, stat_group, stat_type
#              ORDER BY pub_day ASC, stat_type;"""
# 
# cursor.execute(sql)
# rows = cursor.fetchall()
# for row in rows:
#     print row
# 
# cursor.close()
# conn.close()

# pool = PersistentDB(
#             MySQLdb, host='192.168.150.22', user='backend',
#             passwd='rahasia123', db='ipa_main', charset='utf8')