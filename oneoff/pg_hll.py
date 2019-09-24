# Postgre HyperLogLog Test

import psycopg2
import psycopg2.extras


def open_conn():
    conn = psycopg2.connect(database='postgres',
                            user='postgres',
                            password='',
                            host='192.168.150.122',
                            port=5432)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cursor


def close_conn(conn, cursor):
    cursor.close()
    conn.close()

def insert():
    conn, cursor = open_conn()
    sql = """INSERT INTO topic_tweet_events_rollup_minute(
                topic,
                pub_year,
                pub_month,
                pub_day,
                pub_hour,
                stat_group,
                stat_type,
                stat_value
            )
            SELECT t_id, pub_year, pub_month, pub_day, pub_hour, 'type' AS type_stat, type, hll_add_agg(hll_hash_bigint(tweet_id)) FROM topic_tweet_mapping GROUP BY t_id, pub_year, pub_month, pub_day, pub_hour, type;"""
    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)

def selectreal():
    conn, cursor = open_conn()
    sql = """SELECT t_id, pub_day, type, COUNT(tweet_id) FROM topic_tweet_mapping
             WHERE pub_day = 20181129
             GROUP BY t_id, pub_day, type;"""
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)

def select1():
    conn, cursor = open_conn()
    sql = """SELECT
                 topic,
                 pub_day,
                 stat_type,
                 SUM(hll_cardinality(stat_value)) AS value
             FROM
                 topic_tweet_events_rollup_minute
             WHERE pub_day = 20181129
             group BY topic, pub_day, stat_type
             ORDER BY pub_day ASC, stat_type;"""
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)

def select2():
    conn, cursor = open_conn()
    sql = """SELECT
                 topic,
                 pub_day,
                 stat_type,
                 hll_cardinality(hll_union_agg(stat_value)) AS value
             FROM
                 topic_tweet_events_rollup_minute
             WHERE pub_day = 20181129
             group BY topic, pub_day, stat_type
             ORDER BY pub_day ASC, stat_type;"""
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)

print 'real'
selectreal()
print 'select1: SUM(hll_cardinality(stat_value)) AS value'
select1()
print 'select2: hll_cardinality(hll_union_agg(stat_value)) AS value'
select2()