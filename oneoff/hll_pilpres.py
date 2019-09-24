import psycopg2
import psycopg2.extras
from library.eBeanstalk import Worker, Pusher


def open_conn():
    conn = psycopg2.connect(database='postgres',
                            user='postgres',
                            password='rahasia',
                            host='192.168.150.155',
                            port=5432)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cursor

def close_conn(conn, cursor):
    try:
        cursor.close()
    except:
        pass
    try:
        conn.close()
    except:
        pass

def worker_demography():
    worker = Worker('pilpres_demography', '', '192.168.150.21')
    run = True
    while run:
        try:
            job = worker.getJob()
            topic_id, pub_day = job.body.split('|')
            process_type(topic_id, pub_day)
            process_gender(topic_id, pub_day)
            process_age(topic_id, pub_day)
            process_religion(topic_id, pub_day)
            worker.deleteJob(job)
        except Exception, e:
            print e


def process_type(topic_id, pub_day):
    conn, cursor = open_conn()
    sql = """DELETE FROM topic_tweet_events_rollup_minute
             WHERE topic = {} AND pub_day = {} AND stat_group = 'type'""".format(topic_id, pub_day)
    cursor.execute(sql)
    sql = """CREATE TEMPORARY TABLE temp_rollup AS
                 SELECT t_id, pub_year, pub_month, pub_day, pub_hour, 'type' AS stat_group,
                 type, hll_add_agg(hll_hash_bigint(tweet_id))
                 FROM topic_tweet_mapping
                 WHERE t_id = {} AND
                 pub_day = {}
                 GROUP BY t_id, pub_year, pub_month, pub_day, pub_hour, type;
          """.format(topic_id, pub_day)
    cursor.execute(sql)
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
            SELECT * FROM temp_rollup;"""
    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)
    
def process_gender(topic_id, pub_day):
    conn, cursor = open_conn()
    sql = """DELETE FROM topic_tweet_events_rollup_minute
             WHERE topic = {} AND pub_day = {} AND stat_group = 'account_status'""".format(topic_id, pub_day)
    cursor.execute(sql)
    sql = """CREATE TEMPORARY TABLE temp_rollup AS
                 SELECT t_id, pub_year, pub_month, pub_day, pub_hour, 'account_status' AS stat_group,
                 gender, hll_add_agg(hll_hash_bigint(tweet_id))
                 FROM topic_tweet_mapping
                 WHERE t_id = {} AND
                 pub_day = {}
                 GROUP BY t_id, pub_year, pub_month, pub_day, pub_hour, gender;
          """.format(topic_id, pub_day)
    cursor.execute(sql)
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
            SELECT * FROM temp_rollup;"""
    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)

def process_age(topic_id, pub_day):
    conn, cursor = open_conn()
    sql = """DELETE FROM topic_tweet_events_rollup_minute
             WHERE topic = {} AND pub_day = {} AND stat_group = 'age'""".format(topic_id, pub_day)
    cursor.execute(sql)
    sql = """CREATE TEMPORARY TABLE temp_rollup AS
                 SELECT t_id, pub_year, pub_month, pub_day, pub_hour, 'age' AS stat_group,
                 age, hll_add_agg(hll_hash_bigint(tweet_id))
                 FROM topic_tweet_mapping
                 WHERE t_id = {} AND
                 pub_day = {}
                 GROUP BY t_id, pub_year, pub_month, pub_day, pub_hour, age;
          """.format(topic_id, pub_day)
    cursor.execute(sql)
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
            SELECT * FROM temp_rollup;"""
    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)

def process_religion(topic_id, pub_day):
    conn, cursor = open_conn()
    sql = """DELETE FROM topic_tweet_events_rollup_minute
             WHERE topic = {} AND pub_day = {} AND stat_group = 'religion'""".format(topic_id, pub_day)
    cursor.execute(sql)
    sql = """CREATE TEMPORARY TABLE temp_rollup AS
                 SELECT t_id, pub_year, pub_month, pub_day, pub_hour, 'religion' AS stat_group,
                 religion, hll_add_agg(hll_hash_bigint(tweet_id))
                 FROM topic_tweet_mapping
                 WHERE t_id = {} AND
                 pub_day = {}
                 GROUP BY t_id, pub_year, pub_month, pub_day, pub_hour, religion;
          """.format(topic_id, pub_day)
    cursor.execute(sql)
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
            SELECT * FROM temp_rollup;"""
    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)

def select1(topic_id, pub_day):
    conn, cursor = open_conn()
    sql = """SELECT
                 topic,
                 pub_day,
                 stat_group,
                 stat_type,
                 SUM(hll_cardinality(stat_value)) AS value
             FROM
                 topic_tweet_events_rollup_minute
             WHERE topic = {} AND 
             pub_day = {}
             GROUP BY topic, pub_day, stat_group, stat_type
             ORDER BY pub_day ASC, stat_type;""".format(topic_id, pub_day)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)


# process_type(5494, '20190113')
# process_gender(5494, '20190113')
# process_age(5494, '20190113')
# process_religion(5494, '20190113')
select1(5494, '20190113')

